package compaction;

import level.Level;
import level.LevelManager;
import sstable.SSTableEntry;
import sstable.SSTableReader;

import java.io.IOException;
import java.util.*;

public class Compactor {

    public static final int L0_TRIGGER_COUNT = 4;
    public static final int LN_TRIGGER_COUNT = 4;
    public static final int TARGET_ENTRIES_PER_SSTABLE = 1000;

    private final LevelManager levelManager;

    public Compactor(LevelManager levelManager) {
        this.levelManager = levelManager;
    }

    /**
     * Checks all levels and triggers cascading compaction if thresholds are exceeded:
     * Level 0 -> Level 1 -> Level 2 ...
     */
    public synchronized boolean checkAndCompact() throws IOException {
        boolean compacted = false;

        // Check Level 0
        Level l0 = levelManager.getLevel(0);
        if (l0 != null && l0.size() >= L0_TRIGGER_COUNT) {
            compactLevel(0);
            compacted = true;
        }

        // Check deeper levels (L1, L2, etc.) for cascading compactions
        for (int i = 1; i < LevelManager.MAX_LEVELS - 1; i++) {
            Level level = levelManager.getLevel(i);
            if (level != null && level.size() >= LN_TRIGGER_COUNT) {
                compactLevel(i);
                compacted = true;
            }
        }

        return compacted;
    }

    /**
     * Compacts fromLevel -> toLevel (fromLevel + 1):
     * 1. Selects input files from fromLevel.
     * 2. Finds overlapping files in toLevel.
     * 3. Multi-way merge-sorts all records, deduplicating keys (keeping latest timestamp).
     * 4. Purges tombstones if at bottom-most active level.
     * 5. Writes non-overlapping partitioned SSTables into toLevel.
     * 6. Atomically updates MANIFEST.sb and deletes old files.
     */
    public synchronized void compactLevel(int fromLevelNum) throws IOException {
        int toLevelNum = fromLevelNum + 1;
        if (toLevelNum >= LevelManager.MAX_LEVELS) {
            return;
        }

        Level fromLevel = levelManager.getLevel(fromLevelNum);
        Level toLevel = levelManager.getLevel(toLevelNum);
        if (fromLevel == null || fromLevel.size() == 0 || toLevel == null) {
            return;
        }

        System.out.println("\n[Compactor] Triggering Level " + fromLevelNum + " -> Level " + toLevelNum + " Compaction...");

        // 1. Select files from fromLevel
        List<SSTableReader> fromFiles = new ArrayList<>();
        if (fromLevelNum == 0) {
            // In L0, all files overlap, so take all L0 files
            fromFiles.addAll(fromLevel.getTables());
        } else {
            // In L1+, pick the first/oldest file
            fromFiles.add(fromLevel.getTables().get(0));
        }

        // 2. Find range [minKey, maxKey] across fromFiles
        String minKey = null;
        String maxKey = null;
        for (SSTableReader reader : fromFiles) {
            if (minKey == null || reader.getMinKey().compareTo(minKey) < 0) minKey = reader.getMinKey();
            if (maxKey == null || reader.getMaxKey().compareTo(maxKey) > 0) maxKey = reader.getMaxKey();
        }

        // 3. Find overlapping files in toLevel
        List<SSTableReader> toOverlappingFiles = new ArrayList<>();
        for (SSTableReader reader : toLevel.getTables()) {
            if (rangesOverlap(minKey, maxKey, reader.getMinKey(), reader.getMaxKey())) {
                toOverlappingFiles.add(reader);
            }
        }

        System.out.println("[Compactor] Merging " + fromFiles.size() + " files from L" + fromLevelNum +
                " and " + toOverlappingFiles.size() + " overlapping files from L" + toLevelNum);

        // 4. Multi-way merge-sort
        List<SSTableReader> allInputFiles = new ArrayList<>();
        allInputFiles.addAll(fromFiles);
        allInputFiles.addAll(toOverlappingFiles);

        List<SSTableReader> newTables = mergeAndPartition(allInputFiles, toLevelNum);

        // 5. Atomic replacement & disk cleanup
        levelManager.replaceLevelTables(fromLevelNum, fromFiles, toLevelNum, toOverlappingFiles, newTables);

        System.out.println("[Compactor] Level " + fromLevelNum + " -> Level " + toLevelNum +
                " compaction completed! (L" + fromLevelNum + " size: " + fromLevel.size() +
                ", L" + toLevelNum + " size: " + toLevel.size() + ")");

        // Check if next level needs compaction now (cascading)
        if (toLevel.size() >= LN_TRIGGER_COUNT) {
            compactLevel(toLevelNum);
        }
    }

    private static class MergeItem {
        SSTableEntry entry;
        int fileIndex;
        Iterator<SSTableEntry> iterator;

        MergeItem(SSTableEntry entry, int fileIndex, Iterator<SSTableEntry> iterator) {
            this.entry = entry;
            this.fileIndex = fileIndex;
            this.iterator = iterator;
        }
    }

    private List<SSTableReader> mergeAndPartition(List<SSTableReader> inputFiles, int toLevelNum) throws IOException {
        PriorityQueue<MergeItem> pq = new PriorityQueue<>((a, b) -> {
            int cmp = a.entry.getKey().compareTo(b.entry.getKey());
            if (cmp != 0) return cmp;
            // Newer timestamp/epoch comes first for identical keys
            return Long.compare(b.entry.getEpoch(), a.entry.getEpoch());
        });

        // Initialize priority queue with first entry of each file
        for (int i = 0; i < inputFiles.size(); i++) {
            List<SSTableEntry> allEntries = inputFiles.get(i).scanAll();
            if (!allEntries.isEmpty()) {
                Iterator<SSTableEntry> iter = allEntries.iterator();
                pq.offer(new MergeItem(iter.next(), i, iter));
            }
        }

        List<SSTableReader> newTables = new ArrayList<>();
        Map<String, Object> currentChunk = new TreeMap<>();
        String lastKey = null;

        // Check if toLevel is the bottom-most level containing data to decide tombstone GC
        boolean isBottomLevel = isBottomMostActiveLevel(toLevelNum);

        while (!pq.isEmpty()) {
            MergeItem top = pq.poll();
            SSTableEntry currentEntry = top.entry;
            String key = currentEntry.getKey();

            // Advance iterator for this file
            if (top.iterator.hasNext()) {
                top.entry = top.iterator.next();
                pq.offer(top);
            }

            // Deduplicate: if key was already processed, skip older version
            if (key.equals(lastKey)) {
                continue;
            }
            lastKey = key;

            // Handle tombstone purging
            if (currentEntry.isTombstone()) {
                if (isBottomLevel) {
                    // Purge tombstone from disk completely!
                    continue;
                } else {
                    currentChunk.put(key, null); // Retain tombstone to shadow deeper levels
                }
            } else {
                currentChunk.put(key, currentEntry.getValue());
            }

            // If chunk reaches target size, flush a partitioned SSTable for toLevel
            if (currentChunk.size() >= TARGET_ENTRIES_PER_SSTABLE) {
                SSTableReader newTable = levelManager.createSSTable(currentChunk);
                newTables.add(newTable);
                currentChunk.clear();
            }
        }

        // Flush remaining entries
        if (!currentChunk.isEmpty()) {
            SSTableReader newTable = levelManager.createSSTable(currentChunk);
            newTables.add(newTable);
            currentChunk.clear();
        }

        return newTables;
    }

    private boolean rangesOverlap(String min1, String max1, String min2, String max2) {
        if (min1 == null || max1 == null || min2 == null || max2 == null) {
            return true;
        }
        return min1.compareTo(max2) <= 0 && max1.compareTo(min2) >= 0;
    }

    private boolean isBottomMostActiveLevel(int levelNum) {
        for (int i = levelNum + 1; i < LevelManager.MAX_LEVELS; i++) {
            Level level = levelManager.getLevel(i);
            if (level != null && level.size() > 0) {
                return false;
            }
        }
        return true;
    }
}
