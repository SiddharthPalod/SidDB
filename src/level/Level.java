package level;

import sstable.SSTableEntry;
import sstable.SSTableReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Level {

    private final int levelNumber;
    private final List<SSTableReader> tables;

    public Level(int levelNumber) {
        this.levelNumber = levelNumber;
        this.tables = new ArrayList<>();
    }

    public synchronized int getLevelNumber() {
        return levelNumber;
    }

    public synchronized List<SSTableReader> getTables() {
        return Collections.unmodifiableList(new ArrayList<>(tables));
    }

    public synchronized int size() {
        return tables.size();
    }

    /**
     * Adds an SSTable to this level.
     * For L0: Prepends to front (index 0) so newest files are checked first.
     * For L1+: Keeps list sorted by minKey for binary search lookup.
     */
    public synchronized void addTable(SSTableReader reader) {
        if (levelNumber == 0) {
            tables.add(0, reader);
        } else {
            tables.add(reader);
            tables.sort(Comparator.comparing(SSTableReader::getMinKey));
        }
    }

    public synchronized void removeTable(SSTableReader reader) {
        tables.remove(reader);
    }

    public synchronized void replaceTables(List<SSTableReader> oldTables, List<SSTableReader> newTables) {
        for (SSTableReader oldTable : oldTables) {
            tables.remove(oldTable);
        }
        for (SSTableReader newTable : newTables) {
            addTable(newTable);
        }
    }

    /**
     * Looks up a key within this level.
     * Returns SSTableEntry if found (including tombstones), or null if not present in this level.
     */
    public synchronized SSTableEntry get(String key) throws IOException {
        if (tables.isEmpty()) {
            return null;
        }

        if (levelNumber == 0) {
            // Level 0: Keys overlap, search newest to oldest
            for (SSTableReader table : tables) {
                SSTableEntry entry = table.get(key);
                if (entry != null) {
                    return entry; // Found newest version (or tombstone)
                }
            }
        } else {
            // Level 1+: Non-overlapping key ranges. Find candidate table using binary search.
            SSTableReader targetTable = findCandidateTable(key);
            if (targetTable != null) {
                return targetTable.get(key);
            }
        }

        return null;
    }

    /**
     * Binary search to find the single SSTable whose [minKey, maxKey] range covers the key.
     */
    private SSTableReader findCandidateTable(String key) {
        int low = 0;
        int high = tables.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            SSTableReader midTable = tables.get(mid);

            if (key.compareTo(midTable.getMinKey()) < 0) {
                high = mid - 1;
            } else if (key.compareTo(midTable.getMaxKey()) > 0) {
                low = mid + 1;
            } else {
                return midTable; // Key falls within [minKey, maxKey]
            }
        }

        return null;
    }

    public synchronized void close() throws IOException {
        for (SSTableReader table : tables) {
            table.close();
        }
    }
}
