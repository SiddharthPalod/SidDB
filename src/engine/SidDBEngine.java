package engine;

import compaction.Compactor;
import level.LevelManager;
import memtable.MemTable;
import serializer.DeserializedData;
import sstable.SSTableEntry;
import wal.WAL;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class SidDBEngine implements AutoCloseable {

    public static final int DEFAULT_MEMTABLE_THRESHOLD = 1000;

    private final String dbDirectory;
    private final MemTable activeMemTable;
    private MemTable frozenMemTable;
    private final WAL wal;
    private final LevelManager levelManager;
    private final Compactor compactor;
    private final int memTableThreshold;

    public SidDBEngine() throws IOException {
        this("data", DEFAULT_MEMTABLE_THRESHOLD);
    }

    public SidDBEngine(String dbDirectory) throws IOException {
        this(dbDirectory, DEFAULT_MEMTABLE_THRESHOLD);
    }

    public SidDBEngine(String dbDirectory, int memTableThreshold) throws IOException {
        this.dbDirectory = dbDirectory;
        this.memTableThreshold = memTableThreshold;
        
        File dir = new File(dbDirectory);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        this.activeMemTable = new MemTable();
        this.frozenMemTable = null;
        this.wal = new WAL(new File(dir, "siddb.wal").getAbsolutePath());
        this.levelManager = new LevelManager(dir.getAbsolutePath());
        this.compactor = new Compactor(this.levelManager);

        // Replay any un-flushed WAL records from previous run
        recover();
    }

    private synchronized void recover() throws IOException {
        System.out.println("[Engine] Replaying WAL for un-flushed mutations...");
        var entries = wal.replay();

        for (DeserializedData entry : entries) {
            String key = entry.getKey().toString();
            Object value = entry.getValue();
            // Retain tombstones (value == null) in active memory so they shadow disk tables
            activeMemTable.put(key, value);
        }
        System.out.println("[Engine] WAL replay complete (" + entries.size() + " records replayed into active MemTable).");
    }

    /**
     * Puts a key-value pair into the database:
     * 1. Appends to WAL (durability).
     * 2. Puts into active MemTable.
     * 3. Triggers flush to L0 SSTable if threshold reached.
     */
    public synchronized void put(String key, Object value) throws IOException {
        // 1. Write to WAL first
        wal.append(key, value);

        // 2. Write to active MemTable
        activeMemTable.put(key, value);

        // 3. Flush to L0 if MemTable reached threshold
        if (activeMemTable.size() >= memTableThreshold) {
            flushMemTable();
        }
    }

    /**
     * Deletes a key by logging a tombstone (null value):
     * 1. Appends tombstone to WAL.
     * 2. Puts tombstone in active MemTable to shadow older disk values.
     * 3. Triggers flush if threshold reached.
     */
    public synchronized void delete(String key) throws IOException {
        wal.append(key, null);
        activeMemTable.put(key, null); // Keep tombstone in MemTable

        if (activeMemTable.size() >= memTableThreshold) {
            flushMemTable();
        }
    }

    /**
     * 4-Tier Hierarchical Read Path:
     * Step 1: Active MemTable (RAM)
     * Step 2: Frozen MemTable (RAM, if currently flushing)
     * Step 3: Level 0 SSTables (Disk, newest to oldest)
     * Step 4: Level 1..N SSTables (Disk, partitioned ranges)
     */
    public synchronized Object get(String key) throws IOException {
        // Step 1: Active MemTable
        if (activeMemTable.containsKey(key)) {
            return activeMemTable.get(key); // returns value, or null if deleted tombstone
        }

        // Step 2: Frozen MemTable
        if (frozenMemTable != null && frozenMemTable.containsKey(key)) {
            return frozenMemTable.get(key);
        }

        // Step 3 & 4: Disk Levels (L0 -> L1 -> L2 ... LN)
        SSTableEntry diskEntry = levelManager.get(key);
        if (diskEntry != null) {
            if (diskEntry.isTombstone()) {
                return null; // Shadowed by deletion tombstone
            }
            return diskEntry.getValue();
        }

        return null; // Not found anywhere
    }

    /**
     * Flushes active MemTable into an immutable Level 0 SSTable and resets the WAL.
     */
    public synchronized void flushMemTable() throws IOException {
        if (activeMemTable.size() == 0) {
            return;
        }

        System.out.println("[Engine] Freezing active MemTable and flushing to Level 0 SSTable...");

        // 1. Freeze active MemTable
        Map<String, Object> snapshot = activeMemTable.getEntries();
        this.frozenMemTable = new MemTable();
        for (Map.Entry<String, Object> e : snapshot.entrySet()) {
            this.frozenMemTable.put(e.getKey(), e.getValue());
        }

        // 2. Clear active MemTable & reset WAL for new writes
        activeMemTable.clear();
        wal.truncate();

        // 3. Write frozen snapshot to L0 SSTable
        levelManager.flushMemTableToL0(snapshot);

        // 4. Clear frozen MemTable
        this.frozenMemTable = null;
        System.out.println("[Engine] Flush to Level 0 completed successfully!");

        // 5. Trigger Compaction check (L0 -> L1 -> L2 cascading)
        compactor.checkAndCompact();
    }

    public synchronized int size() {
        return activeMemTable.size();
    }

    /**
     * Returns all active unique keys in the database across memory and all disk levels.
     */
    public synchronized java.util.Set<String> keys() throws IOException {
        java.util.Set<String> allKeys = new java.util.TreeSet<>();
        java.util.Set<String> deletedKeys = new java.util.HashSet<>();

        // 1. Scan active MemTable
        for (Map.Entry<String, Object> entry : activeMemTable.getEntries().entrySet()) {
            if (entry.getValue() == null) {
                deletedKeys.add(entry.getKey());
            } else {
                allKeys.add(entry.getKey());
            }
        }

        // 2. Scan frozen MemTable
        if (frozenMemTable != null) {
            for (Map.Entry<String, Object> entry : frozenMemTable.getEntries().entrySet()) {
                if (entry.getValue() == null) {
                    deletedKeys.add(entry.getKey());
                } else if (!deletedKeys.contains(entry.getKey())) {
                    allKeys.add(entry.getKey());
                }
            }
        }

        // 3. Scan all disk levels (shallowest to deepest)
        for (level.Level level : levelManager.getLevels()) {
            for (sstable.SSTableReader table : level.getTables()) {
                for (SSTableEntry entry : table.scanAll()) {
                    if (entry.isTombstone()) {
                        deletedKeys.add(entry.getKey());
                    } else if (!deletedKeys.contains(entry.getKey())) {
                        allKeys.add(entry.getKey());
                    }
                }
            }
        }

        return allKeys;
    }

    public synchronized LevelManager getLevelManager() {
        return levelManager;
    }

    public synchronized Compactor getCompactor() {
        return compactor;
    }

    public synchronized void triggerCompaction() throws IOException {
        compactor.checkAndCompact();
    }

    public synchronized MemTable getActiveMemTable() {
        return activeMemTable;
    }

    @Override
    public synchronized void close() throws IOException {
        wal.close();
        levelManager.close();
    }
}