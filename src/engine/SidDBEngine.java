package engine;

import compaction.Compactor;
import level.LevelManager;
import memtable.MemTable;
import serializer.DeserializedData;
import sstable.SSTableEntry;
import tx.Snapshot;
import tx.WriteBatch;
import wal.WAL;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class SidDBEngine implements AutoCloseable {

    public static final int DEFAULT_MEMTABLE_THRESHOLD = 1000;

    private final String dbDirectory;
    private final MemTable activeMemTable;
    private MemTable frozenMemTable;
    private final WAL wal;
    private final LevelManager levelManager;
    private final Compactor compactor;
    private final int memTableThreshold;
    private final AtomicLong sequenceNumber;

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
        this.compactor = new Compactor(this.levelManager, this.memTableThreshold);
        this.sequenceNumber = new AtomicLong(System.currentTimeMillis() / 1000);

        // Replay any un-flushed WAL records from previous run
        recover();
    }

    private synchronized void recover() throws IOException {
        System.out.println("[Engine] Replaying WAL for un-flushed mutations...");
        var entries = wal.replay();

        for (DeserializedData entry : entries) {
            String key = entry.getKey().toString();
            Object value = entry.getValue();
            long epoch = entry.getEpoch();
            if (epoch > sequenceNumber.get()) {
                sequenceNumber.set(epoch);
            }
            // Retain tombstones (value == null) in active memory so they shadow disk tables
            activeMemTable.put(key, value, epoch);
        }
        System.out.println("[Engine] WAL replay complete (" + entries.size() + " records replayed into active MemTable).");
    }

    /**
     * Executes a multi-key atomic WriteBatch (ACID Atomicity & Durability):
     * 1. Atomically appends the entire batch to the WAL (fsync).
     * 2. Applies all mutations into the active MemTable with a monotonic sequence number.
     * 3. Triggers flush if threshold reached.
     */
    public synchronized void write(WriteBatch batch) throws IOException {
        if (batch == null || batch.size() == 0) {
            return;
        }

        long seq = sequenceNumber.incrementAndGet();

        // 1. Write entire batch atomically to WAL first
        wal.appendBatch(batch, seq);

        // 2. Apply all mutations into active MemTable
        for (WriteBatch.Op op : batch.getOperations()) {
            activeMemTable.put(op.getKey(), op.getValue(), seq);
        }

        // 3. Flush if threshold reached
        if (activeMemTable.size() >= memTableThreshold) {
            flushMemTable();
        }
    }

    /**
     * Puts a single key-value pair using an atomic WriteBatch.
     */
    public synchronized void put(String key, Object value) throws IOException {
        write(new WriteBatch().put(key, value));
    }

    /**
     * Deletes a key by logging a tombstone via an atomic WriteBatch.
     */
    public synchronized void delete(String key) throws IOException {
        write(new WriteBatch().delete(key));
    }

    /**
     * Returns a point-in-time Snapshot for MVCC Snapshot Isolation.
     */
    public synchronized Snapshot getSnapshot() {
        return new Snapshot(sequenceNumber.get());
    }

    /**
     * Point lookup for latest active state.
     */
    public synchronized Object get(String key) throws IOException {
        return get(key, Long.MAX_VALUE);
    }

    /**
     * Point lookup respecting MVCC Snapshot Isolation:
     * Reads the database state exactly as it existed at the snapshot sequence number.
     */
    public synchronized Object get(String key, Snapshot snapshot) throws IOException {
        return get(key, snapshot.getSequenceNumber());
    }

    private synchronized Object get(String key, long maxSeq) throws IOException {
        // Step 1: Active MemTable (filtered by snapshot sequence number)
        if (activeMemTable.containsKey(key, maxSeq)) {
            return activeMemTable.get(key, maxSeq);
        }

        // Step 2: Frozen MemTable
        if (frozenMemTable != null && frozenMemTable.containsKey(key, maxSeq)) {
            return frozenMemTable.get(key, maxSeq);
        }

        // Step 3 & 4: Disk Levels (L0 -> L1 -> L2 ... LN)
        SSTableEntry diskEntry = levelManager.get(key, maxSeq);
        if (diskEntry != null) {
            if (diskEntry.isTombstone()) {
                return null;
            }
            return diskEntry.getValue();
        }

        return null;
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
        List<SSTableEntry> sstEntries = activeMemTable.getLatestSSTableEntries();
        this.frozenMemTable = new MemTable();
        for (SSTableEntry e : sstEntries) {
            this.frozenMemTable.put(e.getKey(), e.getValue(), e.getEpoch());
        }

        // 2. Clear active MemTable & reset WAL for new writes
        activeMemTable.clear();
        wal.truncate();

        // 3. Write frozen snapshot to L0 SSTable preserving exact epochs/seq
        levelManager.flushMemTableToL0(sstEntries);

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

    public synchronized sstable.BlockCache getBlockCache() {
        return levelManager.getBlockCache();
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

    public long getSequenceNumber() {
        return sequenceNumber.get();
    }

    @Override
    public synchronized void close() throws IOException {
        wal.close();
        levelManager.close();
    }
}