package level;

import sstable.BlockCache;
import sstable.SSTableEntry;
import sstable.SSTableReader;
import sstable.SSTableWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class LevelManager implements AutoCloseable {

    public static final int MAX_LEVELS = 7;
    private final File dbDirectory;
    private final File manifestFile;
    private final List<Level> levels;
    private final AtomicLong nextSequenceNumber;
    private final BlockCache blockCache;

    public LevelManager(String dbDirPath) throws IOException {
        this(dbDirPath, new BlockCache());
    }

    public LevelManager(String dbDirPath, BlockCache blockCache) throws IOException {
        this.dbDirectory = new File(dbDirPath);
        if (!this.dbDirectory.exists()) {
            this.dbDirectory.mkdirs();
        }
        this.manifestFile = new File(this.dbDirectory, "MANIFEST.sb");
        this.levels = new ArrayList<>();
        for (int i = 0; i < MAX_LEVELS; i++) {
            this.levels.add(new Level(i));
        }
        this.nextSequenceNumber = new AtomicLong(1);
        this.blockCache = (blockCache != null) ? blockCache : new BlockCache();

        // Recover existing SSTables from Manifest if present
        recoverFromManifest();
    }

    /**
     * Recovers existing level hierarchy and SSTables from MANIFEST.sb.
     */
    private synchronized void recoverFromManifest() throws IOException {
        if (!manifestFile.exists()) {
            return;
        }

        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(manifestFile)))) {
            long seq = dis.readLong();
            this.nextSequenceNumber.set(seq);

            int numLevels = dis.readInt();
            for (int i = 0; i < numLevels; i++) {
                int levelNum = dis.readInt();
                int tableCount = dis.readInt();

                Level level = (levelNum < levels.size()) ? levels.get(levelNum) : new Level(levelNum);

                for (int t = 0; t < tableCount; t++) {
                    String basePath = dis.readUTF();
                    File dataFile = new File(basePath + ".sb");
                    if (dataFile.exists()) {
                        SSTableReader reader = SSTableReader.open(basePath, blockCache);
                        level.addTable(reader);
                    }
                }
            }
        }
        System.out.println("[LevelManager] Recovered SSTable hierarchy from MANIFEST.sb (L0 size: " + levels.get(0).size() + ")");
    }

    /**
     * Persists current level state to MANIFEST.sb atomically.
     */
    public synchronized void saveManifest() throws IOException {
        File tempManifest = new File(dbDirectory, "MANIFEST.sb.tmp");

        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempManifest)))) {
            dos.writeLong(nextSequenceNumber.get());
            dos.writeInt(levels.size());

            for (Level level : levels) {
                dos.writeInt(level.getLevelNumber());
                List<SSTableReader> tables = level.getTables();
                dos.writeInt(tables.size());

                for (SSTableReader table : tables) {
                    dos.writeUTF(table.getBasePath());
                }
            }
        }

        // Atomic file replacement
        if (manifestFile.exists()) {
            manifestFile.delete();
        }
        tempManifest.renameTo(manifestFile);
    }

    /**
     * Creates a new SSTable on disk and returns an open reader without placing it into any level yet.
     */
    public synchronized SSTableReader createSSTable(Map<String, Object> entries) throws IOException {
        if (entries == null || entries.isEmpty()) {
            return null;
        }

        String fileName = String.format("sst_%05d", nextSequenceNumber.getAndIncrement());
        String basePath = new File(dbDirectory, fileName).getAbsolutePath();

        SSTableWriter.write(basePath, entries);
        return SSTableReader.open(basePath, blockCache);
    }

    public synchronized SSTableReader createSSTable(List<SSTableEntry> entries) throws IOException {
        if (entries == null || entries.isEmpty()) {
            return null;
        }

        String fileName = String.format("sst_%05d", nextSequenceNumber.getAndIncrement());
        String basePath = new File(dbDirectory, fileName).getAbsolutePath();

        SSTableWriter.writeEntries(basePath, entries);
        return SSTableReader.open(basePath, blockCache);
    }

    /**
     * Flushes MemTable entries to a brand new Level 0 SSTable file (.sb, .idx, .bf).
     */
    public synchronized SSTableReader flushMemTableToL0(Map<String, Object> entries) throws IOException {
        SSTableReader reader = createSSTable(entries);
        if (reader != null) {
            levels.get(0).addTable(reader);
            saveManifest();
            System.out.println("[LevelManager] Flushed " + entries.size() + " entries into L0 SSTable: " + reader.getBasePath() + " (Total L0 tables: " + levels.get(0).size() + ")");
        }
        return reader;
    }

    public synchronized SSTableReader flushMemTableToL0(List<SSTableEntry> entries) throws IOException {
        SSTableReader reader = createSSTable(entries);
        if (reader != null) {
            levels.get(0).addTable(reader);
            saveManifest();
            System.out.println("[LevelManager] Flushed " + entries.size() + " entries into L0 SSTable: " + reader.getBasePath() + " (Total L0 tables: " + levels.get(0).size() + ")");
        }
        return reader;
    }

    /**
     * Atomically replaces old tables with new compacted tables across two levels, saves the manifest,
     * invalidates block cache for old tables, closes old readers, and deletes old files.
     */
    public synchronized void replaceLevelTables(
            int fromLevelNum, List<SSTableReader> fromOldTables,
            int toLevelNum, List<SSTableReader> toOldTables,
            List<SSTableReader> toNewTables
    ) throws IOException {
        Level fromLevel = levels.get(fromLevelNum);
        Level toLevel = levels.get(toLevelNum);

        for (SSTableReader oldTable : fromOldTables) {
            fromLevel.removeTable(oldTable);
        }

        toLevel.replaceTables(toOldTables, toNewTables);

        // 1. Atomically save manifest with the updated state
        saveManifest();

        // 2. Invalidate cache, close readers, and physically delete old files
        List<SSTableReader> allOldTables = new ArrayList<>();
        allOldTables.addAll(fromOldTables);
        allOldTables.addAll(toOldTables);

        for (SSTableReader oldReader : allOldTables) {
            String path = oldReader.getBasePath();
            blockCache.invalidateSSTable(path);
            oldReader.close();
            new File(path + ".sb").delete();
            new File(path + ".idx").delete();
            new File(path + ".bf").delete();
        }
    }

    /**
     * Multi-level hierarchy query (L0 newest-to-oldest -> L1 -> L2 ... LN).
     */
    public synchronized SSTableEntry get(String key) throws IOException {
        for (Level level : levels) {
            SSTableEntry entry = level.get(key);
            if (entry != null) {
                return entry; // Found newest active value or tombstone in shallowest level
            }
        }
        return null;
    }

    /**
     * Multi-level hierarchy query respecting Snapshot Isolation.
     */
    public synchronized SSTableEntry get(String key, long maxSequenceNumber) throws IOException {
        for (Level level : levels) {
            SSTableEntry entry = level.get(key, maxSequenceNumber);
            if (entry != null) {
                return entry;
            }
        }
        return null;
    }

    public synchronized Level getLevel(int levelNum) {
        if (levelNum >= 0 && levelNum < levels.size()) {
            return levels.get(levelNum);
        }
        return null;
    }

    public synchronized List<Level> getLevels() {
        return levels;
    }

    public File getDbDirectory() {
        return dbDirectory;
    }

    public BlockCache getBlockCache() {
        return blockCache;
    }

    @Override
    public synchronized void close() throws IOException {
        for (Level level : levels) {
            level.close();
        }
    }
}
