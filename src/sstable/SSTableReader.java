package sstable;

import serializer.DeserializedData;
import serializer.Serializer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class SSTableReader implements AutoCloseable {

    private final String basePath;
    private final BloomFilter bloomFilter;
    private final BlockIndex blockIndex;
    private final RandomAccessFile dataFile;
    private final Serializer serializer;
    private final String minKey;
    private final String maxKey;
    private final BlockCache blockCache;

    private SSTableReader(String basePath, BloomFilter bloomFilter, BlockIndex blockIndex, RandomAccessFile dataFile, String minKey, String maxKey, BlockCache blockCache) {
        this.basePath = basePath;
        this.bloomFilter = bloomFilter;
        this.blockIndex = blockIndex;
        this.dataFile = dataFile;
        this.serializer = new Serializer();
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.blockCache = blockCache;
    }

    public static SSTableReader open(String basePath) throws IOException {
        return open(basePath, null);
    }

    public static SSTableReader open(String basePath, BlockCache blockCache) throws IOException {
        File dataFile = new File(basePath + ".sb");
        File indexFile = new File(basePath + ".idx");
        File filterFile = new File(basePath + ".bf");

        if (!dataFile.exists() || !indexFile.exists() || !filterFile.exists()) {
            throw new IOException("SSTable files missing for basePath: " + basePath);
        }

        BloomFilter bloomFilter = BloomFilter.readFromFile(filterFile);
        BlockIndex blockIndex = BlockIndex.readFromFile(indexFile);
        RandomAccessFile raf = new RandomAccessFile(dataFile, "r");

        String minKey = "";
        String maxKey = "";

        if (blockIndex.size() > 0) {
            minKey = blockIndex.getEntries().get(0).getFirstKey();
            // To get accurate maxKey, read last record from last block
            Serializer tempSerializer = new Serializer();
            long lastBlockOffset = blockIndex.getEntries().get(blockIndex.size() - 1).getFileOffset();
            raf.seek(lastBlockOffset);
            while (true) {
                DeserializedData data = tempSerializer.readNext(raf);
                if (data == null) break;
                maxKey = data.getKey().toString();
            }
        }

        return new SSTableReader(basePath, bloomFilter, blockIndex, raf, minKey, maxKey, blockCache);
    }

    /**
     * Point lookup using Bloom Filter -> Sparse Index -> LRU Block Cache -> Disk Block Scan.
     */
    public synchronized SSTableEntry get(String key) throws IOException {
        // 1. Boundary check
        if (!minKey.isEmpty() && !maxKey.isEmpty()) {
            if (key.compareTo(minKey) < 0 || key.compareTo(maxKey) > 0) {
                return null;
            }
        }

        // 2. Bloom Filter check (0 Disk I/O if false!)
        if (!bloomFilter.mightContain(key)) {
            return null;
        }

        // 3. Binary search sparse index in memory
        BlockIndex.IndexEntry candidate = blockIndex.findCandidateBlock(key);
        if (candidate == null) {
            return null;
        }

        long blockOffset = candidate.getFileOffset();

        // 4. Check LRU Block Cache (0 Disk I/O on hit!)
        List<SSTableEntry> cachedBlock = null;
        if (blockCache != null) {
            cachedBlock = blockCache.get(basePath, blockOffset);
        }

        if (cachedBlock != null) {
            // CACHE HIT: Scan in-memory cached entries
            for (SSTableEntry entry : cachedBlock) {
                int cmp = entry.getKey().compareTo(key);
                if (cmp == 0) {
                    return entry;
                }
                if (cmp > 0) {
                    break;
                }
            }
            return null;
        }

        // 5. CACHE MISS: Seek to disk block and read entries
        List<SSTableEntry> blockEntries = new ArrayList<>();
        dataFile.seek(blockOffset);

        long nextBlockOffset = Long.MAX_VALUE;
        for (int i = 0; i < blockIndex.size(); i++) {
            if (blockIndex.getEntries().get(i).getFileOffset() == blockOffset && i + 1 < blockIndex.size()) {
                nextBlockOffset = blockIndex.getEntries().get(i + 1).getFileOffset();
                break;
            }
        }

        SSTableEntry matchedEntry = null;

        while (dataFile.getFilePointer() < nextBlockOffset) {
            DeserializedData data = serializer.readNext(dataFile);
            if (data == null) {
                break;
            }

            boolean isTombstone = (data.getValue() == null);
            SSTableEntry entry = new SSTableEntry(data.getKey().toString(), data.getValue(), isTombstone, data.getEpoch());
            blockEntries.add(entry);

            if (matchedEntry == null) {
                int cmp = entry.getKey().compareTo(key);
                if (cmp == 0) {
                    matchedEntry = entry;
                }
            }
        }

        // Populate LRU Block Cache
        if (blockCache != null && !blockEntries.isEmpty()) {
            blockCache.put(basePath, blockOffset, blockEntries);
        }

        return matchedEntry;
    }

    /**
     * Point lookup with Snapshot Isolation: ignores versions created after maxSequenceNumber.
     */
    public synchronized SSTableEntry get(String key, long maxSequenceNumber) throws IOException {
        SSTableEntry entry = get(key);
        if (entry != null && entry.getEpoch() <= maxSequenceNumber) {
            return entry;
        }
        return null;
    }

    /**
     * Scans and returns all records in sorted order (used during compaction).
     */
    public synchronized List<SSTableEntry> scanAll() throws IOException {
        List<SSTableEntry> entries = new ArrayList<>();
        dataFile.seek(0);

        while (true) {
            DeserializedData data = serializer.readNext(dataFile);
            if (data == null) {
                break;
            }
            boolean isTombstone = (data.getValue() == null);
            entries.add(new SSTableEntry(data.getKey().toString(), data.getValue(), isTombstone, data.getEpoch()));
        }

        return entries;
    }

    public String getBasePath() {
        return basePath;
    }

    public String getMinKey() {
        return minKey;
    }

    public String getMaxKey() {
        return maxKey;
    }

    public BloomFilter getBloomFilter() {
        return bloomFilter;
    }

    public BlockIndex getBlockIndex() {
        return blockIndex;
    }

    public BlockCache getBlockCache() {
        return blockCache;
    }

    @Override
    public synchronized void close() throws IOException {
        dataFile.close();
    }
}
