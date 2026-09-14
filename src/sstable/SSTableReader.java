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

    private SSTableReader(String basePath, BloomFilter bloomFilter, BlockIndex blockIndex, RandomAccessFile dataFile, String minKey, String maxKey) {
        this.basePath = basePath;
        this.bloomFilter = bloomFilter;
        this.blockIndex = blockIndex;
        this.dataFile = dataFile;
        this.serializer = new Serializer();
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    public static SSTableReader open(String basePath) throws IOException {
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
            // To get accurate maxKey, read last record or approximate from last block
            Serializer tempSerializer = new Serializer();
            long lastBlockOffset = blockIndex.getEntries().get(blockIndex.size() - 1).getFileOffset();
            raf.seek(lastBlockOffset);
            while (true) {
                DeserializedData data = tempSerializer.readNext(raf);
                if (data == null) break;
                maxKey = data.getKey().toString();
            }
        }

        return new SSTableReader(basePath, bloomFilter, blockIndex, raf, minKey, maxKey);
    }

    /**
     * Point lookup using Bloom Filter -> Sparse Index -> Block Scan.
     * Returns SSTableEntry if found (including tombstones), or null if key is not present.
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

        // 4. Seek to disk block and scan sorted records
        dataFile.seek(candidate.getFileOffset());

        while (true) {
            DeserializedData data = serializer.readNext(dataFile);
            if (data == null) {
                break;
            }

            String recordKey = data.getKey().toString();
            int cmp = recordKey.compareTo(key);

            if (cmp == 0) {
                // Key found! (value is null if it's a tombstone)
                boolean isTombstone = (data.getValue() == null);
                return new SSTableEntry(recordKey, data.getValue(), isTombstone, data.getEpoch());
            }

            if (cmp > 0) {
                // Passed the key in sorted order - key definitely does not exist
                break;
            }
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

    @Override
    public synchronized void close() throws IOException {
        dataFile.close();
    }
}
