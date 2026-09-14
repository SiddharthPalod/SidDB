package sstable;

import serializer.Serializer;
import serializer.SerializedData;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

public class SSTableWriter {

    public static final int DEFAULT_BLOCK_SIZE_BYTES = 4096; // 4KB

    public static void write(String basePath, Map<String, Object> sortedEntries) throws IOException {
        write(basePath, sortedEntries, DEFAULT_BLOCK_SIZE_BYTES);
    }

    public static void write(String basePath, Map<String, Object> sortedEntries, int targetBlockSizeBytes) throws IOException {
        if (sortedEntries == null || sortedEntries.isEmpty()) {
            return;
        }

        File dataFile = new File(basePath + ".sb");
        File indexFile = new File(basePath + ".idx");
        File filterFile = new File(basePath + ".bf");

        // Ensure parent directory exists
        File parent = dataFile.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }

        BloomFilter bloomFilter = new BloomFilter(sortedEntries.size(), 0.01);
        BlockIndex blockIndex = new BlockIndex();
        Serializer serializer = new Serializer();

        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
            raf.setLength(0); // Truncate if existing

            long currentBlockOffset = 0;
            int currentBlockBytes = 0;
            boolean isNewBlock = true;

            for (Map.Entry<String, Object> entry : sortedEntries.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // 1. Add key to Bloom Filter
                bloomFilter.add(key);

                // 2. Mark sparse index block entry at boundary
                if (isNewBlock) {
                    blockIndex.addEntry(key, currentBlockOffset);
                    isNewBlock = false;
                }

                // 3. Serialize key-value record
                long epoch = System.currentTimeMillis() / 1000;
                SerializedData serialized = serializer.serialize(epoch, key, value);
                byte[] recordBytes = serialized.getData();

                // 4. Write to .sb data file
                raf.write(recordBytes);
                currentBlockBytes += recordBytes.length;

                // Check if current block exceeded the threshold block size
                if (currentBlockBytes >= targetBlockSizeBytes) {
                    currentBlockOffset = raf.getFilePointer();
                    currentBlockBytes = 0;
                    isNewBlock = true;
                }
            }

            raf.getFD().sync();
        }

        // 5. Write index (.idx) and bloom filter (.bf)
        blockIndex.writeToFile(indexFile);
        bloomFilter.writeToFile(filterFile);
    }
}
