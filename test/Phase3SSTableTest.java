package test;

import sstable.BloomFilter;
import sstable.SSTableEntry;
import sstable.SSTableReader;
import sstable.SSTableWriter;

import java.io.File;
import java.util.Map;
import java.util.TreeMap;

public class Phase3SSTableTest {

    private static void assertEquals(Object expected, Object actual, String message) {
        if (expected == null && actual == null) return;
        if (expected != null && expected.equals(actual)) return;
        throw new AssertionError(message + " | Expected: [" + expected + "], Actual: [" + actual + "]");
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    public static void run() {
        System.out.println("\n--- [Running Phase3SSTableTest] ---");
        String sstBase = "test_sst_001";

        // Clean up any previous test files
        new File(sstBase + ".sb").delete();
        new File(sstBase + ".idx").delete();
        new File(sstBase + ".bf").delete();

        try {
            // 1. Prepare sorted dataset spanning multiple blocks (small block size to trigger indexing)
            Map<String, Object> data = new TreeMap<>();
            for (int i = 0; i < 200; i++) {
                String key = String.format("user_%04d", i);
                data.put(key, "data_payload_" + i);
            }
            // Add a tombstone entry
            data.put("user_0050", null);

            // 2. Write SSTable with small block size (512 bytes) to create multiple indexed blocks
            SSTableWriter.write(sstBase, data, 512);

            assertTrue(new File(sstBase + ".sb").exists(), ".sb data file should exist");
            assertTrue(new File(sstBase + ".idx").exists(), ".idx index file should exist");
            assertTrue(new File(sstBase + ".bf").exists(), ".bf bloom filter file should exist");

            // 3. Open SSTableReader and verify point lookups
            try (SSTableReader reader = SSTableReader.open(sstBase)) {
                System.out.println("  Indexed blocks count: " + reader.getBlockIndex().size());
                System.out.println("  SSTable minKey: " + reader.getMinKey() + ", maxKey: " + reader.getMaxKey());
                assertTrue(reader.getBlockIndex().size() > 1, "Should have created multiple sparse index blocks");

                // Test existing keys across different blocks
                SSTableEntry entry0 = reader.get("user_0000");
                assertEquals("data_payload_0", entry0.getValue(), "Check first key");

                SSTableEntry entry150 = reader.get("user_0150");
                assertEquals("data_payload_150", entry150.getValue(), "Check middle key");

                SSTableEntry entry199 = reader.get("user_0199");
                assertEquals("data_payload_199", entry199.getValue(), "Check last key");

                // Test tombstone entry
                SSTableEntry tombstone = reader.get("user_0050");
                assertTrue(tombstone != null && tombstone.isTombstone(), "user_0050 should be recognized as tombstone");

                // Test non-existent keys (pruned by Bloom filter / index)
                SSTableEntry notFound1 = reader.get("user_9999");
                assertEquals(null, notFound1, "user_9999 should not exist");

                SSTableEntry notFound2 = reader.get("apple_key");
                assertEquals(null, notFound2, "apple_key should not exist (outside bounds)");

                // Test full scan for compaction
                var allEntries = reader.scanAll();
                assertEquals(200, allEntries.size(), "Scan all should return 200 sorted entries");
            }

            System.out.println("  ✓ Phase3SSTableTest passed successfully!");
        } catch (Exception e) {
            throw new RuntimeException("Phase3SSTableTest failed", e);
        } finally {
            new File(sstBase + ".sb").delete();
            new File(sstBase + ".idx").delete();
            new File(sstBase + ".bf").delete();
        }
    }
}
