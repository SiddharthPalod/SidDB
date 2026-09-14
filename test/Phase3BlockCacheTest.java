package test;

import engine.SidDBEngine;
import sstable.BlockCache;

import java.io.File;

public class Phase3BlockCacheTest {

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

    private static void deleteDir(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) deleteDir(f);
                    else f.delete();
                }
            }
            dir.delete();
        }
    }

    public static void run() {
        System.out.println("\n--- [Running Phase3BlockCacheTest (LRU Block Cache)] ---");
        String dbDir = "test_block_cache_data";
        deleteDir(new File(dbDir));

        try {
            // Threshold = 20 entries per MemTable, small block size triggers block writes
            try (SidDBEngine db = new SidDBEngine(dbDir, 20)) {
                BlockCache cache = db.getBlockCache();

                // 1. Write 20 entries and manually flush to create an L0 SSTable
                for (int i = 0; i < 20; i++) {
                    db.put(String.format("k_%03d", i), "val_" + i);
                }
                db.flushMemTable(); // Flush to L0

                assertEquals(0L, cache.getHitCount(), "Initial cache hits should be 0");
                assertEquals(0L, cache.getMissCount(), "Initial cache misses should be 0");

                // 2. First Read -> Cache Miss (Loads block from disk into cache)
                System.out.println("Reading k_005 (First access)...");
                assertEquals("val_5", db.get("k_005"), "Read k_005");
                assertEquals(0L, cache.getHitCount(), "Hits after 1st read should be 0");
                assertEquals(1L, cache.getMissCount(), "Misses after 1st read should be 1");
                assertEquals(1, cache.size(), "Cache size should now hold 1 block");

                // 3. Second Read to same key -> Cache Hit (0 Disk reads!)
                System.out.println("Reading k_005 (Second access - Cache Hit)...");
                assertEquals("val_5", db.get("k_005"), "Read k_005 again");
                assertEquals(1L, cache.getHitCount(), "Hits after 2nd read should be 1");
                assertEquals(1L, cache.getMissCount(), "Misses should stay 1");

                // 4. Read to a neighbouring key in the SAME block -> Cache Hit!
                System.out.println("Reading neighbouring key k_006 in same block (Cache Hit)...");
                assertEquals("val_6", db.get("k_006"), "Read neighbouring key k_006");
                assertEquals(2L, cache.getHitCount(), "Hits after neighbouring read should be 2");
                assertEquals(1L, cache.getMissCount(), "Misses should stay 1");

                System.out.println("Cache Statistics: " + cache);
                assertTrue(cache.getHitRatio() > 0.5, "Hit ratio should be > 50%");
            }

            // 5. Test Standalone LRU Eviction behavior
            BlockCache smallCache = new BlockCache(2); // Capacity = 2 blocks
            smallCache.put("sst_1", 0, java.util.List.of());
            smallCache.put("sst_1", 100, java.util.List.of());
            assertEquals(2, smallCache.size(), "Cache at capacity");

            // Access block 0 to make it recently used
            smallCache.get("sst_1", 0);

            // Insert 3rd block -> Should evict block 100 (least recently used)
            smallCache.put("sst_2", 0, java.util.List.of());
            assertEquals(2, smallCache.size(), "Cache should stay at capacity 2");
            assertTrue(smallCache.get("sst_1", 0) != null, "Block 0 should still be in cache");
            assertTrue(smallCache.get("sst_2", 0) != null, "Block 2 should be in cache");

            System.out.println("  ✓ Phase3BlockCacheTest passed successfully!");
        } catch (Exception e) {
            throw new RuntimeException("Phase3BlockCacheTest failed", e);
        } finally {
            deleteDir(new File(dbDir));
        }
    }
}
