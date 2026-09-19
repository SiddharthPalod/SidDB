package test;

import engine.SidDBEngine;
import level.LevelManager;

import java.io.File;

public class Phase3CompactionTest {

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

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        System.out.println("\n--- [Running Phase3CompactionTest (L0 -> L1 -> L2 Cascading Compaction)] ---");
        String dbDir = "test_compaction_data";
        deleteDir(new File(dbDir));

        try {
            // Threshold = 10 entries per MemTable
            try (SidDBEngine db = new SidDBEngine(dbDir, 10)) {
                LevelManager lm = db.getLevelManager();

                System.out.println("Step 1: Writing batches to trigger 4 MemTable flushes to Level 0...");
                for (int batch = 0; batch < 4; batch++) {
                    for (int i = 0; i < 10; i++) {
                        int id = (batch * 10) + i;
                        db.put(String.format("k_%03d", id), "val_" + id);
                    }
                }

                // Wait for asynchronous compaction to complete L0 -> L1 merge
                long deadline = System.currentTimeMillis() + 3000;
                while (System.currentTimeMillis() < deadline && lm.getLevel(0).size() > 0) {
                    Thread.sleep(20);
                }

                // After 4 flushes of 10 keys, L0 reached 4 files and triggered L0 -> L1 compaction!
                System.out.println("Checking level sizes after L0 -> L1 Compaction:");
                System.out.println("  Level 0 count: " + lm.getLevel(0).size());
                System.out.println("  Level 1 count: " + lm.getLevel(1).size());
                System.out.println("  Level 2 count: " + lm.getLevel(2).size());

                assertEquals(0, lm.getLevel(0).size(), "Level 0 should be compacted to 0 files");
                assertTrue(lm.getLevel(1).size() > 0, "Level 1 should now contain compacted SSTables");

                // Step 2: Verify all 40 keys are readable from Level 1
                for (int id = 0; id < 40; id++) {
                    String key = String.format("k_%03d", id);
                    assertEquals("val_" + id, db.get(key), "Read key from Level 1: " + key);
                }

                System.out.println("\nStep 3: Writing more batches to fill Level 1 and trigger L1 -> L2 Compaction...");
                // Write 12 more batches of 10 keys (each 4 flushes = 1 L0->L1 compaction)
                for (int batch = 4; batch < 16; batch++) {
                    for (int i = 0; i < 10; i++) {
                        int id = (batch * 10) + i;
                        db.put(String.format("k_%03d", id), "val_" + id);
                    }
                }

                // Manual compaction trigger to ensure any remaining threshold is processed
                db.triggerCompaction();

                System.out.println("Checking level sizes after L1 -> L2 Cascading Compaction:");
                System.out.println("  Level 0 count: " + lm.getLevel(0).size());
                System.out.println("  Level 1 count: " + lm.getLevel(1).size());
                System.out.println("  Level 2 count: " + lm.getLevel(2).size());

                // Verify Level 2 now holds files!
                assertTrue(lm.getLevel(2).size() > 0, "Level 2 should now contain compacted SSTable files!");

                // Step 4: Verify all 160 keys across L0, L1, and L2 are 100% intact and readable
                for (int id = 0; id < 160; id++) {
                    String key = String.format("k_%03d", id);
                    assertEquals("val_" + id, db.get(key), "Read key after L1->L2 compaction: " + key);
                }

                // Step 5: Test Tombstone Purging at bottom level
                db.delete("k_005");
                assertEquals(null, db.get("k_005"), "Deleted key k_005 should return null");
            }

            // Step 6: Verify full persistence across DB restart after L0->L1->L2 compactions
            try (SidDBEngine db = new SidDBEngine(dbDir, 10)) {
                LevelManager lm = db.getLevelManager();
                System.out.println("\nRecovered database from disk:");
                System.out.println("  Level 0 count: " + lm.getLevel(0).size());
                System.out.println("  Level 1 count: " + lm.getLevel(1).size());
                System.out.println("  Level 2 count: " + lm.getLevel(2).size());

                assertTrue(lm.getLevel(2).size() > 0, "Level 2 should still contain files upon restart");
                assertEquals(null, db.get("k_005"), "Deleted key k_005 should remain deleted");
                assertEquals("val_150", db.get("k_150"), "Key k_150 should be intact");
            }

            System.out.println("  ✓ Phase3CompactionTest passed successfully!");
        } catch (Exception e) {
            throw new RuntimeException("Phase3CompactionTest failed", e);
        } finally {
            deleteDir(new File(dbDir));
        }
    }
}
