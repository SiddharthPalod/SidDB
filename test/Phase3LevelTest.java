package test;

import engine.SidDBEngine;
import java.io.File;

public class Phase3LevelTest {

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
        System.out.println("\n--- [Running Phase3LevelTest (MemTable Flush & L0 Hierarchy)] ---");
        String dbDir = "test_lsm_data";
        deleteDir(new File(dbDir));

        try {
            // 1. Start Engine with a small threshold of 5 entries to trigger flushes
            try (SidDBEngine db = new SidDBEngine(dbDir, 5)) {
                // Write 12 entries (Will trigger 2 flushes to Level 0: 5 + 5 + 2 in MemTable)
                for (int i = 1; i <= 12; i++) {
                    String key = String.format("k_%02d", i);
                    db.put(key, String.format("val_%02d", i));
                }

                // Verify L0 has 2 flushed SSTables and active MemTable has 2 entries
                int l0Tables = db.getLevelManager().getLevel(0).size();
                assertEquals(2, l0Tables, "Level 0 should contain 2 flushed SSTables");
                assertEquals(2, db.getActiveMemTable().size(), "Active MemTable should contain 2 un-flushed entries");

                // 2. Test Reads across both L0 SSTables and active MemTable
                assertEquals("val_01", db.get("k_01"), "Read key from 1st L0 SSTable");
                assertEquals("val_07", db.get("k_07"), "Read key from 2nd L0 SSTable");
                assertEquals("val_11", db.get("k_11"), "Read key from active MemTable");
                assertEquals(null, db.get("non_existent"), "Read non-existent key");

                // 3. Test In-Memory Overwrites shadowing older L0 SSTables
                db.put("k_01", "val_01_OVERWRITTEN_IN_RAM");
                assertEquals("val_01_OVERWRITTEN_IN_RAM", db.get("k_01"), "RAM overwrite should shadow L0 SSTable");

                // 4. Test In-Memory Deletion (Tombstone) shadowing older L0 SSTables
                db.delete("k_02");
                assertEquals(null, db.get("k_02"), "Tombstone in RAM should shadow older L0 SSTable value");
            }

            // 5. Test Full Crash Recovery (Re-opening database from MANIFEST.sb and WAL)
            try (SidDBEngine db = new SidDBEngine(dbDir, 5)) {
                int l0Tables = db.getLevelManager().getLevel(0).size();
                assertEquals(2, l0Tables, "Recovered L0 should still contain 2 SSTables");

                // Check recovered state
                assertEquals("val_01_OVERWRITTEN_IN_RAM", db.get("k_01"), "Recovered overwritten k_01");
                assertEquals(null, db.get("k_02"), "Recovered deleted k_02 should stay null");
                assertEquals("val_07", db.get("k_07"), "Recovered k_07 from 2nd L0 SSTable");
                assertEquals("val_11", db.get("k_11"), "Recovered k_11 from replayed WAL");
                assertEquals("val_12", db.get("k_12"), "Recovered k_12 from replayed WAL");
            }

            System.out.println("  ✓ Phase3LevelTest passed successfully!");
        } catch (Exception e) {
            throw new RuntimeException("Phase3LevelTest failed", e);
        } finally {
            deleteDir(new File(dbDir));
        }
    }
}
