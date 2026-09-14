package test;

import engine.SidDBEngine;
import java.io.File;

public class Phase2WALTest {

    private static void assertEquals(Object expected, Object actual, String message) {
        if (expected == null && actual == null) return;
        if (expected != null && expected.equals(actual)) return;
        throw new AssertionError(message + " | Expected: [" + expected + "], Actual: [" + actual + "]");
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
        System.out.println("\n--- [Running Phase2WALTest] ---");
        String dbDir = "phase2_test_data";
        deleteDir(new File(dbDir));

        try {
            // 1. Write mutations to WAL & MemTable (threshold 1000 so no auto flush during basic WAL test)
            try (SidDBEngine db = new SidDBEngine(dbDir, 1000)) {
                db.put("k1", "val1");
                db.put("k2", 42L);
                db.put("k3", 9.99);
                db.put("k4", "to_delete");

                assertEquals("val1", db.get("k1"), "k1 in MemTable");
                assertEquals(42L, db.get("k2"), "k2 in MemTable");
                assertEquals(4, db.size(), "MemTable size before delete");

                // 2. Delete with tombstone in WAL
                db.delete("k4");
                assertEquals(null, db.get("k4"), "k4 should be deleted from MemTable");
                assertEquals(3, db.size(), "MemTable size after delete");
            }

            // 3. Replay WAL recovery on restart
            try (SidDBEngine db = new SidDBEngine(dbDir, 1000)) {
                assertEquals(3, db.size(), "Recovered active keys count");
                assertEquals("val1", db.get("k1"), "Recovered k1");
                assertEquals(42L, db.get("k2"), "Recovered k2");
                assertEquals(9.99, db.get("k3"), "Recovered k3");
                assertEquals(null, db.get("k4"), "k4 should remain null after recovery");

                // 4. Continue writing to recovered WAL
                db.put("k5", "new_entry");
                assertEquals(4, db.size(), "Size after append to recovered DB");
            }

            // 5. Verify 2nd recovery after post-recovery writes
            try (SidDBEngine db = new SidDBEngine(dbDir, 1000)) {
                assertEquals(4, db.size(), "Size on second recovery");
                assertEquals("new_entry", db.get("k5"), "k5 recovered");
            }

            System.out.println("  ✓ Phase2WALTest passed successfully!");
        } catch (Exception e) {
            throw new RuntimeException("Phase2WALTest failed", e);
        } finally {
            deleteDir(new File(dbDir));
        }
    }
}
