package test;

import engine.SidDBEngine;
import tx.Snapshot;
import tx.WriteBatch;

import java.io.File;

public class Phase4ACIDTest {

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
        System.out.println("\n--- [Running Phase4ACIDTest (Atomic WriteBatch & MVCC Snapshot Isolation)] ---");
        String dbDir = "test_acid_data";
        deleteDir(new File(dbDir));

        try {
            Snapshot snap1;

            try (SidDBEngine db = new SidDBEngine(dbDir, 100)) {
                // 1. Initialize balances
                db.put("acc_A", 1000L);
                db.put("acc_B", 500L);

                // 2. Test Multi-Key Atomic WriteBatch (Bank Transfer: A -> B $200)
                System.out.println("Executing Atomic WriteBatch (Transfer $200 from A to B)...");
                WriteBatch transfer = new WriteBatch()
                        .put("acc_A", 800L)
                        .put("acc_B", 700L);
                db.write(transfer);

                assertEquals(800L, db.get("acc_A"), "Account A balance after atomic batch");
                assertEquals(700L, db.get("acc_B"), "Account B balance after atomic batch");

                // 3. Capture Snapshot S1 (Point-in-Time Snapshot Isolation)
                snap1 = db.getSnapshot();
                System.out.println("Captured Snapshot S1: " + snap1);

                // 4. Perform further mutations AFTER Snapshot S1
                System.out.println("Performing new mutations after Snapshot S1...");
                db.put("acc_A", 9999L);
                db.put("acc_NEW", "invisible_to_snap1");
                db.delete("acc_B"); // Deleted after snapshot

                // 5. Verify Isolation: Snapshot S1 must see historical state!
                System.out.println("Verifying Snapshot Isolation on S1 reads:");
                assertEquals(800L, db.get("acc_A", snap1), "acc_A under Snapshot S1 must return 800 (not 9999)");
                assertEquals(700L, db.get("acc_B", snap1), "acc_B under Snapshot S1 must return 700 (not deleted)");
                assertEquals(null, db.get("acc_NEW", snap1), "acc_NEW under Snapshot S1 must be null (invisible)");

                // 6. Verify Latest Unversioned Reads see the current state
                assertEquals(9999L, db.get("acc_A"), "Current unversioned read sees latest 9999");
                assertEquals(null, db.get("acc_B"), "Current unversioned read sees deleted acc_B");
                assertEquals("invisible_to_snap1", db.get("acc_NEW"), "Current unversioned read sees acc_NEW");
            }

            // 7. Test Durability: Restart DB and verify all committed transactions survive
            try (SidDBEngine db = new SidDBEngine(dbDir, 100)) {
                assertEquals(9999L, db.get("acc_A"), "Recovered acc_A after restart");
                assertEquals(null, db.get("acc_B"), "Recovered deleted acc_B after restart");
                assertEquals("invisible_to_snap1", db.get("acc_NEW"), "Recovered acc_NEW after restart");
            }

            System.out.println("  ✓ Phase4ACIDTest passed successfully!");
        } catch (Exception e) {
            throw new RuntimeException("Phase4ACIDTest failed", e);
        } finally {
            deleteDir(new File(dbDir));
        }
    }
}
