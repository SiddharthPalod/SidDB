package test;

import store.DiskStore;
import java.io.File;

public class Phase1DiskStoreTest {

    private static void assertEquals(Object expected, Object actual, String message) {
        if (expected == null && actual == null) return;
        if (expected != null && expected.equals(actual)) return;
        throw new AssertionError(message + " | Expected: [" + expected + "], Actual: [" + actual + "]");
    }

    public static void run() {
        System.out.println("==========================================================");
        System.out.println(" [PHASE 1 TEST] Bitcask DiskStore (KV Store with .sb file)");
        System.out.println("==========================================================");
        
        String dbName = "test_phase1.sb";
        File dbFile = new File(dbName);
        if (dbFile.exists()) {
            dbFile.delete();
        }

        System.out.println("\n--- Step 1: Writing to Database ---");
        try (DiskStore store = new DiskStore(dbName)) {
            System.out.println("Putting [\"greeting\"] = \"hello world\"");
            store.put("greeting", "hello world");
            
            System.out.println("Putting [\"magic_number\"] = 42");
            store.put("magic_number", 42L);
            
            System.out.println("Putting [\"pi\"] = 3.14159");
            store.put("pi", 3.14159);
            
            System.out.println("Total keys in memory: " + store.size());
            System.out.println("Keys: " + store.keys());
            
            assertEquals("hello world", store.get("greeting"), "Check greeting");
            assertEquals(42L, store.get("magic_number"), "Check magic_number");
            assertEquals(3.14159, store.get("pi"), "Check pi");
            assertEquals(3, store.size(), "Store size before delete");

            System.out.println("\n--- Step 1b: Deleting from Database ---");
            System.out.println("Deleting [\"magic_number\"]");
            store.delete("magic_number");
            
            System.out.println("Total keys in memory after delete: " + store.size());
            System.out.println("Keys: " + store.keys());
            
            assertEquals(2, store.size(), "Store size after delete");
            assertEquals("", store.get("magic_number"), "Deleted key returns empty");
        } catch (Exception e) {
            throw new RuntimeException("Phase 1 Step 1 failed", e);
        }

        System.out.println("\n--- Step 2: Reading from Database (Crash/Restart Persistence) ---");
        System.out.println("Re-opening " + dbName + " from disk...");
        
        try (DiskStore store = new DiskStore(dbName)) {
            System.out.println("Total keys loaded from disk: " + store.size());
            System.out.println("Keys: " + store.keys());
            
            System.out.println("\nFetching values:");
            Object greeting = store.get("greeting");
            Object magicNum = store.get("magic_number");
            Object pi = store.get("pi");
            
            System.out.println("greeting     -> " + greeting);
            System.out.println("magic_number -> " + magicNum + " (deleted)");
            System.out.println("pi           -> " + pi);
            
            assertEquals("hello world", greeting, "greeting persisted");
            assertEquals("", magicNum, "magic_number stays deleted");
            assertEquals(3.14159, pi, "pi persisted");
            assertEquals(2, store.size(), "persisted size is 2");

            System.out.println("\n  ✓ Phase1DiskStoreTest passed successfully!");
        } catch (Exception e) {
            throw new RuntimeException("Phase 1 Step 2 failed", e);
        } finally {
            if (dbFile.exists()) {
                dbFile.delete();
            }
        }
    }
}
