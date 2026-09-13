import store.DiskStore;
import engine.SidDBEngine;
import java.io.File;

public class Main {
    public static void main(String[] args) {
        System.out.println("==========================================================");
        System.out.println("               SidDB Interactive Demo                     ");
        System.out.println("==========================================================\n");

        // -----------------------------------------------------------
        // 1. Phase 1: Bitcask DiskStore Demo (.sb file)
        // -----------------------------------------------------------
        System.out.println("--- [Phase 1 Demo] Bitcask DiskStore ---");
        String sbFile = "demo.sb";
        new File(sbFile).delete();

        try (DiskStore store = new DiskStore(sbFile)) {
            System.out.println("Putting [\"greeting\"] = \"hello world\"");
            store.put("greeting", "hello world");

            System.out.println("Putting [\"magic_number\"] = 42");
            store.put("magic_number", 42L);

            System.out.println("Putting [\"pi\"] = 3.14159");
            store.put("pi", 3.14159);

            System.out.println("Keys in memory: " + store.keys());

            System.out.println("Deleting [\"magic_number\"]...");
            store.delete("magic_number");

            System.out.println("Keys after deletion: " + store.keys());
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("\nRe-opening " + sbFile + " from disk to verify persistence...");
        try (DiskStore store = new DiskStore(sbFile)) {
            System.out.println("Recovered keys: " + store.keys());
            System.out.println("greeting     -> " + store.get("greeting"));
            System.out.println("magic_number -> " + store.get("magic_number") + " (deleted)");
            System.out.println("pi           -> " + store.get("pi"));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            new File(sbFile).delete();
        }

        // -----------------------------------------------------------
        // 2. Phase 2: WAL + MemTable Engine Demo (.wal file)
        // -----------------------------------------------------------
        System.out.println("\n\n--- [Phase 2 Demo] WAL + MemTable Engine ---");
        String walFile = "demo.wal";
        new File(walFile).delete();

        try (SidDBEngine db = new SidDBEngine(walFile)) {
            System.out.println("Writing mutations to WAL & MemTable...");
            db.put("user_101", "Alice");
            db.put("user_102", "Bob");
            db.put("session_id", "xyz-987");

            System.out.println("Active keys in MemTable: " + db.keys());

            System.out.println("Deleting 'session_id' (logs tombstone to WAL)...");
            db.delete("session_id");

            System.out.println("Keys in MemTable: " + db.keys());
            System.out.println("[Simulating crash / process exit...]");
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("\nRestarting Engine (Replaying WAL for Crash Recovery)...");
        try (SidDBEngine db = new SidDBEngine(walFile)) {
            System.out.println("Recovered keys: " + db.keys());
            System.out.println("user_101   -> " + db.get("user_101"));
            System.out.println("user_102   -> " + db.get("user_102"));
            System.out.println("session_id -> " + db.get("session_id") + " (null)");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            new File(walFile).delete();
        }

        System.out.println("\n==========================================================");
        System.out.println(" Demo Finished! Run 'test.TestRunner' for unit tests.     ");
        System.out.println("==========================================================");
    }
}
