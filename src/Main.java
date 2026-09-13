import store.DiskStore;
import java.io.File;

public class Main {
    public static void main(String[] args) {
        String dbName = "test.sb";
        
        // Ensure we start fresh for the test
        File dbFile = new File(dbName);
        if (dbFile.exists()) {
            dbFile.delete();
        }

        System.out.println("--- Phase 1: Writing to Database ---");
        try (DiskStore store = new DiskStore(dbName)) {
            System.out.println("Putting [\"greeting\"] = \"hello world\"");
            store.put("greeting", "hello world");
            
            System.out.println("Putting [\"magic_number\"] = 42");
            store.put("magic_number", 42L); // 42 as long because our packer deserializes to Long
            
            System.out.println("Putting [\"pi\"] = 3.14159");
            store.put("pi", 3.14159); // double
            
            System.out.println("Total keys currently in memory: " + store.size());
            System.out.println("Keys: " + store.keys());
            
            System.out.println("\n--- Phase 1b: Deleting from Database ---");
            System.out.println("Deleting [\"magic_number\"]");
            store.delete("magic_number");
            
            System.out.println("Total keys currently in memory after delete: " + store.size());
            System.out.println("Keys: " + store.keys());
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        System.out.println("\n--- Phase 2: Reading from Database ---");
        System.out.println("Re-opening " + dbName + " from disk...");
        
        try (DiskStore store = new DiskStore(dbName)) {
            System.out.println("Total keys loaded from disk: " + store.size());
            System.out.println("Keys: " + store.keys());
            
            System.out.println("\nFetching values:");
            System.out.println("greeting -> " + store.get("greeting"));
            System.out.println("magic_number -> " + store.get("magic_number"));
            System.out.println("pi -> " + store.get("pi"));
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
