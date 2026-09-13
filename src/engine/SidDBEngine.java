package engine;
import memtable.MemTable;
import serializer.DeserializedData;
import wal.WAL;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class SidDBEngine implements AutoCloseable {
    private final MemTable memTable;
    private final WAL wal;

    public SidDBEngine() throws IOException {
        this("siddb.wal");
    }

    public SidDBEngine(String walPath) throws IOException {
        this.memTable = new MemTable();
        this.wal = new WAL(walPath);
        recover();
    }

    private void recover() throws IOException {
        System.out.println("[Engine] Replaying WAL for crash recovery...");
        List<DeserializedData> entries = wal.replay();

        for (DeserializedData entry : entries) {
            String key = entry.getKey().toString();
            Object value = entry.getValue();
            memTable.put(key, value);
        }
        System.out.println("[Engine] Recovery complete! " + memTable.size() + " active keys loaded into MemTable.");
    }

    public synchronized void put(String key, Object value) throws IOException {
        wal.append(key, value);
        memTable.put(key, value);
    }

    public Object get(String key) {
        return memTable.get(key);
    }

    public synchronized void delete(String key) throws IOException {
        wal.append(key, null);
        memTable.delete(key);
    }

    public Set<String> keys() {
        return memTable.keys();
    }

    public int size() {
        return memTable.size();
    }

    @Override
    public void close() throws IOException {
        wal.close();
    }
}