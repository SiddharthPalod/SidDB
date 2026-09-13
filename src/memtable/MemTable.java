
package memtable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable {
    private final ConcurrentSkipListMap<String, Object> table;

    public MemTable() {
        this.table = new ConcurrentSkipListMap<>();
    }

    public void put(String key, Object value) {
        if (value == null) {
            table.remove(key);
        } else {
            table.put(key, value);
        }
    }

    public Object get(String key) {
        return table.get(key);
    }

    public void delete(String key) {
        table.remove(key);
    }

    public boolean containsKey(String key) {
        return table.containsKey(key);
    }
    public Set<String> keys() {
        return table.keySet();
    }
    public int size() {
        return table.size();
    }
    public Map<String, Object> getEntries() {
        return Collections.unmodifiableMap(table);
    }
    public void clear() {
        table.clear();
    }
}