package memtable;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable {

    // Sentinel to represent a deletion tombstone in ConcurrentSkipListMap (which disallows null values)
    private static final Object TOMBSTONE_SENTINEL = new Object();

    private final ConcurrentSkipListMap<String, Object> table;

    public MemTable() {
        this.table = new ConcurrentSkipListMap<>();
    }

    public void put(String key, Object value) {
        if (value == null) {
            table.put(key, TOMBSTONE_SENTINEL);
        } else {
            table.put(key, value);
        }
    }

    public Object get(String key) {
        Object val = table.get(key);
        if (val == TOMBSTONE_SENTINEL) {
            return null;
        }
        return val;
    }

    public boolean isTombstone(String key) {
        return table.get(key) == TOMBSTONE_SENTINEL;
    }

    public void delete(String key) {
        table.put(key, TOMBSTONE_SENTINEL);
    }

    public boolean containsKey(String key) {
        return table.containsKey(key);
    }

    public Set<String> keys() {
        return table.keySet();
    }

    public int size() {
        int count = 0;
        for (Object val : table.values()) {
            if (val != TOMBSTONE_SENTINEL) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns a sorted map of all entries, mapping tombstones to null.
     */
    public Map<String, Object> getEntries() {
        Map<String, Object> copy = new TreeMap<>();
        for (Map.Entry<String, Object> entry : table.entrySet()) {
            copy.put(entry.getKey(), entry.getValue() == TOMBSTONE_SENTINEL ? null : entry.getValue());
        }
        return copy;
    }

    public void clear() {
        table.clear();
    }
}