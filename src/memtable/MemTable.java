package memtable;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable {

    public static class ValueEntry {
        private final Object value;
        private final boolean isTombstone;
        private final long sequenceNumber;

        public ValueEntry(Object value, boolean isTombstone, long sequenceNumber) {
            this.value = value;
            this.isTombstone = isTombstone;
            this.sequenceNumber = sequenceNumber;
        }

        public Object getValue() {
            return value;
        }

        public boolean isTombstone() {
            return isTombstone;
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }
    }

    // Maps: userKey -> (sequenceNumber DESC -> ValueEntry) for MVCC multi-versioning
    private final ConcurrentSkipListMap<String, NavigableMap<Long, ValueEntry>> table;

    public MemTable() {
        this.table = new ConcurrentSkipListMap<>();
    }

    public synchronized void put(String key, Object value, long sequenceNumber) {
        boolean isTombstone = (value == null);
        table.computeIfAbsent(key, k -> new TreeMap<>(Collections.reverseOrder()))
             .put(sequenceNumber, new ValueEntry(value, isTombstone, sequenceNumber));
    }

    public synchronized void put(String key, Object value) {
        put(key, value, System.currentTimeMillis() / 1000);
    }

    public synchronized Object get(String key) {
        return get(key, Long.MAX_VALUE);
    }

    public synchronized ValueEntry getEntry(String key, long maxSequenceNumber) {
        NavigableMap<Long, ValueEntry> versions = table.get(key);
        if (versions == null) {
            return null;
        }
        // Find the newest version <= maxSequenceNumber
        for (Map.Entry<Long, ValueEntry> entry : versions.entrySet()) {
            if (entry.getKey() <= maxSequenceNumber) {
                return entry.getValue();
            }
        }
        return null;
    }

    public synchronized Object get(String key, long maxSequenceNumber) {
        ValueEntry entry = getEntry(key, maxSequenceNumber);
        if (entry == null || entry.isTombstone()) {
            return null;
        }
        return entry.getValue();
    }

    public synchronized boolean isTombstone(String key) {
        ValueEntry entry = getEntry(key, Long.MAX_VALUE);
        return entry != null && entry.isTombstone();
    }

    public synchronized boolean containsKey(String key) {
        return containsKey(key, Long.MAX_VALUE);
    }

    public synchronized boolean containsKey(String key, long maxSequenceNumber) {
        return getEntry(key, maxSequenceNumber) != null;
    }

    public synchronized void delete(String key, long sequenceNumber) {
        put(key, null, sequenceNumber);
    }

    public synchronized void delete(String key) {
        delete(key, System.currentTimeMillis() / 1000);
    }

    public synchronized Set<String> keys() {
        Set<String> activeKeys = new TreeSet<>();
        for (Map.Entry<String, NavigableMap<Long, ValueEntry>> entry : table.entrySet()) {
            ValueEntry latest = entry.getValue().firstEntry() != null ? entry.getValue().firstEntry().getValue() : null;
            if (latest != null && !latest.isTombstone()) {
                activeKeys.add(entry.getKey());
            }
        }
        return activeKeys;
    }

    public synchronized int size() {
        return keys().size();
    }

    public synchronized Map<String, Object> getEntries() {
        Map<String, Object> latestEntries = new TreeMap<>();
        for (Map.Entry<String, NavigableMap<Long, ValueEntry>> entry : table.entrySet()) {
            ValueEntry latest = entry.getValue().firstEntry() != null ? entry.getValue().firstEntry().getValue() : null;
            if (latest != null) {
                latestEntries.put(entry.getKey(), latest.isTombstone() ? null : latest.getValue());
            }
        }
        return latestEntries;
    }

    public synchronized List<sstable.SSTableEntry> getLatestSSTableEntries() {
        List<sstable.SSTableEntry> entries = new ArrayList<>();
        for (Map.Entry<String, NavigableMap<Long, ValueEntry>> entry : table.entrySet()) {
            ValueEntry latest = entry.getValue().firstEntry() != null ? entry.getValue().firstEntry().getValue() : null;
            if (latest != null) {
                entries.add(new sstable.SSTableEntry(entry.getKey(), latest.getValue(), latest.isTombstone(), latest.getSequenceNumber()));
            }
        }
        return entries;
    }

    public synchronized void clear() {
        table.clear();
    }
}