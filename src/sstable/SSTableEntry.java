package sstable;

public class SSTableEntry {
    private final String key;
    private final Object value;
    private final boolean isTombstone;
    private final long epoch;

    public SSTableEntry(String key, Object value, boolean isTombstone, long epoch) {
        this.key = key;
        this.value = value;
        this.isTombstone = isTombstone;
        this.epoch = epoch;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public boolean isTombstone() {
        return isTombstone;
    }

    public long getEpoch() {
        return epoch;
    }

    @Override
    public String toString() {
        return "SSTableEntry{key='" + key + "', value=" + value + ", isTombstone=" + isTombstone + "}";
    }
}
