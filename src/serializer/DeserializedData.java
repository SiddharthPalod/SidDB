package serializer;

public class DeserializedData {
    private final long epoch;
    private final Object key;
    private final Object value;
    private final int size;

    public DeserializedData(long epoch, Object key, Object value, int size) {
        this.epoch = epoch;
        this.key = key;
        this.value = value;
        this.size = size;
    }

    public long getEpoch() {
        return epoch;
    }

    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public int getSize() {
        return size;
    }
}
