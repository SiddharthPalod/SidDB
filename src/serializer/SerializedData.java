package serializer;

public class SerializedData {
    private final int size;
    private final byte[] data;

    public SerializedData(int size, byte[] data) {
        this.size = size;
        this.data = data;
    }

    public int getSize() {
        return size;
    }

    public byte[] getData() {
        return data;
    }
}
