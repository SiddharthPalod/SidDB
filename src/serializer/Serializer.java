package serializer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32;
import java.io.RandomAccessFile;
import java.io.IOException;

public class Serializer {

    public static final int HEADER_SIZE = 16;
    public static final int CRC32_SIZE = 4;

    private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    public SerializedData serialize(long epoch, Object key, Object value) {
        DataType keyType = DataType.fromObject(key);
        DataType valueType = DataType.fromObject(value);

        byte[] keyBytes = keyType.pack(key);
        byte[] valueBytes = valueType.pack(value);

        byte[] header = serializeHeader(
                epoch,
                keyBytes.length,
                keyType.getCode(),
                valueBytes.length,
                valueType.getCode()
        );

        byte[] data = concatenate(keyBytes, valueBytes);
        byte[] crc = crc32(concatenate(header, data));
        byte[] result = concatenate(crc, header, data);

        int size = CRC32_SIZE + HEADER_SIZE + data.length;
        return new SerializedData(size, result);
    }

    public DeserializedData deserialize(byte[] data) {
        if (data == null || data.length < CRC32_SIZE + HEADER_SIZE) {
            return null;
        }

        long storedCrc = deserializeCrc32(data);
        byte[] payload = slice(data, CRC32_SIZE, data.length);

        if (!crc32Valid(storedCrc, payload)) {
            return null;
        }

        byte[] headerData = slice(data, CRC32_SIZE, CRC32_SIZE + HEADER_SIZE);
        Header header = deserializeHeader(headerData);

        int keyStart = CRC32_SIZE + HEADER_SIZE;
        int keyEnd = keyStart + header.keySize;

        byte[] keyBytes = slice(data, keyStart, keyEnd);
        byte[] valueBytes = slice(data, keyEnd, data.length);

        Object key = DataType.fromCode(header.keyType).unpack(keyBytes);
        Object value = DataType.fromCode(header.valueType).unpack(valueBytes);

        return new DeserializedData(header.epoch, key, value, data.length);
    }

    public DeserializedData readNext(RandomAccessFile file) throws IOException {
        byte[] crcAndHeader = new byte[CRC32_SIZE + HEADER_SIZE];
        int total = 0;
        
        while (total < crcAndHeader.length) {
            int read = file.read(crcAndHeader, total, crcAndHeader.length - total);
            if (read == -1) {
                if (total == 0) return null;
                break;
            }
            total += read;
        }

        if (total != crcAndHeader.length) {
            throw new IOException("File corrupted");
        }

        byte[] headerData = slice(crcAndHeader, CRC32_SIZE, CRC32_SIZE + HEADER_SIZE);
        Header header = deserializeHeader(headerData);

        if (header.keySize < 0 || header.valueSize < 0) {
            throw new IOException("File corrupted");
        }

        byte[] keyBytes = new byte[header.keySize];
        byte[] valueBytes = new byte[header.valueSize];

        file.readFully(keyBytes);
        file.readFully(valueBytes);

        int recordSize = CRC32_SIZE + HEADER_SIZE + header.keySize + header.valueSize;
        byte[] record = new byte[recordSize];

        System.arraycopy(crcAndHeader, 0, record, 0, crcAndHeader.length);
        System.arraycopy(keyBytes, 0, record, crcAndHeader.length, header.keySize);
        System.arraycopy(valueBytes, 0, record, crcAndHeader.length + header.keySize, header.valueSize);

        DeserializedData result = deserialize(record);

        if (result == null) {
            throw new IOException("File corrupted");
        }

        return result;
    }

    private byte[] serializeHeader(
            long epoch,
            int keySize,
            short keyType,
            int valueSize,
            short valueType
    ) {
        ByteBuffer buffer = ByteBuffer
                .allocate(HEADER_SIZE)
                .order(BYTE_ORDER);

        buffer.putInt((int) epoch);
        buffer.putInt(keySize);
        buffer.putInt(valueSize);
        buffer.putShort(keyType);
        buffer.putShort(valueType);

        return buffer.array();
    }

    private Header deserializeHeader(byte[] headerData) {
        if (headerData.length != HEADER_SIZE) {
            throw new IllegalArgumentException("Invalid header size");
        }

        ByteBuffer buffer = ByteBuffer
                .wrap(headerData)
                .order(BYTE_ORDER);

        long epoch = Integer.toUnsignedLong(buffer.getInt());
        int keySize = buffer.getInt();
        int valueSize = buffer.getInt();
        short keyType = buffer.getShort();
        short valueType = buffer.getShort();

        return new Header(epoch, keySize, valueSize, keyType, valueType);
    }

    private byte[] crc32(byte[] dataBytes) {
        CRC32 crc = new CRC32();
        crc.update(dataBytes);
        long checksum = crc.getValue();

        ByteBuffer buffer = ByteBuffer
                .allocate(CRC32_SIZE)
                .order(BYTE_ORDER);

        buffer.putInt((int) checksum);
        return buffer.array();
    }

    private long deserializeCrc32(byte[] data) {
        ByteBuffer buffer = ByteBuffer
                .wrap(data, 0, CRC32_SIZE)
                .order(BYTE_ORDER);
        return Integer.toUnsignedLong(buffer.getInt());
    }

    private boolean crc32Valid(long digest, byte[] dataBytes) {
        CRC32 crc = new CRC32();
        crc.update(dataBytes);
        return digest == crc.getValue();
    }

    private byte[] concatenate(byte[]... arrays) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (byte[] array : arrays) {
            output.write(array, 0, array.length);
        }
        return output.toByteArray();
    }

    private byte[] slice(byte[] data, int start, int end) {
        byte[] result = new byte[end - start];
        System.arraycopy(data, start, result, 0, result.length);
        return result;
    }
}