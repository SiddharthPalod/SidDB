package serializer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public enum DataType {
    INTEGER((short) 1) {
        @Override
        public byte[] pack(Object value) {
            if (!(value instanceof Number)) {
                throw new IllegalArgumentException("Expected numeric attribute");
            }
            long integerValue = ((Number) value).longValue();
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putLong(integerValue);
            return buffer.array();
        }

        @Override
        public Object unpack(byte[] bytes) {
            return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }

        @Override
        public boolean matches(Object value) {
            return value instanceof Integer || value instanceof Long || 
                   value instanceof Short || value instanceof Byte;
        }
    },
    FLOAT((short) 2) {
        @Override
        public byte[] pack(Object value) {
            if (!(value instanceof Number)) {
                throw new IllegalArgumentException("Expected numeric attribute");
            }
            double doubleValue = ((Number) value).doubleValue();
            ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
            buffer.putDouble(doubleValue);
            return buffer.array();
        }

        @Override
        public Object unpack(byte[] bytes) {
            return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getDouble();
        }

        @Override
        public boolean matches(Object value) {
            return value instanceof Float || value instanceof Double;
        }
    },
    STRING((short) 3) {
        @Override
        public byte[] pack(Object value) {
            return value.toString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public Object unpack(byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public boolean matches(Object value) {
            return value instanceof String;
        }
    },
    TOMBSTONE((short) 4) {
        @Override
        public byte[] pack(Object value) {
            return new byte[0];
        }

        @Override
        public Object unpack(byte[] bytes) {
            return null;
        }

        @Override
        public boolean matches(Object value) {
            return value == null;
        }
    };

    private final short code;

    DataType(short code) {
        this.code = code;
    }

    public short getCode() {
        return code;
    }

    public abstract byte[] pack(Object value);
    public abstract Object unpack(byte[] bytes);
    public abstract boolean matches(Object value);

    public static DataType fromCode(short code) {
        for (DataType type : values()) {
            if (type.getCode() == code) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid attribute_type");
    }

    public static DataType fromObject(Object value) {
        for (DataType type : values()) {
            if (type.matches(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported attribute type: " + value.getClass().getName());
    }
}
