package serializer;

class Header {
    long epoch;
    int keySize;
    int valueSize;
    short keyType;
    short valueType;

    Header( long epoch,int keySize,int valueSize,short keyType,short valueType) {
        this.epoch = epoch;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.keyType = keyType;
        this.valueType = valueType;
    }
}
