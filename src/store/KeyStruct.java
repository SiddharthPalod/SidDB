package store;

class KeyStruct {
    long writePos;
    int logSize;
    Object key;

    KeyStruct(long writePos, int logSize, Object key) {
        this.writePos = writePos;
        this.logSize = logSize;
        this.key = key;
    }
}
