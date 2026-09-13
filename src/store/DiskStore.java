package store;

import serializer.Serializer;
import serializer.SerializedData;
import serializer.DeserializedData;

import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DiskStore implements AutoCloseable {

    private final Serializer serializer;

    private final RandomAccessFile dbFile;

    private long writePos;

    private final Map<Object, KeyStruct> keyDir;

    /**
     * Creates a new store described by a database file.
     *
     * If the file already exists, its contents are loaded
     * into the key directory.
     *
     * @param dbFilePath path to the database file
     */
    public DiskStore() throws IOException {
        this("siddb.sb");
    }

    public DiskStore(String dbFilePath) throws IOException {

        this.serializer = new Serializer();

        /*
         * Ruby:
         * File.open(db_file, 'a+b')
         *
         * RandomAccessFile supports reading and writing.
         */
        this.dbFile = new RandomAccessFile(dbFilePath, "rw");

        this.writePos = 0;

        this.keyDir = new LinkedHashMap<>();

        initKeyDir();
    }

    /**
     * Get the value for the given key.
     *
     * Returns an empty string when key does not exist.
     */
    public Object get(Object key) throws IOException {

        KeyStruct keyStruct = keyDir.get(key);

        if (keyStruct == null) {
            return "";
        }

        /*
         * Ruby:
         * @db_fh.seek(key_struct[:write_pos])
         */
        dbFile.seek(keyStruct.writePos);

        byte[] data = new byte[keyStruct.logSize];

        dbFile.readFully(data);

        DeserializedData result =
                serializer.deserialize(data);

        if (result == null) {
            throw new IOException("File corrupted");
        }

        return result.getValue();
    }

    /**
     * Sets a new key-value pair.
     */
    public void put(Object key, Object value) throws IOException {

        SerializedData serialized =
                serializer.serialize(
                        System.currentTimeMillis() / 1000,
                        key,
                        value
                );

        int logSize = serialized.getSize();

        byte[] data = serialized.getData();

        if (value == null) {
            keyDir.remove(key);
        } else {
            keyDir.put(
                    key,
                    new KeyStruct(writePos, logSize, key)
            );
        }

        persist(data);

        incrWritePos(logSize);
    }

    /**
     * Deletes a key from the database.
     */
    public void delete(Object key) throws IOException {
        put(key, null);
    }

    /**
     * Returns all keys.
     */
    public List<Object> keys() {

        return new ArrayList<>(keyDir.keySet());
    }

    /**
     * Returns number of unique keys.
     */
    public int size() {

        return keyDir.size();
    }

    /**
     * Flushes data to disk.
     */
    public void flush() throws IOException {

        dbFile.getFD().sync();
    }

    /**
     * Closes the database.
     */
    @Override
    public void close() throws IOException {

        flush();

        dbFile.close();
    }

    /**
     * Writes data to the database and flushes.
     */
    private void persist(byte[] data) throws IOException {

        dbFile.seek(writePos);

        dbFile.write(data);

        /*
         * Ruby flushes the file buffer.
         *
         * RandomAccessFile writes directly to the file,
         * so sync() provides stronger durability.
         */
        dbFile.getFD().sync();
    }

    /**
     * Increments the write position.
     */
    private void incrWritePos(long pos) {

        writePos += pos;
    }

    /**
     * Loads existing records into the key directory.
     */
    private void initKeyDir() throws IOException {
        dbFile.seek(0);

        while (true) {
            long recordStart = writePos;
            
            DeserializedData result = serializer.readNext(dbFile);
            if (result == null) {
                break;
            }

            Object key = result.getKey();
            Object value = result.getValue();
            int logSize = result.getSize();

            if (value == null) {
                keyDir.remove(key);
            } else {
                keyDir.put(
                        key,
                        new KeyStruct(
                                recordStart,
                                logSize,
                                key
                        )
                );
            }

            incrWritePos(logSize);
        }
    }
}