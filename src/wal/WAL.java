package wal;
import serializer.DeserializedData;
import serializer.Serializer;
import serializer.SerializedData;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class WAL implements AutoCloseable {
    private final File walFile;
    private final RandomAccessFile fileHandle;
    private final Serializer serializer;
    public WAL(String walPath) throws IOException {
        this.walFile = new File(walPath);
        this.fileHandle = new RandomAccessFile(this.walFile, "rw");
        this.serializer = new Serializer();
        this.fileHandle.seek(this.fileHandle.length());
    }

    // Appends a mutation (PUT or DELETE) to the WAL and forces a disk sync.
    public synchronized void append(String key, Object value) throws IOException {
        long epoch = System.currentTimeMillis() / 1000;
        SerializedData serialized = serializer.serialize(epoch, key, value);
        fileHandle.write(serialized.getData());        
        fileHandle.getFD().sync();
    }

    // Replays the entire WAL from beginning to recover memory state after a crash. 
    public synchronized List<DeserializedData> replay() throws IOException {
        List<DeserializedData> records = new ArrayList<>();
        fileHandle.seek(0);
        while (true) {
            try {
                DeserializedData data = serializer.readNext(fileHandle);
                if (data == null) {
                    break; // End of file reached safely
                }
                records.add(data);
            } catch (IOException e) {
                // If a partial record is encountered at the end (crash during write),
                // truncate to the last known good position.
                System.err.println("[WAL Recovery] Reached incomplete or corrupted trailing write: " + e.getMessage());
                break;
            }
        }
        fileHandle.seek(fileHandle.length());
        return records;
    }
    
    @Override
    public synchronized void close() throws IOException {
        fileHandle.getFD().sync();
        fileHandle.close();
    }
}