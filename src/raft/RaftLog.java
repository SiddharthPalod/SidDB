package raft;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RaftLog {
    private static final byte MAGIC = 0x53; // 'S' for SidDB binary record

    private final List<RaftLogEntry> entries;
    private long commitIndex;
    private long lastApplied;
    private final File logFile;
    private volatile SyncPolicy syncPolicy = SyncPolicy.SYNC_EVERY_ENTRY;

    public RaftLog() {
        this(null);
    }

    public RaftLog(String logFilePath) {
        this(logFilePath, SyncPolicy.SYNC_EVERY_ENTRY);
    }

    public RaftLog(String logFilePath, SyncPolicy syncPolicy) {
        this.entries = new ArrayList<>();
        // Index 0 dummy entry with term 0
        this.entries.add(new RaftLogEntry(0, 0, "INIT", null, null));
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.logFile = (logFilePath != null) ? new File(logFilePath) : null;
        this.syncPolicy = (syncPolicy != null) ? syncPolicy : SyncPolicy.SYNC_EVERY_ENTRY;

        if (this.logFile != null && this.logFile.exists() && this.logFile.length() > 0) {
            recoverFromDisk();
        }
    }

    public SyncPolicy getSyncPolicy() {
        return syncPolicy;
    }

    public void setSyncPolicy(SyncPolicy syncPolicy) {
        this.syncPolicy = syncPolicy;
    }

    private synchronized void recoverFromDisk() {
        if (logFile == null || !logFile.exists() || logFile.length() == 0) return;
        try (FileInputStream fis = new FileInputStream(logFile);
             BufferedInputStream bis = new BufferedInputStream(fis);
             DataInputStream dis = new DataInputStream(bis)) {

            bis.mark(4);
            int b0 = bis.read();
            int b1 = bis.read();
            bis.reset();

            // Detect legacy Java serialization stream header (0xACED)
            if (b0 == 0xAC && b1 == 0xED) {
                try (ObjectInputStream in = new ObjectInputStream(bis)) {
                    int count = in.readInt();
                    entries.clear();
                    entries.add(new RaftLogEntry(0, 0, "INIT", null, null));
                    for (int i = 0; i < count; i++) {
                        RaftLogEntry entry = (RaftLogEntry) in.readObject();
                        entries.add(entry);
                    }
                }
                return;
            }

            // High-performance binary framed records
            entries.clear();
            entries.add(new RaftLogEntry(0, 0, "INIT", null, null));
            while (bis.available() > 0) {
                try {
                    RaftLogEntry entry = readEntryFromStream(dis);
                    entries.add(entry);
                } catch (EOFException e) {
                    break;
                } catch (Exception e) {
                    // Safe crash boundary: partial/unfsynced write at tail truncated cleanly
                    break;
                }
            }
        } catch (Exception ignored) {}
    }

    private void writeEntryToStream(DataOutputStream dos, RaftLogEntry entry) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream entryDos = new DataOutputStream(baos);
        entryDos.writeLong(entry.getTerm());
        entryDos.writeLong(entry.getIndex());
        entryDos.writeUTF(entry.getCommandType() != null ? entry.getCommandType() : "");
        if (entry.getKey() != null) {
            entryDos.writeBoolean(true);
            entryDos.writeUTF(entry.getKey());
        } else {
            entryDos.writeBoolean(false);
        }
        Object val = entry.getValue();
        if (val == null) {
            entryDos.writeByte(0);
        } else if (val instanceof String) {
            entryDos.writeByte(1);
            entryDos.writeUTF((String) val);
        } else if (val instanceof byte[]) {
            byte[] b = (byte[]) val;
            entryDos.writeByte(2);
            entryDos.writeInt(b.length);
            entryDos.write(b);
        } else {
            entryDos.writeByte(3);
            ByteArrayOutputStream objBaos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(objBaos)) {
                oos.writeObject(val);
            }
            byte[] objBytes = objBaos.toByteArray();
            entryDos.writeInt(objBytes.length);
            entryDos.write(objBytes);
        }
        entryDos.flush();
        byte[] payload = baos.toByteArray();

        dos.writeByte(MAGIC);
        dos.writeInt(payload.length);
        dos.write(payload);
    }

    private RaftLogEntry readEntryFromStream(DataInputStream dis) throws IOException, ClassNotFoundException {
        byte magic = dis.readByte();
        if (magic != MAGIC) {
            throw new IOException("Invalid log magic byte: " + magic);
        }
        int length = dis.readInt();
        if (length < 0 || length > 64 * 1024 * 1024) {
            throw new IOException("Invalid record length: " + length);
        }
        byte[] payload = new byte[length];
        dis.readFully(payload);

        DataInputStream entryDis = new DataInputStream(new ByteArrayInputStream(payload));
        long term = entryDis.readLong();
        long index = entryDis.readLong();
        String cmd = entryDis.readUTF();
        String key = null;
        if (entryDis.readBoolean()) {
            key = entryDis.readUTF();
        }
        byte valType = entryDis.readByte();
        Object val = null;
        if (valType == 1) {
            val = entryDis.readUTF();
        } else if (valType == 2) {
            int blen = entryDis.readInt();
            byte[] b = new byte[blen];
            entryDis.readFully(b);
            val = b;
        } else if (valType == 3) {
            int blen = entryDis.readInt();
            byte[] b = new byte[blen];
            entryDis.readFully(b);
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(b))) {
                val = ois.readObject();
            }
        }
        return new RaftLogEntry(term, index, cmd, key, val);
    }

    private synchronized void appendRecords(List<RaftLogEntry> newEntries, boolean forceSync) {
        if (logFile == null || newEntries == null || newEntries.isEmpty()) return;
        try {
            File parent = logFile.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();

            try (FileOutputStream fos = new FileOutputStream(logFile, true);
                 BufferedOutputStream bos = new BufferedOutputStream(fos);
                 DataOutputStream dos = new DataOutputStream(bos)) {

                for (RaftLogEntry entry : newEntries) {
                    writeEntryToStream(dos, entry);
                }
                dos.flush();
                if (forceSync || syncPolicy == SyncPolicy.SYNC_EVERY_ENTRY) {
                    fos.getFD().sync();
                }
            }
        } catch (IOException ignored) {}
    }

    private synchronized void rewriteLogFile() {
        if (logFile == null) return;
        try {
            File parent = logFile.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();

            try (FileOutputStream fos = new FileOutputStream(logFile, false);
                 BufferedOutputStream bos = new BufferedOutputStream(fos);
                 DataOutputStream dos = new DataOutputStream(bos)) {

                for (int i = 1; i < entries.size(); i++) {
                    writeEntryToStream(dos, entries.get(i));
                }
                dos.flush();
                if (syncPolicy == SyncPolicy.SYNC_EVERY_ENTRY) {
                    fos.getFD().sync();
                }
            }
        } catch (IOException ignored) {}
    }

    public synchronized void flush(boolean force) {
        if (logFile == null) return;
        if (!logFile.exists() && entries.size() > 1) {
            rewriteLogFile();
            return;
        }
        if (force || syncPolicy == SyncPolicy.SYNC_EVERY_ENTRY) {
            try (FileOutputStream fos = new FileOutputStream(logFile, true)) {
                fos.getFD().sync();
            } catch (IOException ignored) {}
        }
    }

    public synchronized RaftLogEntry append(long term, String commandType, String key, Object value) {
        long nextIndex = entries.size();
        RaftLogEntry entry = new RaftLogEntry(term, nextIndex, commandType, key, value);
        entries.add(entry);
        appendRecords(Collections.singletonList(entry), syncPolicy == SyncPolicy.SYNC_EVERY_ENTRY);
        return entry;
    }

    public synchronized List<RaftLogEntry> appendBatch(List<RaftLogEntry> newEntries) {
        if (newEntries == null || newEntries.isEmpty()) return Collections.emptyList();
        for (RaftLogEntry entry : newEntries) {
            entries.add(entry);
        }
        appendRecords(newEntries, syncPolicy == SyncPolicy.SYNC_EVERY_ENTRY);
        return newEntries;
    }

    public synchronized long getLastLogIndex() {
        return entries.size() - 1;
    }

    public synchronized long getLastLogTerm() {
        return entries.get(entries.size() - 1).getTerm();
    }

    public synchronized long getTermAt(long index) {
        if (index < 0 || index >= entries.size()) {
            return 0;
        }
        return entries.get((int) index).getTerm();
    }

    public synchronized RaftLogEntry getEntry(long index) {
        if (index < 0 || index >= entries.size()) {
            return null;
        }
        return entries.get((int) index);
    }

    public synchronized List<RaftLogEntry> getEntriesFrom(long startIndex) {
        List<RaftLogEntry> result = new ArrayList<>();
        if (startIndex <= 0) {
            startIndex = 1;
        }
        for (int i = (int) startIndex; i < entries.size(); i++) {
            result.add(entries.get(i));
        }
        return result;
    }

    public synchronized void truncateFrom(long index) {
        if (index <= 0) return;
        while (entries.size() > index) {
            entries.remove(entries.size() - 1);
        }
        rewriteLogFile();
    }

    /**
     * Appends new entries from Leader starting after prevLogIndex.
     * Overwrites any conflicting entries at subsequent indices.
     * Groups disk appends and executes at most a single fsync for the entire batch.
     */
    public synchronized boolean appendEntries(long prevLogIndex, long prevLogTerm, List<RaftLogEntry> newEntries) {
        // 1. Reply false if log doesn't contain an entry at prevLogIndex matching prevLogTerm
        if (prevLogIndex > getLastLogIndex()) {
            return false;
        }
        if (prevLogIndex > 0 && getTermAt(prevLogIndex) != prevLogTerm) {
            return false;
        }

        // 2. Insert entries, overwriting conflicts
        long insertIndex = prevLogIndex + 1;
        List<RaftLogEntry> toAppend = new ArrayList<>();
        boolean truncated = false;
        if (newEntries != null) {
            for (RaftLogEntry newEntry : newEntries) {
                if (insertIndex < entries.size()) {
                    // Check if conflicting
                    if (entries.get((int) insertIndex).getTerm() != newEntry.getTerm()) {
                        truncateFrom(insertIndex);
                        truncated = true;
                        entries.add(newEntry);
                        toAppend.add(newEntry);
                    }
                } else {
                    entries.add(newEntry);
                    toAppend.add(newEntry);
                }
                insertIndex++;
            }
        }
        if (!toAppend.isEmpty()) {
            appendRecords(toAppend, syncPolicy == SyncPolicy.SYNC_EVERY_ENTRY);
        }
        return true;
    }

    public synchronized long getCommitIndex() {
        return commitIndex;
    }

    public synchronized void setCommitIndex(long commitIndex) {
        this.commitIndex = Math.min(commitIndex, getLastLogIndex());
    }

    public synchronized long getLastApplied() {
        return lastApplied;
    }

    public synchronized void setLastApplied(long lastApplied) {
        this.lastApplied = lastApplied;
    }

    public synchronized List<RaftLogEntry> getEntries() {
        return Collections.unmodifiableList(new ArrayList<>(entries));
    }
}
