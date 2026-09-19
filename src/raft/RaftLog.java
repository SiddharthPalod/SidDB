package raft;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RaftLog {
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

        if (this.logFile != null && this.logFile.exists()) {
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
        if (logFile == null || !logFile.exists()) return;
        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(logFile)))) {
            int count = in.readInt();
            entries.clear();
            entries.add(new RaftLogEntry(0, 0, "INIT", null, null));
            for (int i = 0; i < count; i++) {
                RaftLogEntry entry = (RaftLogEntry) in.readObject();
                entries.add(entry);
            }
        } catch (Exception ignored) {}
    }

    public synchronized void flush(boolean force) {
        if (logFile == null) return;
        try {
            File parent = logFile.getParentFile();
            if (parent != null && !parent.exists()) parent.mkdirs();

            try (FileOutputStream fos = new FileOutputStream(logFile);
                 ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(fos))) {
                out.writeInt(entries.size() - 1);
                for (int i = 1; i < entries.size(); i++) {
                    out.writeObject(entries.get(i));
                }
                out.flush();
                if (force || syncPolicy == SyncPolicy.SYNC_EVERY_ENTRY) {
                    fos.getFD().sync();
                }
            }
        } catch (IOException ignored) {}
    }

    private synchronized void persistToDisk() {
        flush(syncPolicy == SyncPolicy.SYNC_EVERY_ENTRY);
    }

    public synchronized RaftLogEntry append(long term, String commandType, String key, Object value) {
        long nextIndex = entries.size();
        RaftLogEntry entry = new RaftLogEntry(term, nextIndex, commandType, key, value);
        entries.add(entry);
        persistToDisk();
        return entry;
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
        persistToDisk();
    }

    /**
     * Appends new entries from Leader starting after prevLogIndex.
     * Overwrites any conflicting entries at subsequent indices.
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
        if (newEntries != null) {
            for (RaftLogEntry newEntry : newEntries) {
                if (insertIndex < entries.size()) {
                    // Check if conflicting
                    if (entries.get((int) insertIndex).getTerm() != newEntry.getTerm()) {
                        truncateFrom(insertIndex);
                        entries.add(newEntry);
                    }
                } else {
                    entries.add(newEntry);
                }
                insertIndex++;
            }
        }
        persistToDisk();
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
