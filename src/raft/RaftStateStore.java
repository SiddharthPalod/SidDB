package raft;

import java.io.*;

/**
 * Data Access Object (DAO) for persisting and recovering Raft metadata (currentTerm, votedFor).
 */
public class RaftStateStore {

    private final File metaFile;

    public static class PersistedState {
        public final long term;
        public final String votedFor;

        public PersistedState(long term, String votedFor) {
            this.term = term;
            this.votedFor = votedFor;
        }
    }

    public RaftStateStore(String stateDir) {
        if (stateDir != null) {
            File dir = new File(stateDir);
            if (!dir.exists()) dir.mkdirs();
            this.metaFile = new File(dir, "raft.meta");
        } else {
            this.metaFile = null;
        }
    }

    public synchronized PersistedState load() {
        if (metaFile == null || !metaFile.exists()) {
            return new PersistedState(0, null);
        }
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(metaFile)))) {
            long term = in.readLong();
            String vf = in.readUTF();
            return new PersistedState(term, vf.isEmpty() ? null : vf);
        } catch (IOException e) {
            return new PersistedState(0, null);
        }
    }

    public synchronized void save(long currentTerm, String votedFor) {
        if (metaFile == null) return;
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(metaFile)))) {
            out.writeLong(currentTerm);
            out.writeUTF(votedFor != null ? votedFor : "");
            out.flush();
        } catch (IOException ignored) {}
    }
}
