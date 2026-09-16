package raft;

import java.io.Serializable;

public class RaftLogEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long term;
    private final long index;
    private final String commandType; // "PUT", "DELETE", "NOOP"
    private final String key;
    private final Object value;

    public RaftLogEntry(long term, long index, String commandType, String key, Object value) {
        this.term = term;
        this.index = index;
        this.commandType = commandType;
        this.key = key;
        this.value = value;
    }

    public long getTerm() {
        return term;
    }

    public long getIndex() {
        return index;
    }

    public String getCommandType() {
        return commandType;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "LogEntry[index=" + index + ", term=" + term + ", cmd=" + commandType + 
               (key != null ? ", key=" + key + ", val=" + value : "") + "]";
    }
}
