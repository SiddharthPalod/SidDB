package raft;

import java.io.Serializable;

public class AppendEntriesReply implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long term;
    private final boolean success;
    private final long matchIndex;

    public AppendEntriesReply(long term, boolean success, long matchIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }

    public long getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public long getMatchIndex() {
        return matchIndex;
    }

    @Override
    public String toString() {
        return "AppendEntriesReply[term=" + term + ", success=" + success + ", matchIndex=" + matchIndex + "]";
    }
}
