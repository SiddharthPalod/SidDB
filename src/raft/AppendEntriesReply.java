package raft;

public class AppendEntriesReply extends RaftRpcMessage {
    private static final long serialVersionUID = 1L;

    private final boolean success;
    private final long matchIndex;

    public AppendEntriesReply(long term, boolean success, long matchIndex) {
        super(term);
        this.success = success;
        this.matchIndex = matchIndex;
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
