package raft;

public class RequestVoteReply extends RaftRpcMessage {
    private static final long serialVersionUID = 1L;

    private final boolean voteGranted;

    public RequestVoteReply(long term, boolean voteGranted) {
        super(term);
        this.voteGranted = voteGranted;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    @Override
    public String toString() {
        return "RequestVoteReply[term=" + term + ", granted=" + voteGranted + "]";
    }
}
