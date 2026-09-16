package raft;

import java.io.Serializable;

public class RequestVoteReply implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long term;
    private final boolean voteGranted;

    public RequestVoteReply(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public long getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    @Override
    public String toString() {
        return "RequestVoteReply[term=" + term + ", granted=" + voteGranted + "]";
    }
}
