package raft;

import java.io.Serializable;

public class RequestVoteArgs implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long term;
    private final String candidateId;
    private final long lastLogIndex;
    private final long lastLogTerm;

    public RequestVoteArgs(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public long getTerm() {
        return term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public String toString() {
        return "RequestVoteArgs[term=" + term + ", candidate=" + candidateId + 
               ", lastIndex=" + lastLogIndex + ", lastTerm=" + lastLogTerm + "]";
    }
}
