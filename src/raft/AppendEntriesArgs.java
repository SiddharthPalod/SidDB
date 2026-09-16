package raft;

import java.io.Serializable;
import java.util.List;

public class AppendEntriesArgs implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long term;
    private final String leaderId;
    private final long prevLogIndex;
    private final long prevLogTerm;
    private final List<RaftLogEntry> entries;
    private final long leaderCommit;

    public AppendEntriesArgs(long term, String leaderId, long prevLogIndex, long prevLogTerm, 
                             List<RaftLogEntry> entries, long leaderCommit) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public long getTerm() {
        return term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<RaftLogEntry> getEntries() {
        return entries;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    @Override
    public String toString() {
        return "AppendEntriesArgs[term=" + term + ", leader=" + leaderId + 
               ", prevIndex=" + prevLogIndex + ", prevTerm=" + prevLogTerm + 
               ", numEntries=" + (entries != null ? entries.size() : 0) + 
               ", leaderCommit=" + leaderCommit + "]";
    }
}
