package network;

import raft.AppendEntriesArgs;
import raft.AppendEntriesReply;
import raft.RaftNode;
import raft.RequestVoteArgs;
import raft.RequestVoteReply;

public interface Transport {
    void registerNode(String nodeId, RaftNode node);
    void unregisterNode(String nodeId);
    
    RequestVoteReply sendRequestVote(String fromNodeId, String targetNodeId, RequestVoteArgs args);
    AppendEntriesReply sendAppendEntries(String fromNodeId, String targetNodeId, AppendEntriesArgs args);
}
