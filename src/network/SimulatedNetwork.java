package network;

import raft.AppendEntriesArgs;
import raft.AppendEntriesReply;
import raft.RaftNode;
import raft.RequestVoteArgs;
import raft.RequestVoteReply;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SimulatedNetwork implements Transport {

    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
    private final Set<String> isolatedNodes = ConcurrentHashMap.newKeySet();
    private final List<Set<String>> partitions = new ArrayList<>();
    private final AtomicLong messagesSent = new AtomicLong(0);
    private final AtomicLong messagesDropped = new AtomicLong(0);
    private volatile long simulatedDelayMs = 0;

    @Override
    public void registerNode(String nodeId, RaftNode node) {
        nodes.put(nodeId, node);
    }

    @Override
    public void unregisterNode(String nodeId) {
        nodes.remove(nodeId);
        isolatedNodes.remove(nodeId);
    }

    public void setSimulatedDelayMs(long delayMs) {
        this.simulatedDelayMs = delayMs;
    }

    public synchronized void isolateNode(String nodeId) {
        isolatedNodes.add(nodeId);
    }

    public synchronized void healNode(String nodeId) {
        isolatedNodes.remove(nodeId);
    }

    public synchronized boolean isIsolated(String nodeId) {
        return isolatedNodes.contains(nodeId);
    }

    public synchronized void createPartition(List<Set<String>> partitionGroups) {
        partitions.clear();
        for (Set<String> group : partitionGroups) {
            partitions.add(new HashSet<>(group));
        }
    }

    public synchronized void healAll() {
        isolatedNodes.clear();
        partitions.clear();
    }

    public synchronized boolean canCommunicate(String from, String to) {
        if (from == null || to == null) return false;
        if (isolatedNodes.contains(from) || isolatedNodes.contains(to)) {
            return false;
        }
        if (!partitions.isEmpty()) {
            for (Set<String> group : partitions) {
                if (group.contains(from)) {
                    return group.contains(to);
                }
            }
        }
        return true;
    }

    @Override
    public RequestVoteReply sendRequestVote(String fromNodeId, String targetNodeId, RequestVoteArgs args) {
        messagesSent.incrementAndGet();
        if (!canCommunicate(fromNodeId, targetNodeId)) {
            messagesDropped.incrementAndGet();
            return null;
        }

        RaftNode target = nodes.get(targetNodeId);
        if (target == null) {
            messagesDropped.incrementAndGet();
            return null;
        }

        if (simulatedDelayMs > 0) {
            try {
                Thread.sleep(simulatedDelayMs);
            } catch (InterruptedException ignored) {}
        }

        return target.handleRequestVote(args);
    }

    @Override
    public AppendEntriesReply sendAppendEntries(String fromNodeId, String targetNodeId, AppendEntriesArgs args) {
        messagesSent.incrementAndGet();
        if (!canCommunicate(fromNodeId, targetNodeId)) {
            messagesDropped.incrementAndGet();
            return null;
        }

        RaftNode target = nodes.get(targetNodeId);
        if (target == null) {
            messagesDropped.incrementAndGet();
            return null;
        }

        if (simulatedDelayMs > 0) {
            try {
                Thread.sleep(simulatedDelayMs);
            } catch (InterruptedException ignored) {}
        }

        return target.handleAppendEntries(args);
    }

    public long getMessagesSent() {
        return messagesSent.get();
    }

    public long getMessagesDropped() {
        return messagesDropped.get();
    }

    public Set<String> getIsolatedNodes() {
        return Collections.unmodifiableSet(isolatedNodes);
    }

    public List<Set<String>> getPartitions() {
        return Collections.unmodifiableList(partitions);
    }
}
