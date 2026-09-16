package network;

import raft.AppendEntriesArgs;
import raft.AppendEntriesReply;
import raft.RaftNode;
import raft.RequestVoteArgs;
import raft.RequestVoteReply;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Decorator Pattern: Wraps any Transport instance and applies configurable chaos policies
 * (packet loss, latency jitter, slow follower delays, directional packet blocking, reordering).
 * Leaves the underlying Transport unpolluted.
 */
public class ChaoticTransport implements Transport {

    private final Transport delegate;

    // Chaos configuration
    private volatile double packetLossRate = 0.0;
    private volatile long minDelayMs = 0;
    private volatile long maxDelayMs = 0;
    private volatile boolean reorderMessages = false;
    private final Map<String, Long> perNodeLatencyMs = new ConcurrentHashMap<>();
    private final Set<String> directionalBlocks = ConcurrentHashMap.newKeySet();

    // Metrics
    private final AtomicLong messagesSent = new AtomicLong(0);
    private final AtomicLong messagesDropped = new AtomicLong(0);
    private final AtomicLong messagesDelayed = new AtomicLong(0);

    public ChaoticTransport(Transport delegate) {
        this.delegate = delegate;
    }

    public Transport getDelegate() {
        return delegate;
    }

    // --- Configuration Setters (Fluent API) ---

    public ChaoticTransport setPacketLossRate(double rate) {
        this.packetLossRate = Math.max(0.0, Math.min(1.0, rate));
        return this;
    }

    public ChaoticTransport setLatencyJitter(long minMs, long maxMs) {
        this.minDelayMs = minMs;
        this.maxDelayMs = Math.max(minMs, maxMs);
        return this;
    }

    public ChaoticTransport setNodeLatency(String nodeId, long delayMs) {
        if (delayMs <= 0) {
            perNodeLatencyMs.remove(nodeId);
        } else {
            perNodeLatencyMs.put(nodeId, delayMs);
        }
        return this;
    }

    public ChaoticTransport blockTraffic(String fromNodeId, String toNodeId) {
        directionalBlocks.add(fromNodeId + "->" + toNodeId);
        return this;
    }

    public ChaoticTransport unblockTraffic(String fromNodeId, String toNodeId) {
        directionalBlocks.remove(fromNodeId + "->" + toNodeId);
        return this;
    }

    public ChaoticTransport setReorderMessages(boolean reorder) {
        this.reorderMessages = reorder;
        return this;
    }

    public void resetChaos() {
        this.packetLossRate = 0.0;
        this.minDelayMs = 0;
        this.maxDelayMs = 0;
        this.reorderMessages = false;
        this.perNodeLatencyMs.clear();
        this.directionalBlocks.clear();
    }

    public void resetMetrics() {
        this.messagesSent.set(0);
        this.messagesDropped.set(0);
        this.messagesDelayed.set(0);
    }

    // --- Interception Logic ---

    private boolean shouldDrop(String from, String to) {
        if (directionalBlocks.contains(from + "->" + to)) {
            return true;
        }
        if (packetLossRate > 0.0) {
            return ThreadLocalRandom.current().nextDouble() < packetLossRate;
        }
        return false;
    }

    private void applyDelays(String from, String to) {
        long delay = 0;

        if (maxDelayMs > 0) {
            if (maxDelayMs == minDelayMs) {
                delay += minDelayMs;
            } else {
                delay += ThreadLocalRandom.current().nextLong(minDelayMs, maxDelayMs + 1);
            }
        }

        Long nodeDelay = perNodeLatencyMs.get(from);
        if (nodeDelay != null) {
            delay += nodeDelay;
        }
        Long targetDelay = perNodeLatencyMs.get(to);
        if (targetDelay != null) {
            delay += targetDelay;
        }

        if (reorderMessages) {
            delay += ThreadLocalRandom.current().nextLong(5, 50);
        }

        if (delay > 0) {
            messagesDelayed.incrementAndGet();
            try {
                Thread.sleep(delay);
            } catch (InterruptedException ignored) {}
        }
    }

    // --- Transport Delegate Methods ---

    @Override
    public void registerNode(String nodeId, RaftNode node) {
        delegate.registerNode(nodeId, node);
    }

    @Override
    public void unregisterNode(String nodeId) {
        delegate.unregisterNode(nodeId);
        perNodeLatencyMs.remove(nodeId);
    }

    @Override
    public RequestVoteReply sendRequestVote(String fromNodeId, String targetNodeId, RequestVoteArgs args) {
        messagesSent.incrementAndGet();

        if (shouldDrop(fromNodeId, targetNodeId)) {
            messagesDropped.incrementAndGet();
            return null;
        }

        applyDelays(fromNodeId, targetNodeId);
        return delegate.sendRequestVote(fromNodeId, targetNodeId, args);
    }

    @Override
    public AppendEntriesReply sendAppendEntries(String fromNodeId, String targetNodeId, AppendEntriesArgs args) {
        messagesSent.incrementAndGet();

        if (shouldDrop(fromNodeId, targetNodeId)) {
            messagesDropped.incrementAndGet();
            return null;
        }

        applyDelays(fromNodeId, targetNodeId);
        return delegate.sendAppendEntries(fromNodeId, targetNodeId, args);
    }

    // --- Metrics Getters ---

    public long getMessagesSent() { return messagesSent.get(); }
    public long getMessagesDropped() { return messagesDropped.get(); }
    public long getMessagesDelayed() { return messagesDelayed.get(); }
    public double getPacketLossRate() { return packetLossRate; }
    public long getMinDelayMs() { return minDelayMs; }
    public long getMaxDelayMs() { return maxDelayMs; }
}
