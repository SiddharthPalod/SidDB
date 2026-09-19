package raft;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Runs on the LEADER to broadcast AppendEntries periodically to maintain
 * authority and replicate logs to followers.
 */
public class HeartbeatManager implements AutoCloseable {

    private final RaftNode node;
    private final int heartbeatIntervalMs;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> heartbeatTask;

    public HeartbeatManager(RaftNode node, int heartbeatIntervalMs) {
        this.node = node;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public synchronized void startHeartbeats() {
        stopHeartbeats();
        // Broadcast initial heartbeats immediately
        broadcastAppendEntries();
        // Schedule periodic heartbeats
        heartbeatTask = scheduler.scheduleAtFixedRate(this::broadcastAppendEntries,
                heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    public synchronized void stopHeartbeats() {
        if (heartbeatTask != null) {
            heartbeatTask.cancel(true);
            heartbeatTask = null;
        }
    }

    public void broadcastAppendEntries() {
        if (!node.isRunning() || node.getRole() != RaftRole.LEADER) {
            return;
        }

        long term;
        long leaderCommit;
        String nodeId = node.getNodeId();
        RaftLog log = node.getLog();

        synchronized (node) {
            term = node.getCurrentTerm();
            leaderCommit = log.getCommitIndex();
        }

        List<String> peers = node.getPeers();
        if (peers.isEmpty()) {
            // Single node cluster always holds a valid lease
            node.renewLeaderLease();
            return;
        }

        Map<String, Long> nextIndex = node.getNextIndex();
        Map<String, Long> matchIndex = node.getMatchIndex();

        java.util.concurrent.atomic.AtomicInteger ackCount = new java.util.concurrent.atomic.AtomicInteger(1); // Self counts as 1
        int majority = node.getQuorumSize();

        for (String peer : peers) {
            long prevIndex = nextIndex.getOrDefault(peer, log.getLastLogIndex() + 1) - 1;
            long prevTerm = log.getTermAt(prevIndex);
            List<RaftLogEntry> entriesToSend = log.getEntriesFrom(prevIndex + 1);

            CompletableFuture.runAsync(() -> {
                if (node.getRole() != RaftRole.LEADER || node.getCurrentTerm() != term) return;

                AppendEntriesArgs args = new AppendEntriesArgs(term, nodeId, prevIndex, prevTerm, entriesToSend, leaderCommit);
                AppendEntriesReply reply = node.getTransport().sendAppendEntries(nodeId, peer, args);

                if (reply == null) return;

                synchronized (node) {
                    if (node.observeTerm(reply.getTerm(), null)) {
                        return;
                    }

                    if (node.getRole() == RaftRole.LEADER && node.getCurrentTerm() == term) {
                        if (reply.isSuccess()) {
                            long match = reply.getMatchIndex();
                            matchIndex.put(peer, match);
                            nextIndex.put(peer, match + 1);
                            node.checkAndUpdateCommitIndex();

                            // Quorum lease renewal
                            if (ackCount.incrementAndGet() >= majority) {
                                node.renewLeaderLease();
                            }
                        } else {
                            // Step backward nextIndex for peer to find common log prefix
                            long currentNext = nextIndex.getOrDefault(peer, 1L);
                            if (currentNext > 1) {
                                nextIndex.put(peer, currentNext - 1);
                            }
                        }
                    }
                }
            });
        }
    }

    /**
     * Confirms current leadership with quorum via lightweight round-trip without appending log entries.
     * Used by the ReadIndex protocol.
     */
    public CompletableFuture<Boolean> confirmQuorum() {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        if (!node.isRunning() || node.getRole() != RaftRole.LEADER) {
            future.complete(false);
            return future;
        }

        List<String> peers = node.getPeers();
        if (peers.isEmpty()) {
            future.complete(true);
            return future;
        }

        long term;
        long leaderCommit;
        String nodeId = node.getNodeId();
        RaftLog log = node.getLog();

        synchronized (node) {
            term = node.getCurrentTerm();
            leaderCommit = log.getCommitIndex();
        }

        int majority = node.getQuorumSize();
        java.util.concurrent.atomic.AtomicInteger acks = new java.util.concurrent.atomic.AtomicInteger(1); // Self
        java.util.concurrent.atomic.AtomicInteger repliesReceived = new java.util.concurrent.atomic.AtomicInteger(1);

        for (String peer : peers) {
            CompletableFuture.runAsync(() -> {
                if (node.getRole() != RaftRole.LEADER || node.getCurrentTerm() != term) {
                    future.complete(false);
                    return;
                }

                long prevIndex = node.getNextIndex().getOrDefault(peer, log.getLastLogIndex() + 1) - 1;
                long prevTerm = log.getTermAt(prevIndex);
                // Send empty entries for heartbeat check
                AppendEntriesArgs args = new AppendEntriesArgs(term, nodeId, prevIndex, prevTerm, java.util.Collections.emptyList(), leaderCommit);
                AppendEntriesReply reply = node.getTransport().sendAppendEntries(nodeId, peer, args);

                synchronized (node) {
                    if (reply != null) {
                        if (node.observeTerm(reply.getTerm(), null)) {
                            future.complete(false);
                            return;
                        }
                        if (reply.isSuccess() && acks.incrementAndGet() >= majority) {
                            node.renewLeaderLease();
                            future.complete(true);
                        }
                    }

                    int total = repliesReceived.incrementAndGet();
                    if (total >= (peers.size() + 1) && !future.isDone()) {
                        future.complete(acks.get() >= majority);
                    }
                }
            });
        }

        return future;
    }

    @Override
    public synchronized void close() {
        stopHeartbeats();
        scheduler.shutdownNow();
    }
}
