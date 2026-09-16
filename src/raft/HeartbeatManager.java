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

        Map<String, Long> nextIndex = node.getNextIndex();
        Map<String, Long> matchIndex = node.getMatchIndex();

        for (String peer : node.getPeers()) {
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

    @Override
    public synchronized void close() {
        stopHeartbeats();
        scheduler.shutdownNow();
    }
}
