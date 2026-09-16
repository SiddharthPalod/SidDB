package raft;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Manages randomized election timeouts to prevent split votes,
 * triggers transitions to CANDIDATE state, and collects votes from peers.
 */
public class ElectionManager implements AutoCloseable {

    private final RaftNode node;
    private final int minElectionTimeoutMs;
    private final int maxElectionTimeoutMs;
    private final Random random = new Random();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> electionTimeoutTask;

    public ElectionManager(RaftNode node, int minElectionTimeoutMs, int maxElectionTimeoutMs) {
        this.node = node;
        this.minElectionTimeoutMs = minElectionTimeoutMs;
        this.maxElectionTimeoutMs = maxElectionTimeoutMs;
    }

    public synchronized void resetElectionTimeout() {
        if (!node.isRunning() || node.getRole() == RaftRole.OFFLINE || node.getRole() == RaftRole.LEADER) {
            return;
        }

        if (electionTimeoutTask != null) {
            electionTimeoutTask.cancel(true);
        }

        int timeout = minElectionTimeoutMs + random.nextInt(maxElectionTimeoutMs - minElectionTimeoutMs + 1);
        electionTimeoutTask = scheduler.schedule(this::triggerElection, timeout, TimeUnit.MILLISECONDS);
    }

    public synchronized void cancelTimeout() {
        if (electionTimeoutTask != null) {
            electionTimeoutTask.cancel(true);
        }
    }

    /**
     * Converts node to CANDIDATE, increments term, votes for self, and requests votes from peers.
     */
    public void triggerElection() {
        if (!node.isRunning() || node.getRole() == RaftRole.OFFLINE || node.getRole() == RaftRole.LEADER) {
            return;
        }

        long term;
        long lastLogIndex;
        long lastLogTerm;
        String nodeId = node.getNodeId();

        synchronized (node) {
            node.setRole(RaftRole.CANDIDATE);
            node.setCurrentTerm(node.getCurrentTerm() + 1);
            node.setVotedFor(nodeId);
            node.setLeaderId(null);
            term = node.getCurrentTerm();
            lastLogIndex = node.getLog().getLastLogIndex();
            lastLogTerm = node.getLog().getLastLogTerm();
        }

        resetElectionTimeout();

        // 1 vote from self
        Set<String> votes = ConcurrentHashMap.newKeySet();
        votes.add(nodeId);

        int majority = node.getQuorumSize();

        if (votes.size() >= majority) {
            node.becomeLeader();
            return;
        }

        for (String peer : node.getPeers()) {
            CompletableFuture.runAsync(() -> {
                if (node.getRole() != RaftRole.CANDIDATE || node.getCurrentTerm() != term) return;

                RequestVoteArgs args = new RequestVoteArgs(term, nodeId, lastLogIndex, lastLogTerm);
                RequestVoteReply reply = node.getTransport().sendRequestVote(nodeId, peer, args);

                if (reply == null) return;

                synchronized (node) {
                    if (node.observeTerm(reply.getTerm(), null)) {
                        return;
                    }

                    if (node.getRole() == RaftRole.CANDIDATE && node.getCurrentTerm() == term && reply.isVoteGranted()) {
                        votes.add(peer);
                        if (votes.size() >= majority) {
                            node.becomeLeader();
                        }
                    }
                }
            });
        }
    }

    @Override
    public synchronized void close() {
        cancelTimeout();
        scheduler.shutdownNow();
    }
}
