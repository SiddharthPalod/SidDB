package raft;

import engine.SidDBEngine;
import network.Transport;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RaftNode implements AutoCloseable {

    private final String nodeId;
    private final List<String> peers;
    private final Transport transport;
    private final SidDBEngine stateMachine;
    private final RaftLog log;
    private final RaftStateStore stateStore;

    private long currentTerm = 0;
    private String votedFor = null;
    private volatile RaftRole role = RaftRole.FOLLOWER;
    private String leaderId = null;

    // Leader state
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    // Sub-Managers
    private final ElectionManager electionManager;
    private final HeartbeatManager heartbeatManager;
    private final ScheduledExecutorService clientProposalScheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentHashMap<Long, List<CompletableFuture<Boolean>>> pendingProposals = new ConcurrentHashMap<>();

    // Leader Lease & Linearizable Read State (Phase 8.1)
    private final int minElectionTimeoutMs;
    private final long leaseDurationNs;
    private volatile long leaseExpiryNs = 0L;
    private volatile ReadMode readMode = ReadMode.LEADER_LEASE;
    private volatile SyncPolicy syncPolicy = SyncPolicy.SYNC_EVERY_ENTRY;

    public RaftNode(String nodeId, List<String> peers, Transport transport, SidDBEngine stateMachine) {
        this(nodeId, peers, transport, stateMachine, null, 150, 300, 50, SyncPolicy.SYNC_EVERY_ENTRY);
    }

    public RaftNode(String nodeId, List<String> peers, Transport transport, SidDBEngine stateMachine, String stateDir) {
        this(nodeId, peers, transport, stateMachine, stateDir, 150, 300, 50, SyncPolicy.SYNC_EVERY_ENTRY);
    }

    public RaftNode(String nodeId, List<String> peers, Transport transport, SidDBEngine stateMachine, String stateDir,
                    int minElectionTimeoutMs, int maxElectionTimeoutMs, int heartbeatIntervalMs) {
        this(nodeId, peers, transport, stateMachine, stateDir, minElectionTimeoutMs, maxElectionTimeoutMs, heartbeatIntervalMs, SyncPolicy.SYNC_EVERY_ENTRY);
    }

    public RaftNode(String nodeId, List<String> peers, Transport transport, SidDBEngine stateMachine, String stateDir,
                    int minElectionTimeoutMs, int maxElectionTimeoutMs, int heartbeatIntervalMs, SyncPolicy syncPolicy) {
        this.nodeId = nodeId;
        this.peers = new ArrayList<>(peers);
        this.peers.remove(nodeId); // Exclude self
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.stateStore = new RaftStateStore(stateDir);
        this.minElectionTimeoutMs = minElectionTimeoutMs;
        this.syncPolicy = (syncPolicy != null) ? syncPolicy : SyncPolicy.SYNC_EVERY_ENTRY;
        // Conservative lease duration: 80% of min election timeout in nanoseconds
        this.leaseDurationNs = (long) (minElectionTimeoutMs * 0.80 * 1_000_000L);

        if (stateDir != null) {
            File dir = new File(stateDir);
            this.log = new RaftLog(new File(dir, "raft.log").getAbsolutePath(), this.syncPolicy);
            RaftStateStore.PersistedState state = stateStore.load();
            this.currentTerm = state.term;
            this.votedFor = state.votedFor;
        } else {
            this.log = new RaftLog(null, this.syncPolicy);
        }

        this.electionManager = new ElectionManager(this, minElectionTimeoutMs, maxElectionTimeoutMs);
        this.heartbeatManager = new HeartbeatManager(this, heartbeatIntervalMs);

        if (transport != null) {
            transport.registerNode(nodeId, this);
        }
    }

    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            role = RaftRole.FOLLOWER;
            electionManager.resetElectionTimeout();
        }
    }

    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            role = RaftRole.OFFLINE;
            electionManager.cancelTimeout();
            heartbeatManager.stopHeartbeats();
            if (transport != null) {
                transport.unregisterNode(nodeId);
            }
        }
    }

    // --- Role Transitions & Protocol Invariants ---

    public int getQuorumSize() {
        return ((peers.size() + 1) / 2) + 1;
    }

    /**
     * Centralized Raft invariant: If remoteTerm > currentTerm, update term and convert to FOLLOWER.
     */
    public synchronized boolean observeTerm(long remoteTerm, String leaderHint) {
        if (remoteTerm > currentTerm) {
            becomeFollower(remoteTerm, leaderHint);
            return true;
        }
        return false;
    }

    public synchronized void becomeFollower(long term, String leader) {
        this.role = RaftRole.FOLLOWER;
        this.currentTerm = term;
        this.votedFor = null;
        this.leaderId = leader;
        this.leaseExpiryNs = 0L; // Invalidate leader lease immediately
        stateStore.save(currentTerm, votedFor);
        heartbeatManager.stopHeartbeats();
        electionManager.resetElectionTimeout();
        for (List<CompletableFuture<Boolean>> list : pendingProposals.values()) {
            for (CompletableFuture<Boolean> f : list) {
                f.complete(false);
            }
        }
        pendingProposals.clear();
    }

    public synchronized void becomeLeader() {
        if (role != RaftRole.CANDIDATE) return;

        this.role = RaftRole.LEADER;
        this.leaderId = nodeId;
        this.leaseExpiryNs = 0L; // Will be granted upon first successful quorum heartbeat

        electionManager.cancelTimeout();

        for (String peer : peers) {
            nextIndex.put(peer, log.getLastLogIndex() + 1);
            matchIndex.put(peer, 0L);
        }

        // Start periodic heartbeats and initial broadcast via HeartbeatManager
        heartbeatManager.startHeartbeats();
    }

    public synchronized void checkAndUpdateCommitIndex() {
        if (role != RaftRole.LEADER) return;

        long currentCommit = log.getCommitIndex();
        long lastIndex = log.getLastLogIndex();
        int majority = getQuorumSize();

        for (long N = currentCommit + 1; N <= lastIndex; N++) {
            if (log.getTermAt(N) == currentTerm) {
                int replicatedCount = 1; // Leader itself
                for (String peer : peers) {
                    if (matchIndex.getOrDefault(peer, 0L) >= N) {
                        replicatedCount++;
                    }
                }
                if (replicatedCount >= majority) {
                    log.setCommitIndex(N);
                }
            }
        }

        applyCommittedEntries();
    }

    public synchronized void applyCommittedEntries() {
        while (log.getCommitIndex() > log.getLastApplied()) {
            long nextApply = log.getLastApplied() + 1;
            RaftLogEntry entry = log.getEntry(nextApply);
            if (entry != null && stateMachine != null) {
                try {
                    if ("PUT".equalsIgnoreCase(entry.getCommandType())) {
                        stateMachine.put(entry.getKey(), entry.getValue());
                    } else if ("DELETE".equalsIgnoreCase(entry.getCommandType())) {
                        stateMachine.delete(entry.getKey());
                    }
                } catch (IOException e) {
                    System.err.println("[" + nodeId + "] Error applying log entry " + entry + ": " + e.getMessage());
                }
            }
            log.setLastApplied(nextApply);
        }
        notifyCommittedProposals(log.getCommitIndex());
    }

    private void notifyCommittedProposals(long commitIndex) {
        Iterator<Map.Entry<Long, List<CompletableFuture<Boolean>>>> it = pendingProposals.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, List<CompletableFuture<Boolean>>> entry = it.next();
            if (entry.getKey() <= commitIndex) {
                for (CompletableFuture<Boolean> f : entry.getValue()) {
                    f.complete(true);
                }
                it.remove();
            }
        }
    }

    // --- RPC Handlers ---

    public synchronized RequestVoteReply handleRequestVote(RequestVoteArgs args) {
        if (!running.get() || role == RaftRole.OFFLINE) {
            return new RequestVoteReply(currentTerm, false);
        }

        if (args.getTerm() < currentTerm) {
            return new RequestVoteReply(currentTerm, false);
        }

        observeTerm(args.getTerm(), null);

        boolean canVote = (votedFor == null || votedFor.equals(args.getCandidateId()));
        boolean isUpToDate = isLogUpToDate(args.getLastLogTerm(), args.getLastLogIndex());

        if (canVote && isUpToDate) {
            votedFor = args.getCandidateId();
            stateStore.save(currentTerm, votedFor);
            electionManager.resetElectionTimeout();
            return new RequestVoteReply(currentTerm, true);
        }

        return new RequestVoteReply(currentTerm, false);
    }

    private boolean isLogUpToDate(long candidateLastTerm, long candidateLastIndex) {
        long myLastTerm = log.getLastLogTerm();
        long myLastIndex = log.getLastLogIndex();

        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= myLastIndex;
    }

    public synchronized AppendEntriesReply handleAppendEntries(AppendEntriesArgs args) {
        if (!running.get() || role == RaftRole.OFFLINE) {
            return new AppendEntriesReply(currentTerm, false, log.getLastLogIndex());
        }

        if (args.getTerm() < currentTerm) {
            return new AppendEntriesReply(currentTerm, false, log.getLastLogIndex());
        }

        if (args.getTerm() > currentTerm || role == RaftRole.CANDIDATE) {
            becomeFollower(args.getTerm(), args.getLeaderId());
        } else {
            this.leaderId = args.getLeaderId();
            electionManager.resetElectionTimeout();
        }

        boolean success = log.appendEntries(args.getPrevLogIndex(), args.getPrevLogTerm(), args.getEntries());
        if (!success) {
            return new AppendEntriesReply(currentTerm, false, log.getLastLogIndex());
        }

        if (args.getLeaderCommit() > log.getCommitIndex()) {
            log.setCommitIndex(Math.min(args.getLeaderCommit(), log.getLastLogIndex()));
            applyCommittedEntries();
        }

        return new AppendEntriesReply(currentTerm, true, log.getLastLogIndex());
    }

    // --- Client Proposal Interface ---

    public synchronized CompletableFuture<Boolean> propose(String commandType, String key, Object value) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();

        if (role != RaftRole.LEADER) {
            future.complete(false);
            return future;
        }

        RaftLogEntry entry = log.append(currentTerm, commandType, key, value);
        long targetIndex = entry.getIndex();

        if (peers.isEmpty()) {
            log.setCommitIndex(targetIndex);
            applyCommittedEntries();
            future.complete(true);
            return future;
        }

        pendingProposals.computeIfAbsent(targetIndex, k -> new CopyOnWriteArrayList<>()).add(future);

        // Broadcast replication immediately via heartbeat manager
        heartbeatManager.broadcastAppendEntries();

        // Safety fallback timeout check
        clientProposalScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                synchronized (RaftNode.this) {
                    if (log.getCommitIndex() >= targetIndex) {
                        future.complete(true);
                    } else if (role != RaftRole.LEADER || !running.get()) {
                        future.complete(false);
                    } else if (!future.isDone()) {
                        clientProposalScheduler.schedule(this, 10, TimeUnit.MILLISECONDS);
                    }
                }
            }
        }, 10, TimeUnit.MILLISECONDS);

        return future;
    }

    // --- Linearizable Read Interface (Phase 8.1) ---

    public void renewLeaderLease() {
        this.leaseExpiryNs = System.nanoTime() + leaseDurationNs;
    }

    public boolean hasValidLeaderLease() {
        return role == RaftRole.LEADER && System.nanoTime() < leaseExpiryNs;
    }

    public long getLeaseExpiryNs() {
        return leaseExpiryNs;
    }

    public ReadMode getReadMode() {
        return readMode;
    }

    public void setReadMode(ReadMode readMode) {
        this.readMode = readMode;
    }

    /**
     * Executes a linearizable read using the configured ReadMode (LEADER_LEASE, READ_INDEX, or LOG_BARRIER).
     */
    public CompletableFuture<Object> readLinearizable(String key) {
        if (role != RaftRole.LEADER) {
            CompletableFuture<Object> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("Cannot perform linearizable read on non-leader node " + nodeId));
            return f;
        }

        switch (readMode) {
            case LEADER_LEASE:
                return executeLeaseRead(key);
            case READ_INDEX:
                return executeReadIndex(key);
            case LOG_BARRIER:
            default:
                return executeLogBarrierRead(key);
        }
    }

    private CompletableFuture<Object> executeLeaseRead(String key) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        if (hasValidLeaderLease()) {
            try {
                Object val = stateMachine != null ? stateMachine.get(key) : null;
                future.complete(val);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        } else {
            // Lease expired or not yet confirmed by quorum: Fallback smoothly to ReadIndex protocol
            return executeReadIndex(key);
        }
        return future;
    }

    private CompletableFuture<Object> executeReadIndex(String key) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        final long targetReadIndex;
        synchronized (this) {
            if (role != RaftRole.LEADER) {
                future.completeExceptionally(new IllegalStateException("Not leader"));
                return future;
            }
            targetReadIndex = log.getCommitIndex();
        }

        // Lightweight quorum confirmation (no log replication or disk writes)
        heartbeatManager.confirmQuorum().thenAccept(quorumConfirmed -> {
            if (!quorumConfirmed || role != RaftRole.LEADER) {
                future.completeExceptionally(new IllegalStateException("Quorum confirmation failed during ReadIndex"));
                return;
            }

            // Wait until state machine has applied up to targetReadIndex
            Runnable checkApplied = new Runnable() {
                @Override
                public void run() {
                    if (log.getLastApplied() >= targetReadIndex) {
                        try {
                            Object val = stateMachine != null ? stateMachine.get(key) : null;
                            future.complete(val);
                        } catch (Exception e) {
                            future.completeExceptionally(e);
                        }
                    } else if (role != RaftRole.LEADER || !running.get()) {
                        future.completeExceptionally(new IllegalStateException("Node stepped down while awaiting lastApplied"));
                    } else {
                        clientProposalScheduler.schedule(this, 1, TimeUnit.MILLISECONDS);
                    }
                }
            };
            checkApplied.run();
        }).exceptionally(ex -> {
            future.completeExceptionally(ex);
            return null;
        });

        return future;
    }

    private CompletableFuture<Object> executeLogBarrierRead(String key) {
        return propose("READ_BARRIER", key, null).thenApply(ok -> {
            if (Boolean.TRUE.equals(ok) && stateMachine != null) {
                try {
                    return stateMachine.get(key);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            }
            throw new CompletionException(new IllegalStateException("Log barrier failed"));
        });
    }

    // --- Getters & Setters ---

    public String getNodeId() { return nodeId; }
    public RaftRole getRole() { return role; }
    public synchronized void setRole(RaftRole role) { this.role = role; }
    public synchronized long getCurrentTerm() { return currentTerm; }
    public synchronized void setCurrentTerm(long currentTerm) { 
        this.currentTerm = currentTerm; 
        stateStore.save(currentTerm, votedFor);
    }
    public synchronized String getVotedFor() { return votedFor; }
    public synchronized void setVotedFor(String votedFor) { 
        this.votedFor = votedFor; 
        stateStore.save(currentTerm, votedFor);
    }
    public synchronized String getLeaderId() { return leaderId; }
    public synchronized void setLeaderId(String leaderId) { this.leaderId = leaderId; }

    public RaftLog getLog() { return log; }
    public SidDBEngine getStateMachine() { return stateMachine; }
    public Transport getTransport() { return transport; }
    public List<String> getPeers() { return Collections.unmodifiableList(peers); }
    public Map<String, Long> getNextIndex() { return nextIndex; }
    public Map<String, Long> getMatchIndex() { return matchIndex; }
    public boolean isRunning() { return running.get(); }

    public SyncPolicy getSyncPolicy() { return syncPolicy; }
    public void setSyncPolicy(SyncPolicy syncPolicy) {
        this.syncPolicy = syncPolicy;
        if (this.log != null) {
            this.log.setSyncPolicy(syncPolicy);
        }
    }

    public ElectionManager getElectionManager() { return electionManager; }
    public HeartbeatManager getHeartbeatManager() { return heartbeatManager; }

    public synchronized Map<String, Object> getStatusMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("nodeId", nodeId);
        map.put("role", role.toString());
        map.put("term", currentTerm);
        map.put("votedFor", votedFor);
        map.put("leaderId", leaderId);
        map.put("commitIndex", log.getCommitIndex());
        map.put("lastApplied", log.getLastApplied());
        map.put("logSize", log.getLastLogIndex());
        map.put("activeMemTableSize", stateMachine != null ? stateMachine.getActiveMemTable().size() : 0);
        return map;
    }

    @Override
    public void close() {
        stop();
        electionManager.close();
        heartbeatManager.close();
        clientProposalScheduler.shutdownNow();
        if (stateMachine != null) {
            try {
                stateMachine.close();
            } catch (IOException ignored) {}
        }
    }
}
