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
    private RaftRole role = RaftRole.FOLLOWER;
    private String leaderId = null;

    // Leader state
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    // Sub-Managers
    private final ElectionManager electionManager;
    private final HeartbeatManager heartbeatManager;
    private final ScheduledExecutorService clientProposalScheduler = Executors.newSingleThreadScheduledExecutor();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RaftNode(String nodeId, List<String> peers, Transport transport, SidDBEngine stateMachine) {
        this(nodeId, peers, transport, stateMachine, null, 150, 300, 50);
    }

    public RaftNode(String nodeId, List<String> peers, Transport transport, SidDBEngine stateMachine, String stateDir) {
        this(nodeId, peers, transport, stateMachine, stateDir, 150, 300, 50);
    }

    public RaftNode(String nodeId, List<String> peers, Transport transport, SidDBEngine stateMachine, String stateDir,
                    int minElectionTimeoutMs, int maxElectionTimeoutMs, int heartbeatIntervalMs) {
        this.nodeId = nodeId;
        this.peers = new ArrayList<>(peers);
        this.peers.remove(nodeId); // Exclude self
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.stateStore = new RaftStateStore(stateDir);

        if (stateDir != null) {
            File dir = new File(stateDir);
            this.log = new RaftLog(new File(dir, "raft.log").getAbsolutePath());
            RaftStateStore.PersistedState state = stateStore.load();
            this.currentTerm = state.term;
            this.votedFor = state.votedFor;
        } else {
            this.log = new RaftLog();
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
        stateStore.save(currentTerm, votedFor);
        heartbeatManager.stopHeartbeats();
        electionManager.resetElectionTimeout();
    }

    public synchronized void becomeLeader() {
        if (role != RaftRole.CANDIDATE) return;

        this.role = RaftRole.LEADER;
        this.leaderId = nodeId;

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

        // Broadcast replication immediately via heartbeat manager
        heartbeatManager.broadcastAppendEntries();

        if (peers.isEmpty()) {
            log.setCommitIndex(targetIndex);
            applyCommittedEntries();
            future.complete(true);
            return future;
        }

        clientProposalScheduler.schedule(new Runnable() {
            @Override
            public void run() {
                synchronized (RaftNode.this) {
                    if (log.getCommitIndex() >= targetIndex) {
                        future.complete(true);
                    } else if (role != RaftRole.LEADER || !running.get()) {
                        future.complete(false);
                    } else {
                        clientProposalScheduler.schedule(this, 20, TimeUnit.MILLISECONDS);
                    }
                }
            }
        }, 20, TimeUnit.MILLISECONDS);

        return future;
    }

    // --- Getters & Setters ---

    public String getNodeId() { return nodeId; }
    public synchronized RaftRole getRole() { return role; }
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
