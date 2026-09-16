package raft;

import engine.SidDBEngine;
import network.SimulatedNetwork;
import network.Transport;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class RaftCluster implements AutoCloseable {

    private final SimulatedNetwork network;
    private final Transport transport;
    private final Map<String, RaftNode> nodes;
    private final List<String> nodeIds;
    private final String baseDir;
    private final int memTableThreshold;

    public RaftCluster(String baseDir, List<String> nodeIds, int memTableThreshold) throws IOException {
        this(baseDir, nodeIds, memTableThreshold, null);
    }

    public RaftCluster(String baseDir, List<String> nodeIds, int memTableThreshold, Transport customTransport) throws IOException {
        this.baseDir = baseDir;
        this.nodeIds = new ArrayList<>(nodeIds);
        this.memTableThreshold = memTableThreshold;
        if (customTransport instanceof network.ChaoticTransport) {
            this.transport = customTransport;
            Transport del = ((network.ChaoticTransport) customTransport).getDelegate();
            this.network = (del instanceof SimulatedNetwork) ? (SimulatedNetwork) del : new SimulatedNetwork();
        } else if (customTransport instanceof SimulatedNetwork) {
            this.network = (SimulatedNetwork) customTransport;
            this.transport = customTransport;
        } else {
            this.network = new SimulatedNetwork();
            this.transport = (customTransport != null) ? customTransport : this.network;
        }
        this.nodes = new ConcurrentHashMap<>();

        File base = new File(baseDir);
        if (!base.exists()) {
            base.mkdirs();
        }

        for (String id : nodeIds) {
            File nodeDir = new File(base, id);
            if (!nodeDir.exists()) {
                nodeDir.mkdirs();
            }
            SidDBEngine engine = new SidDBEngine(nodeDir.getAbsolutePath(), memTableThreshold);
            RaftNode node = new RaftNode(id, nodeIds, this.transport, engine, nodeDir.getAbsolutePath());
            nodes.put(id, node);
        }
    }

    public synchronized void start() {
        for (RaftNode node : nodes.values()) {
            node.start();
        }
    }

    public synchronized void stop() {
        for (RaftNode node : nodes.values()) {
            node.stop();
        }
    }

    public RaftNode getLeader() {
        for (RaftNode node : nodes.values()) {
            if (node.getRole() == RaftRole.LEADER && !network.isIsolated(node.getNodeId())) {
                return node;
            }
        }
        return null;
    }

    public RaftNode getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public Collection<RaftNode> getNodes() {
        return nodes.values();
    }

    public synchronized SimulatedNetwork getNetwork() {
        return network;
    }

    public synchronized Transport getTransport() {
        return transport;
    }

    public synchronized void crashNode(String nodeId) {
        RaftNode node = nodes.get(nodeId);
        if (node != null) {
            node.close();
        }
    }

    public synchronized RaftNode restartNode(String nodeId) throws IOException {
        File nodeDir = new File(baseDir, nodeId);
        SidDBEngine engine = new SidDBEngine(nodeDir.getAbsolutePath(), memTableThreshold);
        RaftNode node = new RaftNode(nodeId, nodeIds, this.transport, engine, nodeDir.getAbsolutePath());
        nodes.put(nodeId, node);
        node.start();
        return node;
    }

    public synchronized void ensureAllNodesRunning() {
        for (String id : nodeIds) {
            RaftNode n = nodes.get(id);
            if (n == null || !n.isRunning()) {
                try {
                    restartNode(id);
                } catch (IOException ignored) {}
            }
        }
    }

    public synchronized boolean verifyClusterConsistency(String key, Object expectedValue) {
        for (RaftNode node : nodes.values()) {
            if (node.isRunning() && node.getStateMachine() != null) {
                try {
                    Object val = node.getStateMachine().get(key);
                    if (!Objects.equals(val, expectedValue)) {
                        return false;
                    }
                } catch (IOException e) {
                    return false;
                }
            }
        }
        return true;
    }

    public CompletableFuture<Boolean> propose(String commandType, String key, Object value) {
        RaftNode leader = getLeader();
        if (leader == null) {
            CompletableFuture<Boolean> failed = new CompletableFuture<>();
            failed.complete(false);
            return failed;
        }
        return leader.propose(commandType, key, value);
    }

    public void isolateNode(String nodeId) {
        network.isolateNode(nodeId);
    }

    public void healNode(String nodeId) {
        network.healNode(nodeId);
    }

    public void partition(List<Set<String>> groups) {
        network.createPartition(groups);
    }

    public void healAll() {
        network.healAll();
    }

    public synchronized Map<String, Object> getClusterStatus() {
        Map<String, Object> status = new LinkedHashMap<>();
        List<Map<String, Object>> nodeList = new ArrayList<>();

        for (RaftNode node : nodes.values()) {
            Map<String, Object> nMap = new LinkedHashMap<>(node.getStatusMap());
            nMap.put("isIsolated", network.isIsolated(node.getNodeId()));

            List<Map<String, Object>> entriesList = new ArrayList<>();
            for (RaftLogEntry entry : node.getLog().getEntries()) {
                if (entry.getIndex() == 0) continue; // skip dummy
                Map<String, Object> eMap = new LinkedHashMap<>();
                eMap.put("index", entry.getIndex());
                eMap.put("term", entry.getTerm());
                eMap.put("cmd", entry.getCommandType());
                eMap.put("key", entry.getKey());
                eMap.put("val", entry.getValue());
                eMap.put("isCommitted", entry.getIndex() <= node.getLog().getCommitIndex());
                entriesList.add(eMap);
            }
            nMap.put("logEntries", entriesList);
            nodeList.add(nMap);
        }

        status.put("nodes", nodeList);
        status.put("leader", getLeader() != null ? getLeader().getNodeId() : "NONE");
        status.put("messagesSent", network.getMessagesSent());
        status.put("messagesDropped", network.getMessagesDropped());
        status.put("isolatedNodes", new ArrayList<>(network.getIsolatedNodes()));
        return status;
    }

    @Override
    public void close() {
        stop();
        for (RaftNode node : nodes.values()) {
            node.close();
        }
    }
}
