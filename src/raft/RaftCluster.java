package raft;

import engine.SidDBEngine;
import network.SimulatedNetwork;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class RaftCluster implements AutoCloseable {

    private final SimulatedNetwork network;
    private final Map<String, RaftNode> nodes;
    private final List<String> nodeIds;
    private final String baseDir;
    private final int memTableThreshold;

    public RaftCluster(String baseDir, List<String> nodeIds, int memTableThreshold) throws IOException {
        this.baseDir = baseDir;
        this.nodeIds = new ArrayList<>(nodeIds);
        this.memTableThreshold = memTableThreshold;
        this.network = new SimulatedNetwork();
        this.nodes = new LinkedHashMap<>();

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
            RaftNode node = new RaftNode(id, nodeIds, network, engine, nodeDir.getAbsolutePath());
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

    public synchronized RaftNode getLeader() {
        for (RaftNode node : nodes.values()) {
            if (node.getRole() == RaftRole.LEADER && !network.isIsolated(node.getNodeId())) {
                return node;
            }
        }
        return null;
    }

    public synchronized RaftNode getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public synchronized Collection<RaftNode> getNodes() {
        return nodes.values();
    }

    public synchronized SimulatedNetwork getNetwork() {
        return network;
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
