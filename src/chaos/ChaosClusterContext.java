package chaos;

import network.ChaoticTransport;
import raft.RaftCluster;
import raft.RaftNode;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Value Object / Context passing object for Chaos Scenarios.
 * Encapsulates cluster manipulation and state query utilities.
 */
public class ChaosClusterContext {

    private final RaftCluster cluster;
    private final ChaoticTransport transport;

    public ChaosClusterContext(RaftCluster cluster, ChaoticTransport transport) {
        this.cluster = cluster;
        this.transport = transport;
    }

    public RaftCluster getCluster() {
        return cluster;
    }

    public ChaoticTransport getTransport() {
        return transport;
    }

    public RaftNode getLeader() {
        return cluster.getLeader();
    }

    public RaftNode getNode(String nodeId) {
        return cluster.getNode(nodeId);
    }

    public boolean propose(String key, Object value, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            long waitWindow = Math.min(600, Math.max(50, deadline - System.currentTimeMillis()));
            RaftNode leader = waitForLeader(waitWindow);
            if (leader != null) {
                try {
                    long remaining = Math.max(100, deadline - System.currentTimeMillis());
                    boolean ok = leader.propose("PUT", key, value).get(remaining, TimeUnit.MILLISECONDS);
                    if (ok) return true;
                } catch (TimeoutException e) {
                    return false;
                } catch (Exception ignored) {
                    // Leader might have stepped down; retry with new leader if time permits
                }
            }
            sleep(40);
        }
        return false;
    }

    public RaftNode waitForLeader(long maxWaitMs) {
        long deadline = System.currentTimeMillis() + maxWaitMs;
        while (System.currentTimeMillis() < deadline) {
            RaftNode leader = getLeader();
            if (leader != null) {
                return leader;
            }
            sleep(50);
        }
        return null;
    }

    public Map<String, Object> collectAllNodeValues(String key) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (RaftNode node : cluster.getNodes()) {
            if (node.getStateMachine() != null && node.isRunning()) {
                try {
                    Object val = node.getStateMachine().get(key);
                    values.put(node.getNodeId(), val);
                } catch (IOException e) {
                    values.put(node.getNodeId(), "ERROR: " + e.getMessage());
                }
            } else {
                values.put(node.getNodeId(), "OFFLINE");
            }
        }
        return values;
    }

    public boolean waitForConsistency(String key, Object expectedValue, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cluster.verifyClusterConsistency(key, expectedValue)) {
                return true;
            }
            sleep(40);
        }
        return cluster.verifyClusterConsistency(key, expectedValue);
    }

    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {}
    }
}
