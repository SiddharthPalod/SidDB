package test;

import raft.*;
import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public class Phase8LeaderLeaseTest {

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        System.out.println("\n--- [Running Phase 8.1 Leader Lease & ReadIndex Test Suite] ---");
        String baseDir = "data/test_phase8_lease";
        deleteDir(new File(baseDir));

        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");

        try (RaftCluster cluster = new RaftCluster(baseDir, nodeIds, 1000)) {
            cluster.start();

            System.out.println("Step 1: Waiting for initial leader election and quorum heartbeat lease grant...");
            RaftNode leader = waitForLeader(cluster);
            assert leader != null : "Leader should be elected";
            System.out.println("  ✓ Leader elected: " + leader.getNodeId());

            // Allow initial heartbeat broadcast
            Thread.sleep(150);

            // Test 1: Verify leader lease is held
            System.out.println("Step 2: Verifying Leader Lease status on active leader...");
            assert leader.hasValidLeaderLease() : "Leader should hold a valid lease after quorum heartbeat";
            System.out.println("  ✓ Leader lease valid! (Lease expiry: " + leader.getLeaseExpiryNs() + " ns)");

            // Test 2: Perform write and verify high-performance LEADER_LEASE linearizable read
            System.out.println("Step 3: Performing write and reading under LEADER_LEASE mode...");
            boolean putOk = leader.propose("PUT", "phase8:key1", "phase8_val1").get(2000, TimeUnit.MILLISECONDS);
            assert putOk : "PUT proposal should succeed";

            long startNs = System.nanoTime();
            Object readVal = leader.readLinearizable("phase8:key1").get(500, TimeUnit.MILLISECONDS);
            long latencyNs = System.nanoTime() - startNs;

            assert "phase8_val1".equals(readVal) : "Read value must match committed value, got: " + readVal;
            System.out.println("  ✓ Read value matched: '" + readVal + "' (Latency: " + (latencyNs / 1000) + " µs)");

            // Test 3: Test ReadIndex protocol explicitly
            System.out.println("Step 4: Testing READ_INDEX mode (Heartbeat quorum confirmation without log append)...");
            leader.setReadMode(ReadMode.READ_INDEX);
            startNs = System.nanoTime();
            Object readIndexVal = leader.readLinearizable("phase8:key1").get(1000, TimeUnit.MILLISECONDS);
            long readIndexLatencyNs = System.nanoTime() - startNs;

            assert "phase8_val1".equals(readIndexVal) : "ReadIndex value must match committed value";
            System.out.println("  ✓ ReadIndex protocol confirmed value: '" + readIndexVal + "' (Latency: " + (readIndexLatencyNs / 1000) + " µs)");

            // Reset back to LEADER_LEASE
            leader.setReadMode(ReadMode.LEADER_LEASE);

            // Test 4: Verify lease expiration / safety under network isolation
            System.out.println("Step 5: Simulating leader network isolation and testing lease expiry safety...");
            cluster.isolateNode(leader.getNodeId());
            // Wait beyond election timeout / lease duration for lease to expire
            Thread.sleep(350);

            assert !leader.hasValidLeaderLease() : "Isolated leader lease must have expired";
            System.out.println("  ✓ Isolated leader lease correctly expired.");

            // Test 5: Verify new leader in majority partition acquires valid lease
            System.out.println("Step 6: Waiting for new leader in remaining majority partition (node-2, node-3)...");
            RaftNode newLeader = null;
            long deadline = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < deadline) {
                RaftNode candidate = cluster.getLeader();
                if (candidate != null && !candidate.getNodeId().equals(leader.getNodeId())) {
                    newLeader = candidate;
                    break;
                }
                Thread.sleep(50);
            }
            assert newLeader != null : "New leader should be elected in majority partition";
            System.out.println("  ✓ New leader elected: " + newLeader.getNodeId());

            Thread.sleep(150); // Allow heartbeats in majority
            assert newLeader.hasValidLeaderLease() : "New leader should have valid lease in majority";

            // Write through new leader and verify read
            boolean putNewOk = newLeader.propose("PUT", "phase8:key2", "phase8_val2").get(2000, TimeUnit.MILLISECONDS);
            assert putNewOk : "PUT to new leader should succeed";
            Object val2 = newLeader.readLinearizable("phase8:key2").get(500, TimeUnit.MILLISECONDS);
            assert "phase8_val2".equals(val2) : "Linearizable read on new leader must match";
            System.out.println("  ✓ New leader linearizable read verified: '" + val2 + "'");

            // Heal isolated node
            System.out.println("Step 7: Healing isolated node and verifying cluster-wide reconciliation...");
            cluster.healNode(leader.getNodeId());
            Thread.sleep(300);

            Object reconciledVal = cluster.readLinearizable("phase8:key2").get(500, TimeUnit.MILLISECONDS);
            assert "phase8_val2".equals(reconciledVal) : "Cluster read must return latest value";
            System.out.println("  ✓ Cluster-wide read healed and consistent: '" + reconciledVal + "'");

            System.out.println("[+] Phase 8.1 Leader Lease & ReadIndex Test Suite PASSED 100%!");
        } finally {
            deleteDir(new File(baseDir));
        }
    }

    private static RaftNode waitForLeader(RaftCluster cluster) {
        long deadline = System.currentTimeMillis() + 4000;
        while (System.currentTimeMillis() < deadline) {
            RaftNode leader = cluster.getLeader();
            if (leader != null && leader.isRunning()) return leader;
            try { Thread.sleep(20); } catch (Exception ignored) {}
        }
        return null;
    }

    private static void deleteDir(File file) {
        if (file.isDirectory()) {
            File[] entries = file.listFiles();
            if (entries != null) {
                for (File entry : entries) {
                    deleteDir(entry);
                }
            }
        }
        file.delete();
    }
}
