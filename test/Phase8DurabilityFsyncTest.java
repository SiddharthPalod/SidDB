package test;

import raft.*;
import java.io.File;
import java.util.*;
import java.util.concurrent.*;

public class Phase8DurabilityFsyncTest {

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        System.out.println("\n--- [Running Phase 8.2 Follower WAL Durability & Dynamic fsync Test Suite] ---");
        String baseDir = "data/test_phase8_durability";
        deleteDir(new File(baseDir));

        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");

        try (RaftCluster cluster = new RaftCluster(baseDir, nodeIds, 1000)) {
            cluster.setSyncPolicy(SyncPolicy.SYNC_EVERY_ENTRY);
            cluster.start();

            System.out.println("Step 1: Waiting for cluster leader election with SYNC_EVERY_ENTRY enabled...");
            RaftNode leader = waitForLeader(cluster);
            assert leader != null : "Leader should be elected";
            System.out.println("  ✓ Leader elected: " + leader.getNodeId());

            // Test 1: Verify follower and leader disk files exist and contain fsynced logs
            System.out.println("Step 2: Proposing 10 writes under strict SYNC_EVERY_ENTRY durability...");
            for (int i = 1; i <= 10; i++) {
                boolean ok = leader.propose("PUT", "durable:k" + i, "val" + i).get(2000, TimeUnit.MILLISECONDS);
                assert ok : "Write " + i + " must commit";
            }
            System.out.println("  ✓ 10 writes committed across quorum.");

            // Verify disk files on followers
            System.out.println("Step 3: Checking physical raft.log file sizes on all nodes...");
            for (String id : nodeIds) {
                File nodeRaftLog = new File(baseDir + "/" + id + "/raft.log");
                assert nodeRaftLog.exists() : "raft.log must exist on disk for node " + id;
                assert nodeRaftLog.length() > 0 : "raft.log must not be empty on disk for node " + id;
                System.out.println("  ✓ Node " + id + " raft.log size on disk: " + nodeRaftLog.length() + " bytes (fsynced)");
            }

            // Test 2: Simultaneous hard kill of leader + 1 follower (minority surviving: 1 node alive)
            System.out.println("Step 4: Simulating simultaneous ungraceful crash of leader (" + leader.getNodeId() + ") and node-2...");
            String oldLeaderId = leader.getNodeId();
            cluster.crashNode(oldLeaderId);
            cluster.crashNode("node-2".equals(oldLeaderId) ? "node-1" : "node-2");

            System.out.println("  ✓ 2 of 3 nodes crashed simultaneously without clean memory shutdown.");

            // Test 3: Reboot crashed nodes from disk logs and verify state preservation
            System.out.println("Step 5: Rebooting cluster from physical disk state...");
            cluster.ensureAllNodesRunning();

            RaftNode newLeader = waitForLeader(cluster);
            assert newLeader != null : "New leader should be elected after reboot";
            System.out.println("  ✓ Cluster restored! New leader elected: " + newLeader.getNodeId());

            // Verify all 10 keys survived the simultaneous crash
            System.out.println("Step 6: Invariant Verification: Verifying 10/10 keys recovered with 0 data loss...");
            for (int i = 1; i <= 10; i++) {
                Object val = newLeader.readLinearizable("durable:k" + i).get(1000, TimeUnit.MILLISECONDS);
                assert ("val" + i).equals(val) : "Key durable:k" + i + " must match 'val" + i + "', got: " + val;
            }
            System.out.println("  ✓ 10/10 keys verified 100% intact across rebooted cluster!");

            // Test 4: Dynamic policy switching between SYNC_EVERY_ENTRY and ASYNC_FLUSH
            System.out.println("Step 7: Testing dynamic switching to ASYNC_FLUSH mode...");
            cluster.setSyncPolicy(SyncPolicy.ASYNC_FLUSH);
            assert newLeader.getSyncPolicy() == SyncPolicy.ASYNC_FLUSH : "SyncPolicy must update dynamically";

            boolean asyncOk = newLeader.propose("PUT", "durable:async_key", "async_val").get(1000, TimeUnit.MILLISECONDS);
            assert asyncOk : "Async write should commit";
            Object asyncRead = newLeader.readLinearizable("durable:async_key").get(500, TimeUnit.MILLISECONDS);
            assert "async_val".equals(asyncRead) : "Async read must match";
            System.out.println("  ✓ Dynamic policy switch to ASYNC_FLUSH verified.");

            System.out.println("[+] Phase 8.2 Follower WAL Durability & Dynamic fsync Test PASSED 100%!");
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