package test;

import raft.RaftCluster;
import raft.RaftNode;
import raft.RaftRole;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Phase5RaftTest {

    private static final String TEST_DIR = "test_raft_data";

    private static void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) deleteDirectory(f);
                    else f.delete();
                }
            }
            dir.delete();
        }
    }

    public static void run() throws Exception {
        System.out.println("\n--- [Running Phase5RaftTest (Raft Consensus & Distributed Replication)] ---");
        deleteDirectory(new File(TEST_DIR));

        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");

        try (RaftCluster cluster = new RaftCluster(TEST_DIR, nodeIds, 10)) {
            cluster.start();

            // 1. Leader Election Test
            System.out.println("Step 1: Waiting for initial Leader Election...");
            RaftNode leader = waitForLeader(cluster, 3000);
            if (leader == null) {
                throw new AssertionError("Failed to elect a Raft Leader within 3 seconds!");
            }
            System.out.println("  ✓ Leader successfully elected: " + leader.getNodeId() + " in Term " + leader.getCurrentTerm());

            int leaderCount = 0;
            int followerCount = 0;
            for (RaftNode node : cluster.getNodes()) {
                if (node.getRole() == RaftRole.LEADER) leaderCount++;
                if (node.getRole() == RaftRole.FOLLOWER) followerCount++;
            }
            if (leaderCount != 1 || followerCount != 2) {
                throw new AssertionError("Cluster role invariant broken: " + leaderCount + " leaders, " + followerCount + " followers.");
            }

            // 2. Log Replication Test
            System.out.println("Step 2: Proposing Client writes to Leader (" + leader.getNodeId() + ")...");
            CompletableFuture<Boolean> w1 = cluster.propose("PUT", "user:1", "Alice");
            CompletableFuture<Boolean> w2 = cluster.propose("PUT", "user:2", "Bob");
            CompletableFuture<Boolean> w3 = cluster.propose("PUT", "user:3", "Charlie");

            boolean c1 = w1.get(2, TimeUnit.SECONDS);
            boolean c2 = w2.get(2, TimeUnit.SECONDS);
            boolean c3 = w3.get(2, TimeUnit.SECONDS);

            if (!c1 || !c2 || !c3) {
                throw new AssertionError("Failed to commit Raft client proposals with quorum consensus!");
            }

            // Give a short moment for follower state machine apply
            Thread.sleep(150);

            // Verify state machines across all nodes
            for (RaftNode node : cluster.getNodes()) {
                Object val1 = node.getStateMachine().get("user:1");
                Object val2 = node.getStateMachine().get("user:2");
                Object val3 = node.getStateMachine().get("user:3");
                if (!"Alice".equals(val1) || !"Bob".equals(val2) || !"Charlie".equals(val3)) {
                    throw new AssertionError("Node " + node.getNodeId() + " state machine desynchronized: val1=" + val1 + ", val2=" + val2);
                }
            }
            System.out.println("  ✓ Log replication verified across all 3 nodes! All state machines match.");

            // 3. Network Partition & Re-election Test
            String oldLeaderId = leader.getNodeId();
            System.out.println("Step 3: Simulating Leader Crash / Network Isolation on " + oldLeaderId + "...");
            cluster.isolateNode(oldLeaderId);

            System.out.println("  Waiting for remaining nodes to detect failure and elect a new Leader...");
            Thread.sleep(600); // Wait for election timeout

            RaftNode newLeader = waitForLeader(cluster, 3000);
            if (newLeader == null || newLeader.getNodeId().equals(oldLeaderId)) {
                throw new AssertionError("Failed to elect a new leader among healthy partition nodes!");
            }
            System.out.println("  ✓ New Leader elected: " + newLeader.getNodeId() + " in Term " + newLeader.getCurrentTerm());

            // Propose new write on healthy partition (2/3 nodes = majority)
            System.out.println("  Writing 'user:4' -> 'Diana' to new leader (" + newLeader.getNodeId() + ")...");
            CompletableFuture<Boolean> w4 = newLeader.propose("PUT", "user:4", "Diana");
            if (!w4.get(2, TimeUnit.SECONDS)) {
                throw new AssertionError("New leader failed to commit write with majority quorum!");
            }

            // 4. Partition Healing & Catch-Up Test
            System.out.println("Step 4: Healing network partition and recovering " + oldLeaderId + "...");
            cluster.healAll();
            Thread.sleep(300); // Allow heartbeats and catch-up log replication

            RaftNode healedNode = cluster.getNode(oldLeaderId);
            if (healedNode.getRole() == RaftRole.LEADER) {
                throw new AssertionError("Healed old leader did not step down to FOLLOWER!");
            }

            Object val4OnHealed = healedNode.getStateMachine().get("user:4");
            if (!"Diana".equals(val4OnHealed)) {
                throw new AssertionError("Healed node failed to catch up missing state! Expected 'Diana', got: " + val4OnHealed);
            }
            System.out.println("  ✓ Healed node stepped down to Follower, replicated missing logs, and updated state machine to 'Diana'!");
        }

        deleteDirectory(new File(TEST_DIR));
        System.out.println("  ✓ Phase5RaftTest passed successfully!");
    }

    private static RaftNode waitForLeader(RaftCluster cluster, long maxWaitMs) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < maxWaitMs) {
            RaftNode leader = cluster.getLeader();
            if (leader != null) {
                return leader;
            }
            Thread.sleep(50);
        }
        return null;
    }
}
