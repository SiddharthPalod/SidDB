package test;

import engine.SidDBEngine;
import network.SimulatedNetwork;
import raft.RaftCluster;
import raft.RaftNode;
import raft.RaftRole;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Phase5RaftCrashRecoveryTest {

    private static final String TEST_DIR = "test_raft_recovery_data";

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
        System.out.println("\n--- [Running Phase5RaftCrashRecoveryTest (Raft Disk Persistence & Node Crash Recovery)] ---");
        deleteDirectory(new File(TEST_DIR));

        List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");

        // 1. Initial Cluster Run
        try (RaftCluster cluster = new RaftCluster(TEST_DIR, nodeIds, 10)) {
            cluster.start();

            // Wait for leader
            RaftNode leader = null;
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 3000) {
                leader = cluster.getLeader();
                if (leader != null) break;
                Thread.sleep(50);
            }

            if (leader == null) {
                throw new AssertionError("Failed to elect leader!");
            }
            System.out.println("  Leader elected: " + leader.getNodeId() + " (Term " + leader.getCurrentTerm() + ")");

            // Write 3 entries
            cluster.propose("PUT", "persist:1", "Alpha").get(2, TimeUnit.SECONDS);
            cluster.propose("PUT", "persist:2", "Beta").get(2, TimeUnit.SECONDS);
            cluster.propose("PUT", "persist:3", "Gamma").get(2, TimeUnit.SECONDS);
            Thread.sleep(150);

            // Record node-1 term and log size
            RaftNode node1 = cluster.getNode("node-1");
            long originalTerm = node1.getCurrentTerm();
            long originalLogSize = node1.getLog().getLastLogIndex();

            // 2. Simulate Node Crash: Stop node-1 cleanly and close engine
            System.out.println("  Simulating crash on node-1...");
            cluster.isolateNode("node-1");
            node1.stop();
            node1.close();

            // 3. Reboot node-1 from persistent disk files
            System.out.println("  Rebooting node-1 from disk state (raft.meta, raft.log, and LSM files)...");
            File node1Dir = new File(new File(TEST_DIR), "node-1");
            SidDBEngine recoveredEngine = new SidDBEngine(node1Dir.getAbsolutePath(), 10);
            RaftNode rebootedNode1 = new RaftNode("node-1", nodeIds, cluster.getNetwork(), recoveredEngine, node1Dir.getAbsolutePath());

            if (rebootedNode1.getCurrentTerm() != originalTerm) {
                throw new AssertionError("Rebooted node failed to restore currentTerm! Expected " + originalTerm + ", got " + rebootedNode1.getCurrentTerm());
            }
            if (rebootedNode1.getLog().getLastLogIndex() != originalLogSize) {
                throw new AssertionError("Rebooted node failed to restore RaftLog! Expected " + originalLogSize + ", got " + rebootedNode1.getLog().getLastLogIndex());
            }

            // Verify underlying state machine recovered
            Object v1 = recoveredEngine.get("persist:1");
            Object v2 = recoveredEngine.get("persist:2");
            Object v3 = recoveredEngine.get("persist:3");

            if (!"Alpha".equals(v1) || !"Beta".equals(v2) || !"Gamma".equals(v3)) {
                throw new AssertionError("Underlying SidDBEngine failed to recover applied data: v1=" + v1 + ", v2=" + v2);
            }

            System.out.println("  ✓ Recovered term (" + rebootedNode1.getCurrentTerm() + "), log entries (" + rebootedNode1.getLog().getLastLogIndex() + " entries), and state machine values perfectly!");
            rebootedNode1.close();
        }

        deleteDirectory(new File(TEST_DIR));
        System.out.println("  ✓ Phase5RaftCrashRecoveryTest passed successfully!");
    }
}
