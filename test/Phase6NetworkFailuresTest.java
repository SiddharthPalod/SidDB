package test;

import chaos.ChaosClusterContext;
import chaos.ChaosEngine;
import chaos.ChaosReport;
import network.ChaoticTransport;
import network.SimulatedNetwork;
import raft.RaftCluster;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Phase6NetworkFailuresTest {

    public static void main(String[] args) {
        run();
    }

    public static void run() {
        System.out.println("=== Running Phase 6 Network Failures & Chaos Testing Suite ===");

        String testDir = "test-data-phase6-chaos";
        deleteDir(new File(testDir));

        try {
            // Setup 3-node cluster with ChaoticTransport Decorator
            SimulatedNetwork underlying = new SimulatedNetwork();
            ChaoticTransport transport = new ChaoticTransport(underlying);
            List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");

            RaftCluster cluster = new RaftCluster(testDir, nodeIds, 100, transport);
            cluster.start();

            ChaosClusterContext ctx = new ChaosClusterContext(cluster, transport);
            ChaosEngine engine = ChaosEngine.createStandardSuite();

            System.out.println("[*] Executing all 9 failure mode scenarios...");
            ChaosReport report = engine.run(ctx);

            // Print report markdown preview
            System.out.println("\n" + report.toMarkdown());

            // Save report to disk
            String reportPath = "ChaosReport.md";
            report.writeToFile(reportPath);
            System.out.println("[+] Consistency & Partition Tolerance Report written to: " + reportPath);

            // Close cluster
            cluster.close();

            if (!report.isAllPassed()) {
                throw new RuntimeException("One or more chaos scenarios failed! Check report details above.");
            }

            System.out.println("[+] Phase 6 Network Failures & Chaos Test PASSED successfully!");

        } catch (Exception e) {
            System.err.println("[-] Phase 6 Test FAILED: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            deleteDir(new File(testDir));
        }
    }

    private static void deleteDir(File file) {
        if (file.exists()) {
            File[] contents = file.listFiles();
            if (contents != null) {
                for (File f : contents) {
                    deleteDir(f);
                }
            }
            file.delete();
        }
    }
}
