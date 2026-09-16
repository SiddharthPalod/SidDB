package benchmark;

import benchmark.workloads.*;
import raft.RaftCluster;
import raft.RaftNode;

import java.io.File;
import java.util.*;

public class BenchmarkRunner {

    public static class WorkloadSpec {
        final BenchmarkWorkload workload;
        final int nodeCount;
        final int clients;
        final int iterations;

        public WorkloadSpec(BenchmarkWorkload workload, int nodeCount, int clients, int iterations) {
            this.workload = workload;
            this.nodeCount = nodeCount;
            this.clients = clients;
            this.iterations = iterations;
        }
    }

    public static void main(String[] args) {
        System.out.println("Starting SidDB Phase 7.2 Production-Grade Multi-Iteration Benchmarking Suite...");

        List<WorkloadSpec> specs = new ArrayList<>();

        // Core Write Concurrency Matrix (Run 3 iterations to compute median, min, max, std dev)
        specs.add(new WorkloadSpec(new PutThroughputWorkload(), 3, 1, 2));
        specs.add(new WorkloadSpec(new PutThroughputWorkload(), 3, 16, 3));
        specs.add(new WorkloadSpec(new PutThroughputWorkload(), 3, 64, 2));
        specs.add(new WorkloadSpec(new PutThroughputWorkload(), 5, 16, 3));
        specs.add(new WorkloadSpec(new PutThroughputWorkload(), 1, 16, 2));

        // Read Path: Local In-Memory vs Raft Linearizable
        specs.add(new WorkloadSpec(new GetLocalThroughputWorkload(), 3, 16, 2));
        specs.add(new WorkloadSpec(new GetLocalThroughputWorkload(), 5, 16, 2));
        specs.add(new WorkloadSpec(new GetLinearizableWorkload(), 3, 16, 2));
        specs.add(new WorkloadSpec(new GetLinearizableWorkload(), 5, 16, 2));

        // Compaction Stress Test
        specs.add(new WorkloadSpec(new CompactionWorkload(), 3, 16, 1));

        // 3-Stage Leader Recovery (Election, Service Resume, Data Preservation)
        specs.add(new WorkloadSpec(new RecoveryWorkload(), 3, 1, 2));
        specs.add(new WorkloadSpec(new RecoveryWorkload(), 5, 1, 2));

        Map<String, BenchmarkReportGenerator.RunAggregate> aggregates = new LinkedHashMap<>();
        String baseBenchmarkDir = "benchmark-data";

        for (int i = 0; i < specs.size(); i++) {
            WorkloadSpec spec = specs.get(i);
            String key = spec.workload.getName() + "_" + spec.nodeCount + "_" + spec.clients;
            BenchmarkReportGenerator.RunAggregate agg = aggregates.computeIfAbsent(
                    key, k -> new BenchmarkReportGenerator.RunAggregate(spec.workload.getName(), spec.nodeCount, spec.clients));

            System.out.printf("\n[%d/%d] Executing %s (%d nodes, %d clients, %d iterations)...%n",
                    i + 1, specs.size(), spec.workload.getName(), spec.nodeCount, spec.clients, spec.iterations);

            for (int iter = 1; iter <= spec.iterations; iter++) {
                String runDir = baseBenchmarkDir + File.separator + "run-" + System.currentTimeMillis() + "-" + spec.nodeCount;
                File dir = new File(runDir);
                dir.mkdirs();

                List<String> nodeIds = new ArrayList<>();
                for (int n = 1; n <= spec.nodeCount; n++) {
                    nodeIds.add("node-" + n);
                }

                try (RaftCluster cluster = new RaftCluster(runDir, nodeIds, 100)) {
                    cluster.start();
                    waitForClusterLeader(cluster);
                    cluster.ensureAllNodesRunning();
                    Thread.sleep(300);

                    BenchmarkMetrics metrics = spec.workload.run(cluster, spec.nodeCount, spec.clients);
                    agg.addRun(metrics);
                    System.out.printf("   Iteration %d/%d: %.2f ops/sec (P50: %.2f ms, errors: %d)%n",
                            iter, spec.iterations, metrics.getThroughput(), metrics.getP50LatencyUs() / 1000.0, metrics.getErrors());
                } catch (Exception e) {
                    System.err.println("Error running iteration " + iter + ": " + e.getMessage());
                } finally {
                    safeDelete(dir);
                }
            }
        }

        System.out.println("\nGenerating Final Production-Grade Benchmark Reports...\n");
        BenchmarkReportGenerator.generateReport(aggregates, "BenchmarkReport.md");
    }

    private static void waitForClusterLeader(RaftCluster cluster) {
        long deadline = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < deadline) {
            RaftNode leader = cluster.getLeader();
            if (leader != null && leader.isRunning()) {
                return;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static void safeDelete(File f) {
        if (f == null || !f.exists()) return;
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            if (files != null) {
                for (File child : files) safeDelete(child);
            }
        }
        try {
            f.delete();
        } catch (Exception ignored) {}
    }
}
