package benchmark.workloads;

import benchmark.BenchmarkMetrics;
import raft.RaftCluster;
import raft.RaftNode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Measures 3 distinct phases of cluster recovery after an unexpected leader crash:
 * Phase A: Election Time (ms until new leader elected)
 * Phase B: Service Recovery Time (ms until first write commits on new leader)
 * Phase C: Data Durability (every acknowledged write remains present post-failover)
 */
public class RecoveryWorkload implements BenchmarkWorkload {

    @Override
    public String getName() {
        return "Leader recovery";
    }

    @Override
    public BenchmarkMetrics run(RaftCluster cluster, int nodeCount, int clients) {
        if (nodeCount < 3) {
            return new BenchmarkMetrics(getName(), nodeCount, clients, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        RaftNode leader = waitForLeader(cluster);
        if (leader == null) {
            return new BenchmarkMetrics(getName(), nodeCount, clients, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
        }

        // 1. Commit 50 pre-crash verified records
        int preCommitCount = 50;
        for (int i = 0; i < preCommitCount; i++) {
            try {
                CompletableFuture<Boolean> f = leader.propose("PUT", "recovery-key-" + i, "durable-val-" + i);
                f.get(1500, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {}
        }

        leader = waitForLeader(cluster);
        if (leader == null) {
            return new BenchmarkMetrics(getName(), nodeCount, clients, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
        }

        String oldLeaderId = leader.getNodeId();
        long crashNano = System.nanoTime();
        cluster.crashNode(oldLeaderId);

        // Phase A: Measure Election Time
        RaftNode newLeader = null;
        long deadline = System.currentTimeMillis() + 8000;
        while (System.currentTimeMillis() < deadline) {
            newLeader = cluster.getLeader();
            if (newLeader != null && !newLeader.getNodeId().equals(oldLeaderId) && newLeader.isRunning()) {
                break;
            }
            try { Thread.sleep(10); } catch (Exception ignored) {}
        }

        long electionNano = System.nanoTime() - crashNano;
        double electionTimeMs = electionNano / 1_000_000.0;

        // Phase B: Measure Service Recovery Time (First successful write on new leader)
        double serviceRecoveryTimeMs = 0;
        boolean writeResumed = false;
        if (newLeader != null) {
            try {
                CompletableFuture<Boolean> resumeWrite = newLeader.propose("PUT", "recovery-probe", "ok");
                Boolean ok = resumeWrite.get(3000, TimeUnit.MILLISECONDS);
                writeResumed = Boolean.TRUE.equals(ok);
                long serviceNano = System.nanoTime() - crashNano;
                serviceRecoveryTimeMs = serviceNano / 1_000_000.0;
            } catch (Exception ignored) {}
        }

        // Phase C: Validate Data Durability & Invariants
        int survivingRecords = 0;
        int errors = 0;

        if (newLeader == null || !writeResumed) {
            errors = 1;
        } else {
            for (int i = 0; i < preCommitCount; i++) {
                try {
                    Object v = newLeader.getStateMachine().get("recovery-key-" + i);
                    if (v != null) {
                        survivingRecords++;
                    } else {
                        errors++;
                    }
                } catch (Exception e) {
                    errors++;
                }
            }
        }

        // Restart old node to heal cluster
        try {
            cluster.restartNode(oldLeaderId);
            Thread.sleep(200);
        } catch (Exception ignored) {}

        List<Long> lat = new ArrayList<>();
        lat.add((long) (serviceRecoveryTimeMs * 1000.0)); // µs

        return BenchmarkMetrics.fromLatencies(getName(), nodeCount, clients,
                lat, preCommitCount, survivingRecords, 0, 0, errors,
                serviceRecoveryTimeMs / 1000.0, 0, electionTimeMs,
                electionTimeMs, serviceRecoveryTimeMs, 0, survivingRecords);
    }

    private RaftNode waitForLeader(RaftCluster cluster) {
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            RaftNode leader = cluster.getLeader();
            if (leader != null && leader.isRunning()) return leader;
            try { Thread.sleep(20); } catch (Exception ignored) {}
        }
        return null;
    }
}
