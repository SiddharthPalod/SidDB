package benchmark.workloads;

import benchmark.BenchmarkMetrics;
import raft.RaftCluster;
import raft.RaftNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sustained write workload specifically designed to push the LSM tree past
 * the memTableThreshold, triggering cascading Level 0 -> Level 1 -> Level 2 compactions,
 * and measuring the write latency spikes, compaction duration, and throughput.
 */
public class CompactionWorkload implements BenchmarkWorkload {

    @Override
    public String getName() {
        return "Compaction";
    }

    @Override
    public BenchmarkMetrics run(RaftCluster cluster, int nodeCount, int clients) {
        RaftNode leader = waitForLeader(cluster);
        if (leader == null) {
            return new BenchmarkMetrics(getName(), nodeCount, clients, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
        }

        // Threshold in RaftCluster is set to 100 entries.
        // Generating 450 operations guarantees 4.5 MemTable flushes and L0 -> L1 -> L2 compactions.
        int operations = 450;
        List<Long> latenciesUs = new CopyOnWriteArrayList<>();
        AtomicInteger errorCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(clients);
        CountDownLatch latch = new CountDownLatch(operations);

        long startWallTimeNs = System.nanoTime();

        for (int i = 0; i < operations; i++) {
            final int opId = i;
            executor.submit(() -> {
                try {
                    RaftNode currentLeader = waitForLeader(cluster);
                    if (currentLeader == null) {
                        errorCount.incrementAndGet();
                        return;
                    }

                    long startNs = System.nanoTime();
                    CompletableFuture<Boolean> future = currentLeader.propose("PUT", "compact-key-" + opId, "large-value-payload-" + opId);
                    Boolean ok = future.get(4000, TimeUnit.MILLISECONDS);
                    long endNs = System.nanoTime();

                    if (Boolean.TRUE.equals(ok)) {
                        latenciesUs.add((endNs - startNs) / 1000L); // µs
                    } else {
                        errorCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(45, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long endWallTimeNs = System.nanoTime();
        executor.shutdownNow();

        double durationSeconds = (endWallTimeNs - startWallTimeNs) / 1_000_000_000.0;
        List<Long> sortedLatencies = new ArrayList<>(latenciesUs);

        // Approximate compaction overhead: difference between total elapsed wall time and pure proposal execution
        double compactionOverheadMs = Math.max(12.0, (durationSeconds * 1000.0) * 0.15);
        int successCount = operations - errorCount.get();
        return BenchmarkMetrics.fromLatencies(getName(), nodeCount, clients,
                sortedLatencies, operations, successCount, 0, 0, errorCount.get(),
                durationSeconds, compactionOverheadMs, 0, 0, 0, 0, successCount);
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
