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
 * Measures Linearizable Reads where reads are confirmed through Raft consensus
 * to ensure no stale data is read even during network partitions or leadership transitions.
 */
public class GetLinearizableWorkload implements BenchmarkWorkload {

    @Override
    public String getName() {
        return "Get linearizable";
    }

    @Override
    public BenchmarkMetrics run(RaftCluster cluster, int nodeCount, int clients) {
        RaftNode leader = waitForLeader(cluster);
        if (leader == null) {
            return new BenchmarkMetrics(getName(), nodeCount, clients, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
        }

        // 1. Prepopulate dataset with 200 keys
        int datasetSize = 200;
        for (int i = 0; i < datasetSize; i++) {
            try {
                leader.propose("PUT", "lin-key-" + i, "val-" + i).get(1000, TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {}
        }

        int operations = Math.max(500, clients * 50);
        List<Long> latenciesUs = new CopyOnWriteArrayList<>();
        AtomicInteger errorCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(clients);
        CountDownLatch latch = new CountDownLatch(operations);

        long startWallTimeNs = System.nanoTime();

        for (int i = 0; i < operations; i++) {
            final int keyId = i % datasetSize;
            executor.submit(() -> {
                try {
                    RaftNode currentLeader = waitForLeader(cluster);
                    if (currentLeader == null) {
                        errorCount.incrementAndGet();
                        return;
                    }

                    long startNs = System.nanoTime();
                    // High-performance linearizable read via Leader Lease / ReadIndex
                    CompletableFuture<Object> future = currentLeader.readLinearizable("lin-key-" + keyId);
                    Object val = future.get(3000, TimeUnit.MILLISECONDS);
                    long endNs = System.nanoTime();

                    if (val != null) {
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
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        long endWallTimeNs = System.nanoTime();
        executor.shutdownNow();

        double durationSeconds = (endWallTimeNs - startWallTimeNs) / 1_000_000_000.0;
        List<Long> sortedLatencies = new ArrayList<>(latenciesUs);
        int successCount = operations - errorCount.get();

        return BenchmarkMetrics.fromLatencies(getName(), nodeCount, clients,
                sortedLatencies, operations, successCount, 0, 0, errorCount.get(),
                durationSeconds, 0, 0, 0, 0, 0, successCount);
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
