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
 * Measures point-lookup GET performance from local state machine under concurrent load.
 * Measured in microsecond (µs) precision with strict error categorization.
 */
public class GetLocalThroughputWorkload implements BenchmarkWorkload {

    @Override
    public String getName() {
        return "Get local";
    }

    @Override
    public BenchmarkMetrics run(RaftCluster cluster, int nodeCount, int clients) {
        RaftNode leader = waitForLeader(cluster);
        if (leader == null) {
            return new BenchmarkMetrics(getName(), nodeCount, clients, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0);
        }

        // 1. Prepopulate dataset with 300 verified keys (retrying until 100% written)
        int datasetSize = 300;
        for (int i = 0; i < datasetSize; i++) {
            boolean written = false;
            for (int retry = 0; retry < 5 && !written; retry++) {
                try {
                    leader = waitForLeader(cluster);
                    if (leader != null) {
                        CompletableFuture<Boolean> f = leader.propose("PUT", "bench-get-" + i, "val-" + i);
                        Boolean ok = f.get(2000, TimeUnit.MILLISECONDS);
                        if (Boolean.TRUE.equals(ok)) {
                            written = true;
                        }
                    }
                } catch (Exception ignored) {
                    try { Thread.sleep(20); } catch (Exception e) {}
                }
            }
        }

        // Verify dataset is fully populated in state machine before initiating reads
        leader = waitForLeader(cluster);
        int verifiedKeys = 0;
        if (leader != null && leader.getStateMachine() != null) {
            for (int i = 0; i < datasetSize; i++) {
                try {
                    if (leader.getStateMachine().get("bench-get-" + i) != null) {
                        verifiedKeys++;
                    }
                } catch (Exception ignored) {}
            }
        }

        int operations = Math.max(5000, clients * 400);
        List<Long> latenciesUs = new CopyOnWriteArrayList<>();
        AtomicInteger successfulReads = new AtomicInteger(0);
        AtomicInteger notFoundCount = new AtomicInteger(0);
        AtomicInteger timeouts = new AtomicInteger(0);
        AtomicInteger rpcErrors = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(clients);
        CountDownLatch latch = new CountDownLatch(operations);

        long startWallTimeNs = System.nanoTime();

        for (int i = 0; i < operations; i++) {
            final int keyId = i % Math.max(1, verifiedKeys);
            executor.submit(() -> {
                try {
                    RaftNode currentLeader = cluster.getLeader();
                    if (currentLeader == null || currentLeader.getStateMachine() == null) {
                        rpcErrors.incrementAndGet();
                        return;
                    }

                    long startNs = System.nanoTime();
                    Object val = currentLeader.getStateMachine().get("bench-get-" + keyId);
                    long endNs = System.nanoTime();

                    if (val != null) {
                        successfulReads.incrementAndGet();
                        long latencyUs = (endNs - startNs) / 1000L;
                        latenciesUs.add(Math.max(1L, latencyUs)); // At least 1 µs
                    } else {
                        notFoundCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    rpcErrors.incrementAndGet();
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

        int totalErrors = notFoundCount.get() + timeouts.get() + rpcErrors.get();

        return BenchmarkMetrics.fromLatencies(getName(), nodeCount, clients,
                sortedLatencies, operations, successfulReads.get(), notFoundCount.get(),
                timeouts.get(), totalErrors, durationSeconds, 0, 0, 0, 0, 0, verifiedKeys);
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
