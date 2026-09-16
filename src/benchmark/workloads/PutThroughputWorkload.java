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
 * Benchmarks PUT throughput and latency across multiple concurrent clients.
 * Latency is measured at microsecond precision (µs) with clear timeout/saturation accounting.
 */
public class PutThroughputWorkload implements BenchmarkWorkload {

    public static final long CLIENT_PROPOSAL_TIMEOUT_MS = 2500;

    @Override
    public String getName() {
        return "Put";
    }

    @Override
    public BenchmarkMetrics run(RaftCluster cluster, int nodeCount, int clients) {
        int operations = Math.min(800, Math.max(150, clients * 25));
        List<Long> latenciesUs = new CopyOnWriteArrayList<>();
        AtomicInteger successfulOps = new AtomicInteger(0);
        AtomicInteger timeoutCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        ExecutorService executor = Executors.newFixedThreadPool(clients);
        CountDownLatch latch = new CountDownLatch(operations);

        long startWallTimeNs = System.nanoTime();

        for (int i = 0; i < operations; i++) {
            final int opId = i;
            executor.submit(() -> {
                try {
                    RaftNode leader = waitForLeader(cluster);
                    if (leader == null) {
                        errorCount.incrementAndGet();
                        return;
                    }

                    long startNs = System.nanoTime();
                    CompletableFuture<Boolean> future = leader.propose("PUT", "bench-key-" + opId, "val-" + opId);
                    Boolean ok = future.get(CLIENT_PROPOSAL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    long endNs = System.nanoTime();

                    if (Boolean.TRUE.equals(ok)) {
                        successfulOps.incrementAndGet();
                        latenciesUs.add((endNs - startNs) / 1000L); // microseconds
                    } else {
                        errorCount.incrementAndGet();
                    }
                } catch (TimeoutException te) {
                    timeoutCount.incrementAndGet();
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

        double replicationUs = 0;
        if (!sortedLatencies.isEmpty()) {
            Collections.sort(sortedLatencies);
            replicationUs = sortedLatencies.get((int) (sortedLatencies.size() * 0.50));
        }

        int totalErrors = timeoutCount.get() + errorCount.get();

        return BenchmarkMetrics.fromLatencies(getName(), nodeCount, clients,
                sortedLatencies, operations, successfulOps.get(), 0, timeoutCount.get(),
                totalErrors, durationSeconds, 0, 0, 0, 0, replicationUs, successfulOps.get());
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
