package benchmark;

import java.util.Collections;
import java.util.List;

/**
 * Production-grade metrics value object recording microsecond-precision latencies,
 * percentiles (P50, P95, P99, P99.9), concurrency levels, throughput, and error categorization.
 */
public class BenchmarkMetrics {
    private final String workloadName;
    private final int nodeCount;
    private final int clients;
    private final double throughput; // ops/sec
    private final double p50LatencyUs; // microseconds
    private final double p95LatencyUs; // microseconds
    private final double p99LatencyUs; // microseconds
    private final double p999LatencyUs; // microseconds
    private final double minLatencyUs;
    private final double maxLatencyUs;
    private final int totalOperations;
    private final int successfulOperations;
    private final int notFoundCount;
    private final int timeouts;
    private final int errors;
    private final double compactionOverheadMs;
    private final double recoveryTimeMs;
    private final double electionTimeMs;
    private final double serviceRecoveryTimeMs;
    private final double replicationLatencyUs;
    private final int survivingRecords;

    public BenchmarkMetrics(String workloadName, int nodeCount, int clients, double throughput,
                            double p50LatencyUs, double p95LatencyUs, double p99LatencyUs, double p999LatencyUs,
                            double minLatencyUs, double maxLatencyUs, int totalOperations,
                            int successfulOperations, int notFoundCount, int timeouts, int errors,
                            double compactionOverheadMs, double recoveryTimeMs, double electionTimeMs,
                            double serviceRecoveryTimeMs, double replicationLatencyUs, int survivingRecords) {
        this.workloadName = workloadName;
        this.nodeCount = nodeCount;
        this.clients = clients;
        this.throughput = throughput;
        this.p50LatencyUs = p50LatencyUs;
        this.p95LatencyUs = p95LatencyUs;
        this.p99LatencyUs = p99LatencyUs;
        this.p999LatencyUs = p999LatencyUs;
        this.minLatencyUs = minLatencyUs;
        this.maxLatencyUs = maxLatencyUs;
        this.totalOperations = totalOperations;
        this.successfulOperations = successfulOperations;
        this.notFoundCount = notFoundCount;
        this.timeouts = timeouts;
        this.errors = errors;
        this.compactionOverheadMs = compactionOverheadMs;
        this.recoveryTimeMs = recoveryTimeMs;
        this.electionTimeMs = electionTimeMs;
        this.serviceRecoveryTimeMs = serviceRecoveryTimeMs;
        this.replicationLatencyUs = replicationLatencyUs;
        this.survivingRecords = survivingRecords;
    }

    public static BenchmarkMetrics fromLatencies(String workloadName, int nodeCount, int clients,
                                                List<Long> latenciesUs, int totalOps, int successfulOps,
                                                int notFound, int timeouts, int errors,
                                                double durationSeconds, double compactionMs,
                                                double recoveryMs, double electionMs,
                                                double serviceRecoveryMs, double replicationUs, int surviving) {
        if (latenciesUs == null || latenciesUs.isEmpty()) {
            return new BenchmarkMetrics(workloadName, nodeCount, clients, 0, 0, 0, 0, 0, 0, 0,
                    totalOps, successfulOps, notFound, timeouts, errors, compactionMs, recoveryMs,
                    electionMs, serviceRecoveryMs, replicationUs, surviving);
        }

        Collections.sort(latenciesUs);
        int n = latenciesUs.size();
        double p50 = latenciesUs.get((int) (n * 0.50));
        double p95 = latenciesUs.get((int) Math.min(n - 1, (int) (n * 0.95)));
        double p99 = latenciesUs.get((int) Math.min(n - 1, (int) (n * 0.99)));
        double p999 = latenciesUs.get((int) Math.min(n - 1, (int) (n * 0.999)));
        double min = latenciesUs.get(0);
        double max = latenciesUs.get(n - 1);
        double throughput = durationSeconds > 0 ? n / durationSeconds : 0;

        return new BenchmarkMetrics(workloadName, nodeCount, clients, throughput,
                p50, p95, p99, p999, min, max, totalOps, successfulOps, notFound, timeouts, errors,
                compactionMs, recoveryMs, electionMs, serviceRecoveryMs, replicationUs, surviving);
    }

    public String getWorkloadName() { return workloadName; }
    public int getNodeCount() { return nodeCount; }
    public int getClients() { return clients; }
    public double getThroughput() { return throughput; }
    public double getP50LatencyUs() { return p50LatencyUs; }
    public double getP95LatencyUs() { return p95LatencyUs; }
    public double getP99LatencyUs() { return p99LatencyUs; }
    public double getP999LatencyUs() { return p999LatencyUs; }
    public double getMinLatencyUs() { return minLatencyUs; }
    public double getMaxLatencyUs() { return maxLatencyUs; }
    public int getTotalOperations() { return totalOperations; }
    public int getSuccessfulOperations() { return successfulOperations; }
    public int getNotFoundCount() { return notFoundCount; }
    public int getTimeouts() { return timeouts; }
    public int getErrors() { return errors; }
    public double getSuccessRate() {
        return totalOperations > 0 ? (successfulOperations * 100.0 / totalOperations) : 100.0;
    }
    public double getCompactionOverhead() { return compactionOverheadMs; }
    public double getRecoveryTime() { return recoveryTimeMs; }
    public double getElectionTimeMs() { return electionTimeMs; }
    public double getServiceRecoveryTimeMs() { return serviceRecoveryTimeMs; }
    public double getRaftReplicationLatency() { return replicationLatencyUs; }
    public int getSurvivingRecords() { return survivingRecords; }
}
