package benchmark.workloads;

import benchmark.BenchmarkMetrics;
import raft.RaftCluster;

public interface BenchmarkWorkload {
    String getName();
    BenchmarkMetrics run(RaftCluster cluster, int nodeCount, int clients);
}
