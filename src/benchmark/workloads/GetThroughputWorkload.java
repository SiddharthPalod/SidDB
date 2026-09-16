package benchmark.workloads;

import benchmark.BenchmarkMetrics;
import raft.RaftCluster;

public class GetThroughputWorkload extends GetLocalThroughputWorkload {
    @Override
    public String getName() {
        return "Get";
    }
}
