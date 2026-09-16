package chaos;

import chaos.scenarios.*;
import raft.RaftCluster;
import raft.RaftNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Context & Runner (Strategy Pattern):
 * Manages the registration and execution of Chaos Scenarios, benchmarking invariants,
 * and assembling the resulting ChaosReport.
 */
public class ChaosEngine {

    private final List<ChaosScenario> scenarios = new ArrayList<>();

    public ChaosEngine register(ChaosScenario scenario) {
        scenarios.add(scenario);
        return this;
    }

    public static ChaosEngine createStandardSuite() {
        ChaosEngine engine = new ChaosEngine();
        engine.register(new PacketLossScenario())
              .register(new LatencyJitterScenario())
              .register(new SlowFollowerScenario())
              .register(new NodeCrashRestartScenario())
              .register(new SplitBrainScenario())
              .register(new MessageReorderScenario())
              .register(new CorrelatedCrashScenario())
              .register(new FlappingNodeScenario())
              .register(new DiskFaultScenario());
        return engine;
    }

    public ChaosReport run(ChaosClusterContext ctx) {
        ChaosReport.Builder reportBuilder = new ChaosReport.Builder();

        // Warm up and benchmark baseline commit latency
        benchmarkBaseline(ctx, reportBuilder);

        for (ChaosScenario scenario : scenarios) {
            System.out.println("[CHAOS] Executing: " + scenario.name() + "...");
            try {
                scenario.execute(ctx, reportBuilder);
            } catch (Exception e) {
                System.err.println("[CHAOS FAIL] " + scenario.name() + " threw exception: " + e.getMessage());
                e.printStackTrace();
                reportBuilder.addScenarioResult(scenario.name(), false, "Exception: " + e.getMessage(), 0);
            }
            // Reset any remaining chaos state between scenarios
            ctx.getTransport().resetChaos();
            ctx.getCluster().healAll();
            ctx.getCluster().ensureAllNodesRunning();
            ctx.sleep(150);
            ctx.waitForLeader(3000); // Ensure settled leader before proceeding to next scenario
        }

        return reportBuilder.build();
    }

    private void benchmarkBaseline(ChaosClusterContext ctx, ChaosReport.Builder reportBuilder) {
        RaftNode leader = ctx.waitForLeader(3000);
        if (leader != null) {
            long t0 = System.currentTimeMillis();
            for (int i = 1; i <= 5; i++) {
                ctx.propose("baseline-k" + i, "baseline-v" + i, 2000);
            }
            long totalMs = System.currentTimeMillis() - t0;
            double avgLatency = (double) totalMs / 5.0;
            reportBuilder.addBenchmark("Baseline Commit Latency (3-node in-memory quorum)", avgLatency, "ms / write");
        }
    }
}
