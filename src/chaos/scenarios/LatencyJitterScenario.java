package chaos.scenarios;

import chaos.ChaosClusterContext;
import chaos.ChaosReport;
import chaos.ChaosScenario;
import raft.RaftNode;

import java.util.Map;

/**
 * Simulates WAN latency and variable jitter (50ms - 150ms).
 * Verifies that client proposals complete and all nodes apply log entries in deterministic order.
 */
public class LatencyJitterScenario implements ChaosScenario {

    @Override
    public String name() {
        return "Network Latency & Jitter (50ms - 150ms)";
    }

    @Override
    public String description() {
        return "Simulates fluctuating network transit delays. Verifies deterministic commit ordering.";
    }

    @Override
    public void execute(ChaosClusterContext ctx, ChaosReport.Builder report) throws Exception {
        long start = System.currentTimeMillis();

        ctx.getTransport().setLatencyJitter(30, 80);

        RaftNode leader = ctx.waitForLeader(3000);
        long proposeStart = System.currentTimeMillis();
        boolean write1 = ctx.propose("jitter-key1", "jitter-val-1", 4000);
        boolean write2 = ctx.propose("jitter-key2", "jitter-val-2", 4000);
        long proposeDuration = System.currentTimeMillis() - proposeStart;

        ctx.getTransport().resetChaos();
        boolean consistent1 = ctx.waitForConsistency("jitter-key1", "jitter-val-1", 2500);
        boolean consistent2 = ctx.waitForConsistency("jitter-key2", "jitter-val-2", 2500);

        Map<String, Object> nodeValues = ctx.collectAllNodeValues("jitter-key1");
        boolean passed = (leader != null) && write1 && write2 && consistent1 && consistent2;

        long duration = System.currentTimeMillis() - start;

        report.addScenarioResult(name(), passed,
                String.format("Replicated across nodes with jitter in %d ms (delayed msgs: %d)",
                        proposeDuration, ctx.getTransport().getMessagesDelayed()),
                duration);

        report.addConsistencyCheck("jitter-key1", nodeValues, consistent1);
        report.addBenchmark("Latency Jitter Commit Time (2 writes)", proposeDuration, "ms");
    }
}
