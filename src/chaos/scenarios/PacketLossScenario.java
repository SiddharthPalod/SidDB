package chaos.scenarios;

import chaos.ChaosClusterContext;
import chaos.ChaosReport;
import chaos.ChaosScenario;
import raft.RaftNode;

import java.util.Map;

/**
 * Simulates 25% random packet loss across all RPC communication.
 * Verifies Raft's heartbeat and retry mechanisms successfully maintain quorum
 * and log replication without losing writes or corrupting invariants.
 */
public class PacketLossScenario implements ChaosScenario {

    @Override
    public String name() {
        return "25% Random Packet Loss";
    }

    @Override
    public String description() {
        return "Injects 25% packet drop rate across RPCs. Asserts Raft retries achieve quorum and consistency.";
    }

    @Override
    public void execute(ChaosClusterContext ctx, ChaosReport.Builder report) throws Exception {
        long start = System.currentTimeMillis();

        ctx.getTransport().setPacketLossRate(0.25);

        RaftNode leader = ctx.waitForLeader(3000);
        boolean leaderAlive = leader != null;

        long proposeStart = System.currentTimeMillis();
        boolean write1 = ctx.propose("pktloss-key1", "val-reliable-1", 4000);
        boolean write2 = ctx.propose("pktloss-key2", "val-reliable-2", 4000);
        long proposeDuration = System.currentTimeMillis() - proposeStart;

        ctx.getTransport().resetChaos();
        boolean consistent1 = ctx.waitForConsistency("pktloss-key1", "val-reliable-1", 2500);
        boolean consistent2 = ctx.waitForConsistency("pktloss-key2", "val-reliable-2", 2500);

        Map<String, Object> nodeValues1 = ctx.collectAllNodeValues("pktloss-key1");
        Map<String, Object> nodeValues2 = ctx.collectAllNodeValues("pktloss-key2");

        boolean passed = leaderAlive && write1 && write2 && consistent1 && consistent2;

        long duration = System.currentTimeMillis() - start;

        report.addScenarioResult(name(), passed,
                String.format("Leader %s elected; writes committed in %d ms under 25%% drop rate (dropped: %d pkts)",
                        leader != null ? leader.getNodeId() : "NONE", proposeDuration, ctx.getTransport().getMessagesDropped()),
                duration);

        report.addConsistencyCheck("pktloss-key1", nodeValues1, consistent1);
        report.addConsistencyCheck("pktloss-key2", nodeValues2, consistent2);
        report.addBenchmark("Packet Loss Commit Latency (2 writes)", proposeDuration, "ms");
    }
}
