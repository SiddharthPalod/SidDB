package chaos.scenarios;

import chaos.ChaosClusterContext;
import chaos.ChaosReport;
import chaos.ChaosScenario;
import raft.RaftNode;

import java.util.Map;

/**
 * Simulates asynchronous out-of-order message delivery and packet shuffling.
 * Raft's strict term and prevLogIndex checks ensure stale or out-of-order
 * messages do not corrupt the replicated state machine.
 */
public class MessageReorderScenario implements ChaosScenario {

    @Override
    public String name() {
        return "Out-of-Order Message Delivery";
    }

    @Override
    public String description() {
        return "Shuffles message arrival order. Asserts term and log index checks prevent corruption.";
    }

    @Override
    public void execute(ChaosClusterContext ctx, ChaosReport.Builder report) throws Exception {
        long start = System.currentTimeMillis();

        ctx.getTransport().setReorderMessages(true);

        RaftNode leader = ctx.waitForLeader(3000);
        boolean w1 = ctx.propose("reorder-k1", "val-seq-1", 3000);
        boolean w2 = ctx.propose("reorder-k2", "val-seq-2", 3000);
        boolean w3 = ctx.propose("reorder-k3", "val-seq-3", 3000);

        ctx.getTransport().resetChaos();
        boolean consistent1 = ctx.waitForConsistency("reorder-k1", "val-seq-1", 2500);
        boolean consistent2 = ctx.waitForConsistency("reorder-k2", "val-seq-2", 2500);
        boolean consistent3 = ctx.waitForConsistency("reorder-k3", "val-seq-3", 2500);

        Map<String, Object> nodeValues = ctx.collectAllNodeValues("reorder-k3");
        boolean passed = (leader != null) && w1 && w2 && w3 && consistent1 && consistent2 && consistent3;
        long duration = System.currentTimeMillis() - start;

        report.addScenarioResult(name(), passed,
                "Out-of-order RPCs safely filtered by Raft prevLogIndex/term invariants across all nodes",
                duration);

        report.addConsistencyCheck("reorder-k3", nodeValues, consistent3);
    }
}
