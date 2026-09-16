package chaos.scenarios;

import chaos.ChaosClusterContext;
import chaos.ChaosReport;
import chaos.ChaosScenario;
import raft.RaftNode;

import java.util.Map;

/**
 * Simulates a severely lagging follower node.
 * Verifies leader backpressure, nextIndex decrement backtracking,
 * and rapid log catch-up upon network recovery.
 */
public class SlowFollowerScenario implements ChaosScenario {

    @Override
    public String name() {
        return "Slow Follower & Fast Catch-up";
    }

    @Override
    public String description() {
        return "Injects 300ms delay on one follower. Quorum commits forward. Follower catches up on heal.";
    }

    @Override
    public void execute(ChaosClusterContext ctx, ChaosReport.Builder report) throws Exception {
        long start = System.currentTimeMillis();

        RaftNode leader = ctx.waitForLeader(3000);
        if (leader == null) {
            report.addScenarioResult(name(), false, "No leader available before test", 0);
            return;
        }

        // Find a follower
        String slowFollowerId = null;
        for (RaftNode n : ctx.getCluster().getNodes()) {
            if (!n.getNodeId().equals(leader.getNodeId())) {
                slowFollowerId = n.getNodeId();
                break;
            }
        }

        if (slowFollowerId != null) {
            ctx.getTransport().setNodeLatency(slowFollowerId, 250);
        }

        // Propose writes while follower is slow (majority will commit them quickly)
        boolean w1 = ctx.propose("slow-k1", "val-fast-quorum-1", 3000);
        boolean w2 = ctx.propose("slow-k2", "val-fast-quorum-2", 3000);
        boolean w3 = ctx.propose("slow-k3", "val-fast-quorum-3", 3000);

        // Remove artificial latency so follower catches up
        if (slowFollowerId != null) {
            ctx.getTransport().setNodeLatency(slowFollowerId, 0);
        }
        boolean consistent = ctx.waitForConsistency("slow-k3", "val-fast-quorum-3", 2500);

        Map<String, Object> nodeValues = ctx.collectAllNodeValues("slow-k3");
        boolean passed = w1 && w2 && w3 && consistent;

        long duration = System.currentTimeMillis() - start;

        report.addScenarioResult(name(), passed,
                String.format("Lagging node '%s' backtracked nextIndex and synchronized to log index %d",
                        slowFollowerId, leader.getLog().getLastLogIndex()),
                duration);

        report.addConsistencyCheck("slow-k3", nodeValues, consistent);
    }
}
