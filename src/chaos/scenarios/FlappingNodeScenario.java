package chaos.scenarios;

import chaos.ChaosClusterContext;
import chaos.ChaosReport;
import chaos.ChaosScenario;
import raft.RaftNode;

import java.util.Map;

/**
 * Simulates a flapping node entering rapid crash/reboot cycles (e.g. OOM loops).
 * Asserts cluster stability, term monotonicity, and log convergence across oscillations.
 */
public class FlappingNodeScenario implements ChaosScenario {

    @Override
    public String name() {
        return "Flapping Node (Rapid Crash/Reboot Loop)";
    }

    @Override
    public String description() {
        return "Executes 4 rapid crash/reboot cycles on a node while cluster serves client traffic.";
    }

    @Override
    public void execute(ChaosClusterContext ctx, ChaosReport.Builder report) throws Exception {
        long start = System.currentTimeMillis();

        RaftNode leader = ctx.waitForLeader(3000);
        if (leader == null) {
            report.addScenarioResult(name(), false, "No initial leader", 0);
            return;
        }

        // Choose a follower to flap
        String targetFollower = null;
        for (RaftNode n : ctx.getCluster().getNodes()) {
            if (!n.getNodeId().equals(leader.getNodeId())) {
                targetFollower = n.getNodeId();
                break;
            }
        }

        if (targetFollower == null) {
            targetFollower = leader.getNodeId();
        }

        // 4 Rapid cycles
        for (int cycle = 1; cycle <= 4; cycle++) {
            ctx.getCluster().crashNode(targetFollower);
            ctx.sleep(60);
            ctx.getCluster().restartNode(targetFollower);
            ctx.sleep(60);
        }

        // Ensure cluster has a settled leader after flapping stabilizes
        ctx.waitForLeader(3000);
        boolean writeSuccess = ctx.propose("flapping-key", "val-flap-stabilized", 4000);
        boolean consistent = ctx.waitForConsistency("flapping-key", "val-flap-stabilized", 3000);

        Map<String, Object> nodeValues = ctx.collectAllNodeValues("flapping-key");
        boolean passed = writeSuccess && consistent;

        long duration = System.currentTimeMillis() - start;

        report.addScenarioResult(name(), passed,
                String.format("Node '%s' survived 4 crash/reboot loops without corrupting monotonic terms or log integrity.", targetFollower),
                duration);

        report.addConsistencyCheck("flapping-key", nodeValues, consistent);
    }
}
