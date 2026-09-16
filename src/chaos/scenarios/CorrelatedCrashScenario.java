package chaos.scenarios;

import chaos.ChaosClusterContext;
import chaos.ChaosReport;
import chaos.ChaosScenario;
import raft.RaftNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Simulates correlated concurrent failures (e.g. rack power loss or OS patch reboot)
 * taking down a minority of nodes simultaneously.
 * Asserts remaining majority maintains uninterrupted quorum and progress.
 */
public class CorrelatedCrashScenario implements ChaosScenario {

    @Override
    public String name() {
        return "Correlated Multi-Node Crash";
    }

    @Override
    public String description() {
        return "Simultaneously crashes minority nodes. Asserts surviving majority maintains commit quorum.";
    }

    @Override
    public void execute(ChaosClusterContext ctx, ChaosReport.Builder report) throws Exception {
        long start = System.currentTimeMillis();

        RaftNode leader = ctx.waitForLeader(3000);
        if (leader == null) {
            report.addScenarioResult(name(), false, "No initial leader", 0);
            return;
        }

        List<RaftNode> nodes = new ArrayList<>(ctx.getCluster().getNodes());
        int total = nodes.size();
        int maxMinority = (total - 1) / 2; // e.g. 1 in 3-node, 2 in 5-node
        if (maxMinority < 1) maxMinority = 1;

        List<String> crashedNodes = new ArrayList<>();
        // Select followers to crash first
        for (RaftNode n : nodes) {
            if (!n.getNodeId().equals(leader.getNodeId()) && crashedNodes.size() < maxMinority) {
                crashedNodes.add(n.getNodeId());
            }
        }

        // Correlated crash
        for (String cId : crashedNodes) {
            ctx.getCluster().crashNode(cId);
        }

        ctx.sleep(200);

        // Propose to surviving quorum
        boolean commitWithSurvivingQuorum = ctx.propose("correlated-key", "val-surviving-quorum", 3000);

        // Restart crashed nodes
        for (String cId : crashedNodes) {
            ctx.getCluster().restartNode(cId);
        }
        boolean consistent = ctx.waitForConsistency("correlated-key", "val-surviving-quorum", 2500);

        Map<String, Object> nodeValues = ctx.collectAllNodeValues("correlated-key");
        boolean passed = commitWithSurvivingQuorum && consistent;

        long duration = System.currentTimeMillis() - start;

        report.addScenarioResult(name(), passed,
                String.format("Crashed %d nodes simultaneously (%s). Surviving quorum committed write; rebooted nodes caught up.",
                        crashedNodes.size(), String.join(", ", crashedNodes)),
                duration);

        report.addConsistencyCheck("correlated-key", nodeValues, consistent);
    }
}
