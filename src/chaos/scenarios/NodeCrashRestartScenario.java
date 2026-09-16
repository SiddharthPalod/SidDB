package chaos.scenarios;

import chaos.ChaosClusterContext;
import chaos.ChaosReport;
import chaos.ChaosScenario;
import raft.RaftNode;

import java.util.Map;

/**
 * Simulates power loss / ungraceful crash of the active Leader,
 * measuring failover election time, followed by disk recovery and rejoining.
 */
public class NodeCrashRestartScenario implements ChaosScenario {

    @Override
    public String name() {
        return "Leader Crash & Disk Reboot";
    }

    @Override
    public String description() {
        return "Kills leader thread without graceful shutdown. Asserts failover election, then reboots from disk state.";
    }

    @Override
    public void execute(ChaosClusterContext ctx, ChaosReport.Builder report) throws Exception {
        long start = System.currentTimeMillis();

        RaftNode oldLeader = ctx.waitForLeader(3000);
        if (oldLeader == null) {
            report.addScenarioResult(name(), false, "Initial leader not elected", 0);
            return;
        }

        String crashedLeaderId = oldLeader.getNodeId();
        boolean wPre = ctx.propose("crash-pre-key", "val-pre-crash", 2000);

        // Abruptly terminate the leader
        ctx.getCluster().crashNode(crashedLeaderId);

        // Measure failover election window
        long failoverStart = System.currentTimeMillis();
        RaftNode newLeader = ctx.waitForLeader(4000);
        long failoverDuration = System.currentTimeMillis() - failoverStart;

        boolean newLeaderElected = (newLeader != null && !newLeader.getNodeId().equals(crashedLeaderId));

        // Propose write to new leader
        boolean wPost = false;
        if (newLeaderElected) {
            wPost = ctx.propose("crash-post-key", "val-post-crash", 3000);
        }

        // Reboot crashed node from persistent disk state (raft.meta, raft.log, WAL)
        ctx.getCluster().restartNode(crashedLeaderId);
        boolean consistentPre = ctx.waitForConsistency("crash-pre-key", "val-pre-crash", 2500);
        boolean consistentPost = ctx.waitForConsistency("crash-post-key", "val-post-crash", 2500);

        Map<String, Object> nodeValuesPre = ctx.collectAllNodeValues("crash-pre-key");
        Map<String, Object> nodeValuesPost = ctx.collectAllNodeValues("crash-post-key");

        boolean passed = wPre && newLeaderElected && wPost && consistentPre && consistentPost;

        long duration = System.currentTimeMillis() - start;

        report.addScenarioResult(name(), passed,
                String.format("Old leader '%s' crashed. New leader '%s' elected in %d ms. Rebooted node reconciled.",
                        crashedLeaderId, newLeader != null ? newLeader.getNodeId() : "NONE", failoverDuration),
                duration);

        report.addConsistencyCheck("crash-pre-key", nodeValuesPre, consistentPre);
        report.addConsistencyCheck("crash-post-key", nodeValuesPost, consistentPost);
        report.addBenchmark("Leader Failover & Election Duration", failoverDuration, "ms");
    }
}
