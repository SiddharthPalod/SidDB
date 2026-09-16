package chaos.scenarios;

import chaos.ChaosClusterContext;
import chaos.ChaosReport;
import chaos.ChaosScenario;
import raft.RaftNode;

import java.util.*;

/**
 * Simulates asymmetric network partition (Split-Brain scenario).
 * Partitions cluster into Minority vs. Majority.
 * Invariant: Minority CANNOT commit writes; Majority CAN commit;
 * On partition heal, all nodes converge to exact majority state.
 */
public class SplitBrainScenario implements ChaosScenario {

    @Override
    public String name() {
        return "Asymmetric Partition & Split-Brain";
    }

    @Override
    public String description() {
        return "Partitions cluster into Minority vs. Majority. Proves minority rejection and zero split-brain divergence.";
    }

    @Override
    public void execute(ChaosClusterContext ctx, ChaosReport.Builder report) throws Exception {
        long start = System.currentTimeMillis();

        RaftNode leader = ctx.waitForLeader(3000);
        if (leader == null) {
            report.addScenarioResult(name(), false, "No initial leader", 0);
            return;
        }

        List<RaftNode> allNodes = new ArrayList<>(ctx.getCluster().getNodes());
        int total = allNodes.size();

        // Separate 1 node (minority) from remaining nodes (majority)
        String minorityNode = leader.getNodeId();
        Set<String> minorityGroup = Collections.singleton(minorityNode);
        Set<String> majorityGroup = new HashSet<>();
        for (RaftNode n : allNodes) {
            if (!n.getNodeId().equals(minorityNode)) {
                majorityGroup.add(n.getNodeId());
            }
        }

        // Apply partition
        ctx.getCluster().partition(Arrays.asList(minorityGroup, majorityGroup));
        ctx.sleep(300); // Allow majority to detect timeout and elect leader if needed

        // 1. Attempt proposal to minority node (must fail because it has no quorum)
        boolean minorityCommitSucceeded = false;
        try {
            minorityCommitSucceeded = leader.propose("PUT", "split-key", "minority-dirty-val").get(600, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (Exception ignored) {}

        // 2. Propose to majority partition (should succeed)
        RaftNode majorityLeader = null;
        long electionDeadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < electionDeadline && majorityLeader == null) {
            for (String mId : majorityGroup) {
                RaftNode n = ctx.getNode(mId);
                if (n != null && n.getRole() == raft.RaftRole.LEADER) {
                    majorityLeader = n;
                    break;
                }
            }
            if (majorityLeader == null) ctx.sleep(50);
        }

        boolean majorityCommitSucceeded = false;
        if (majorityLeader != null) {
            try {
                majorityCommitSucceeded = majorityLeader.propose("PUT", "split-key", "majority-true-val").get(3000, java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (Exception ignored) {}
        }

        // 3. Heal partition
        ctx.getCluster().healAll();
        
        // Wait up to 2.5s for heartbeat log replication to converge minority node
        boolean consistent = ctx.waitForConsistency("split-key", "majority-true-val", 2500);

        Map<String, Object> nodeValues = ctx.collectAllNodeValues("split-key");
        boolean passed = !minorityCommitSucceeded && majorityCommitSucceeded && consistent;

        long duration = System.currentTimeMillis() - start;

        report.addScenarioResult(name(), passed,
                String.format("Minority partition write rejected (%s); Majority committed (%s); Healed state converged to 'majority-true-val'",
                        !minorityCommitSucceeded ? "CORRECT" : "VIOLATION", majorityCommitSucceeded ? "CORRECT" : "FAILED"),
                duration);

        report.addConsistencyCheck("split-key", nodeValues, consistent);
    }
}
