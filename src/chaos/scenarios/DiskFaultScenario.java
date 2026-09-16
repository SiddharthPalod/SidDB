package chaos.scenarios;

import chaos.ChaosClusterContext;
import chaos.ChaosReport;
import chaos.ChaosScenario;
import raft.RaftNode;

import java.util.Map;

/**
 * Simulates storage/disk degradation and ungraceful engine halts mid-operation.
 * Asserts that remaining quorum remains operational and re-synchronizes the faulted node upon storage recovery.
 */
public class DiskFaultScenario implements ChaosScenario {

    @Override
    public String name() {
        return "Storage / Disk Fault Simulation";
    }

    @Override
    public String description() {
        return "Simulates ungraceful engine halt simulating I/O device error. Asserts quorum isolation and recovery.";
    }

    @Override
    public void execute(ChaosClusterContext ctx, ChaosReport.Builder report) throws Exception {
        long start = System.currentTimeMillis();

        RaftNode leader = ctx.waitForLeader(3000);
        if (leader == null) {
            report.addScenarioResult(name(), false, "No initial leader", 0);
            return;
        }

        // Identify a follower for disk fault simulation
        String faultyNodeId = null;
        for (RaftNode n : ctx.getCluster().getNodes()) {
            if (!n.getNodeId().equals(leader.getNodeId())) {
                faultyNodeId = n.getNodeId();
                break;
            }
        }

        if (faultyNodeId == null) faultyNodeId = leader.getNodeId();

        // Simulate abrupt I/O failure on faulty node
        ctx.getCluster().crashNode(faultyNodeId);

        // Client writes continue to healthy quorum
        boolean writeCommitted = ctx.propose("disk-fault-key", "val-quorum-durable", 3000);

        // Restore storage engine
        ctx.getCluster().restartNode(faultyNodeId);
        boolean consistent = ctx.waitForConsistency("disk-fault-key", "val-quorum-durable", 2500);

        Map<String, Object> nodeValues = ctx.collectAllNodeValues("disk-fault-key");
        boolean passed = writeCommitted && consistent;

        long duration = System.currentTimeMillis() - start;

        report.addScenarioResult(name(), passed,
                String.format("Node '%s' handled simulated I/O fault; healthy quorum preserved durability and caught up node on restart.", faultyNodeId),
                duration);

        report.addConsistencyCheck("disk-fault-key", nodeValues, consistent);
    }
}
