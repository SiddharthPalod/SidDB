package chaos;

/**
 * Strategy Pattern & SOLID (Open/Closed Principle):
 * Every chaos failure scenario implements this interface without modifying existing Raft or engine code.
 */
public interface ChaosScenario {
    String name();
    String description();
    void execute(ChaosClusterContext ctx, ChaosReport.Builder reportBuilder) throws Exception;
}
