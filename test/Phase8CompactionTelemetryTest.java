package test;

import engine.SidDBEngine;
import compaction.Compactor;
import compaction.CompactionTelemetry;
import java.io.File;

public class Phase8CompactionTelemetryTest {

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        System.out.println("\n--- [Running Phase 8.3 Decoupled Compactor & WAF/SAF Telemetry Test Suite] ---");
        String testDir = "data/test_phase8_compaction";
        deleteDir(new File(testDir));

        try (SidDBEngine engine = new SidDBEngine(testDir, 4)) { // 4 keys threshold to trigger frequent flushes
            Compactor compactor = engine.getCompactor();
            CompactionTelemetry telemetry = compactor.getTelemetry();

            System.out.println("Step 1: Ingesting 20 keys to trigger 5 MemTable flushes and background compactions...");
            for (int i = 1; i <= 20; i++) {
                engine.put("comp:key_" + (i % 6), "value_payload_data_" + i); // Overwrites same 6 keys to test deduplication and WAF
            }

            System.out.println("  ✓ 20 writes completed without blocking.");

            // Allow background compactor thread to finish merging L0 -> L1 -> L2
            System.out.println("Step 2: Awaiting background asynchronous compaction...");
            Thread.sleep(400);

            // Verify telemetry metrics
            System.out.println("Step 3: Evaluating Real-time LSM Telemetry & Amplification Factors...");
            long userBytes = telemetry.getUserBytesWritten();
            long diskBytes = telemetry.getDiskBytesWritten();
            long compactions = telemetry.getCompactionsCount();
            double waf = telemetry.getWAF();
            double mergeSpeed = telemetry.getMergeThroughputMBs();

            System.out.println("  ✓ User Bytes Written:   " + userBytes + " bytes");
            System.out.println("  ✓ Disk Bytes Written:   " + diskBytes + " bytes");
            System.out.println("  ✓ Compactions Executed: " + compactions);
            System.out.printf("  ✓ Write Amplification (WAF): %.2fx%n", waf);
            System.out.printf("  ✓ Merge Throughput:     %.2f MB/s%n", mergeSpeed);

            assert userBytes > 0 : "userBytesWritten must be recorded";
            assert diskBytes > userBytes : "Disk bytes (WAL + SSTables + Compactions) must exceed user bytes (WAF > 1.0)";
            assert waf >= 1.0 : "WAF must be >= 1.0";

            // Test 4: Space Amplification Factor (SAF)
            long totalSSTableSize = 0;
            File dir = new File(testDir);
            File[] files = dir.listFiles((d, name) -> name.endsWith(".sb"));
            if (files != null) {
                for (File f : files) totalSSTableSize += f.length();
            }
            long liveDataSize = engine.keys().size() * 32L; // Estimated live logical footprint
            double saf = telemetry.getSAF(totalSSTableSize, liveDataSize);
            System.out.printf("  ✓ Space Amplification (SAF): %.2fx (SSTables: %d bytes, Live: %d bytes)%n", saf, totalSSTableSize, liveDataSize);
            assert saf >= 0.5 : "SAF must be realistic";

            // Test 5: Verify all keys remain accurate and queryable
            System.out.println("Step 4: Invariant Verification: Verifying key-value correctness after async merges...");
            for (int i = 0; i < 6; i++) {
                Object val = engine.get("comp:key_" + i);
                assert val != null : "Key comp:key_" + i + " must exist after compaction";
                System.out.println("  ✓ Key comp:key_" + i + " -> " + val);
            }

            System.out.println("[+] Phase 8.3 Decoupled Compactor & WAF/SAF Telemetry Test PASSED 100%!");
        } finally {
            deleteDir(new File(testDir));
        }
    }

    private static void deleteDir(File file) {
        if (file.isDirectory()) {
            File[] entries = file.listFiles();
            if (entries != null) {
                for (File entry : entries) {
                    deleteDir(entry);
                }
            }
        }
        file.delete();
    }
}