package test;

import java.util.ArrayList;
import java.util.List;

public class TestRunner {

    interface TestCase {
        void execute() throws Exception;
    }

    static class TestEntry {
        String name;
        TestCase testCase;

        TestEntry(String name, TestCase testCase) {
            this.name = name;
            this.testCase = testCase;
        }
    }

    public static void main(String[] args) {
        List<TestEntry> testSuites = new ArrayList<>();
        testSuites.add(new TestEntry("Phase 1: Bitcask DiskStore Test", Phase1DiskStoreTest::run));
        testSuites.add(new TestEntry("Phase 2: WAL & Crash Recovery Test", Phase2WALTest::run));

        System.out.println("=================================================");
        System.out.println("          SidDB Regression Test Suite            ");
        System.out.println("=================================================");

        int passed = 0;
        int failed = 0;

        for (TestEntry entry : testSuites) {
            try {
                entry.testCase.execute();
                passed++;
            } catch (Throwable t) {
                System.err.println("  ✗ FAILED: " + entry.name);
                t.printStackTrace();
                failed++;
            }
        }

        System.out.println("\n=================================================");
        System.out.println(" TEST RESULTS: " + passed + " Passed, " + failed + " Failed, Total: " + testSuites.size());
        System.out.println("=================================================");

        if (failed > 0) {
            System.exit(1);
        }
    }
}
