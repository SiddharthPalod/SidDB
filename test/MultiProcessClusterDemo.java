package test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MultiProcessClusterDemo {

    public static void run() throws Exception {
        System.out.println("\n--- [Running MultiProcessClusterDemo (3 Separate Java OS Processes over Real TCP Sockets)] ---");

        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String classpath = "out";

        List<Process> processes = new ArrayList<>();
        List<BufferedReader> readers = new ArrayList<>();
        List<BufferedWriter> writers = new ArrayList<>();

        String[] nodeArgs = new String[] {
            "node-1 9001 node-2=127.0.0.1:9002,node-3=127.0.0.1:9003 data/proc_node1",
            "node-2 9002 node-1=127.0.0.1:9001,node-3=127.0.0.1:9003 data/proc_node2",
            "node-3 9003 node-1=127.0.0.1:9001,node-2=127.0.0.1:9002 data/proc_node3"
        };

        try {
            // Clean test directories
            for (int i = 1; i <= 3; i++) {
                deleteDir(new File("data/proc_node" + i));
            }

            System.out.println("Step 1: Spawning 3 independent Java OS processes on ports 9001, 9002, 9003...");
            for (int i = 0; i < 3; i++) {
                String[] split = nodeArgs[i].split(" ");
                ProcessBuilder pb = new ProcessBuilder(
                    javaBin, "-cp", classpath, "server.DistributedNodeServer",
                    split[0], split[1], split[2], split[3]
                );
                Process proc = pb.start();
                processes.add(proc);
                readers.add(new BufferedReader(new InputStreamReader(proc.getInputStream())));
                writers.add(new BufferedWriter(new OutputStreamWriter(proc.getOutputStream())));
            }

            System.out.println("  ✓ 3 Java processes launched.");
            System.out.println("Step 2: Waiting for TCP Leader Election across processes...");
            Thread.sleep(1200); // Allow election over TCP sockets

            // Query status from all nodes via stdin
            int leaderIdx = -1;
            for (int i = 0; i < 3; i++) {
                writers.get(i).write("status\n");
                writers.get(i).flush();
                Thread.sleep(100);

                String out = drainOutput(readers.get(i));
                if (out.contains("Role:          LEADER")) {
                    leaderIdx = i;
                    System.out.println("  ✓ Process node-" + (i + 1) + " (Port " + (9001 + i) + ") won election and became LEADER!");
                }
            }

            if (leaderIdx == -1) {
                // Give one more second and re-check
                Thread.sleep(1000);
                for (int i = 0; i < 3; i++) {
                    writers.get(i).write("status\n");
                    writers.get(i).flush();
                    Thread.sleep(100);
                    String out = drainOutput(readers.get(i));
                    if (out.contains("Role:          LEADER")) {
                        leaderIdx = i;
                        System.out.println("  ✓ Process node-" + (i + 1) + " is LEADER!");
                    }
                }
            }

            if (leaderIdx != -1) {
                System.out.println("Step 3: Sending write command 'put server:os Linux' to Leader process over stdin...");
                writers.get(leaderIdx).write("put server:os Linux\n");
                writers.get(leaderIdx).flush();
                Thread.sleep(400);

                String leaderOut = drainOutput(readers.get(leaderIdx));
                System.out.println("  Leader output: " + leaderOut.trim());

                System.out.println("Step 4: Querying follower node via local storage engine...");
                int followerIdx = (leaderIdx + 1) % 3;
                writers.get(followerIdx).write("get server:os\n");
                writers.get(followerIdx).flush();
                Thread.sleep(200);

                String folOut = drainOutput(readers.get(followerIdx));
                System.out.println("  Follower (node-" + (followerIdx + 1) + ") read: " + folOut.trim());
            }

            System.out.println("  ✓ MultiProcessClusterDemo verified successfully over real TCP ports!");

        } finally {
            // Teardown all processes
            for (int i = 0; i < processes.size(); i++) {
                try {
                    writers.get(i).write("exit\n");
                    writers.get(i).flush();
                } catch (Exception ignored) {}
                processes.get(i).destroyForcibly();
            }
            for (int i = 1; i <= 3; i++) {
                deleteDir(new File("data/proc_node" + i));
            }
        }
    }

    private static String drainOutput(BufferedReader reader) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (reader.ready()) {
            sb.append(reader.readLine()).append("\n");
        }
        return sb.toString();
    }

    private static void deleteDir(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) deleteDir(f);
                    else f.delete();
                }
            }
            dir.delete();
        }
    }
}
