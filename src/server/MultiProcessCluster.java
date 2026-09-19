package server;

import network.DistributedClient;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Manages an arbitrary N-node cluster composed of independent OS Java processes
 * communicating strictly over real OS TCP sockets.
 */
public class MultiProcessCluster implements AutoCloseable {

    private final String baseDir;
    private final int nodeCount;
    private final int basePort;
    private final List<Process> processes = new ArrayList<>();
    private final List<String> nodeIds = new ArrayList<>();
    private final List<Integer> ports = new ArrayList<>();
    private final List<DistributedClient> clients = new ArrayList<>();
    private final String javaBin;
    private final String classpath = "out";

    public MultiProcessCluster(String baseDir, int nodeCount, int basePort) {
        this.baseDir = baseDir;
        this.nodeCount = nodeCount;
        this.basePort = basePort;
        this.javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";

        for (int i = 1; i <= nodeCount; i++) {
            nodeIds.add("node-" + i);
            ports.add(basePort + i - 1);
            clients.add(new DistributedClient("127.0.0.1", basePort + i - 1, "node-" + i));
        }
    }

    public void start() throws Exception {
        // Build peer configs
        for (int i = 0; i < nodeCount; i++) {
            StringBuilder peersConfig = new StringBuilder();
            for (int j = 0; j < nodeCount; j++) {
                if (i != j) {
                    if (peersConfig.length() > 0) peersConfig.append(",");
                    peersConfig.append(nodeIds.get(j)).append("=127.0.0.1:").append(ports.get(j));
                }
            }
            if (peersConfig.length() == 0) {
                peersConfig.append("none");
            }

            File nodeDir = new File(baseDir, nodeIds.get(i));
            if (!nodeDir.exists()) nodeDir.mkdirs();

            ProcessBuilder pb = new ProcessBuilder(
                    javaBin, "-cp", classpath, "server.DistributedNodeServer",
                    nodeIds.get(i), String.valueOf(ports.get(i)), peersConfig.toString(), nodeDir.getAbsolutePath()
            );
            pb.redirectErrorStream(true);
            Process proc = pb.start();
            processes.add(proc);

            // Drain stdout asynchronously
            final InputStream is = proc.getInputStream();
            Executors.newSingleThreadExecutor().submit(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                    while (reader.readLine() != null) {}
                } catch (Exception ignored) {}
            });
        }
    }

    public DistributedClient getClient(int index) {
        return clients.get(index);
    }

    public DistributedClient getClient(String nodeId) {
        int idx = nodeIds.indexOf(nodeId);
        return idx >= 0 ? clients.get(idx) : null;
    }

    public DistributedClient waitForLeader(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < nodeCount; i++) {
                try {
                    Map<String, Object> status = clients.get(i).getStatus();
                    if (status != null && "LEADER".equals(status.get("role"))) {
                        return clients.get(i);
                    }
                } catch (Exception ignored) {}
            }
            try { Thread.sleep(50); } catch (Exception ignored) {}
        }
        return null;
    }

    public int getLeaderIndex(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (int i = 0; i < nodeCount; i++) {
                try {
                    Map<String, Object> status = clients.get(i).getStatus();
                    if (status != null && "LEADER".equals(status.get("role"))) {
                        return i;
                    }
                } catch (Exception ignored) {}
            }
            try { Thread.sleep(50); } catch (Exception ignored) {}
        }
        return -1;
    }

    public void killProcess(int index) {
        if (index >= 0 && index < processes.size()) {
            Process p = processes.get(index);
            if (p != null) {
                p.destroyForcibly();
            }
        }
    }

    public void restartProcess(int index) throws Exception {
        if (index < 0 || index >= nodeCount) return;

        StringBuilder peersConfig = new StringBuilder();
        for (int j = 0; j < nodeCount; j++) {
            if (index != j) {
                if (peersConfig.length() > 0) peersConfig.append(",");
                peersConfig.append(nodeIds.get(j)).append("=127.0.0.1:").append(ports.get(j));
            }
        }
        if (peersConfig.length() == 0) peersConfig.append("none");

        File nodeDir = new File(baseDir, nodeIds.get(index));
        ProcessBuilder pb = new ProcessBuilder(
                javaBin, "-cp", classpath, "server.DistributedNodeServer",
                nodeIds.get(index), String.valueOf(ports.get(index)), peersConfig.toString(), nodeDir.getAbsolutePath()
        );
        pb.redirectErrorStream(true);
        Process proc = pb.start();
        processes.set(index, proc);

        final InputStream is = proc.getInputStream();
        Executors.newSingleThreadExecutor().submit(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
                while (reader.readLine() != null) {}
            } catch (Exception ignored) {}
        });
    }

    public List<String> getNodeIds() { return Collections.unmodifiableList(nodeIds); }
    public int getNodeCount() { return nodeCount; }

    @Override
    public void close() {
        for (Process p : processes) {
            if (p != null && p.isAlive()) {
                p.destroyForcibly();
            }
        }
    }
}