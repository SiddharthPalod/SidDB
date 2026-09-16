package server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import engine.SidDBEngine;
import level.Level;
import raft.RaftCluster;
import raft.RaftLogEntry;
import raft.RaftNode;
import raft.RaftRole;
import sstable.BlockCache;
import sstable.SSTableReader;
import chaos.*;
import chaos.scenarios.*;
import network.ChaoticTransport;
import network.SimulatedNetwork;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SidDBServer {

    private static int port = 8080;
    private static SidDBEngine engine;
    private static RaftCluster cluster;
    private static String currentDbDir = "data";
    private static int currentThreshold = 4;

    public static void main(String[] args) throws IOException {
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException ignored) {}
        }

        // Initialize default engine
        engine = new SidDBEngine(currentDbDir, currentThreshold);

        // Initialize 3-node Raft cluster
        List<String> clusterNodes = Arrays.asList("node-1", "node-2", "node-3");
        cluster = new RaftCluster("data/cluster", clusterNodes, currentThreshold);
        cluster.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (engine != null) engine.close();
                if (cluster != null) cluster.close();
            } catch (Exception ignored) {}
        }));

        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (java.net.BindException e) {
            System.err.println("\n[ERROR] Port " + port + " is already in use by another process.");
            System.err.println("To fix this, you can either:");
            System.err.println("  1. Kill the process running on port " + port + " (PowerShell: Stop-Process -Id (Get-NetTCPConnection -LocalPort " + port + ").OwningProcess -Force)");
            System.err.println("  2. Or run SidDB on another port, e.g.: java -cp out server.SidDBServer " + (port + 1) + "\n");
            return;
        }

        // Static Files & Single-Node Endpoints
        server.createContext("/", new StaticFileHandler());
        server.createContext("/api/state", new StateHandler());
        server.createContext("/api/put", new PutHandler());
        server.createContext("/api/get", new GetHandler());
        server.createContext("/api/delete", new DeleteHandler());
        server.createContext("/api/flush", new FlushHandler());
        server.createContext("/api/compact", new CompactHandler());
        server.createContext("/api/db/switch", new SwitchDbHandler());
        server.createContext("/api/reset", new ResetHandler());

        // Raft Consensus Cluster Endpoints
        server.createContext("/api/cluster/status", new ClusterStatusHandler());
        server.createContext("/api/cluster/propose", new ClusterProposeHandler());
        server.createContext("/api/cluster/partition", new ClusterPartitionHandler());
        server.createContext("/api/cluster/heal", new ClusterHealHandler());
        server.createContext("/api/cluster/reset", new ClusterResetHandler());

        // Chaos Testing & Failure Injection Endpoints (Isolated Mode)
        server.createContext("/api/chaos/run", new ChaosRunHandler());
        server.createContext("/api/chaos/report", new ChaosReportHandler());
        server.createContext("/api/chaos/status", new ChaosStatusHandler());
        server.createContext("/api/chaos/reset", new ChaosResetHandler());

        server.setExecutor(null); // Default executor
        System.out.println("==========================================================");
        System.out.println(" [*] SidDB Live Server running at: http://localhost:" + port);
        System.out.println(" [*] Single-Node DB Directory: " + new File(currentDbDir).getAbsolutePath());
        System.out.println(" [*] Raft 3-Node Cluster Active: node-1, node-2, node-3");
        System.out.println("==========================================================");
        server.start();
    }

    // CORS & Response Helper
    private static void sendResponse(HttpExchange exchange, int statusCode, String response, String contentType) throws IOException {
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, Authorization");
        exchange.getResponseHeaders().set("Content-Type", contentType + "; charset=UTF-8");

        if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }

        byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String readBody(HttpExchange exchange) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    private static String extractJsonField(String json, String field) {
        if (json == null) return null;
        String pattern = "\"" + field + "\"\\s*:\\s*\"?([^\",}]+)\"?";
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
        java.util.regex.Matcher m = p.matcher(json);
        if (m.find()) {
            return m.group(1).trim();
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static String toJson(Object obj) {
        if (obj == null) return "null";
        if (obj instanceof String) {
            return "\"" + escapeJson((String) obj) + "\"";
        }
        if (obj instanceof Number || obj instanceof Boolean) {
            return obj.toString();
        }
        if (obj instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) obj;
            StringBuilder sb = new StringBuilder("{");
            int idx = 0;
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (idx++ > 0) sb.append(",");
                sb.append("\"").append(escapeJson(String.valueOf(entry.getKey()))).append("\":");
                sb.append(toJson(entry.getValue()));
            }
            sb.append("}");
            return sb.toString();
        }
        if (obj instanceof Collection) {
            Collection<?> col = (Collection<?>) obj;
            StringBuilder sb = new StringBuilder("[");
            int idx = 0;
            for (Object item : col) {
                if (idx++ > 0) sb.append(",");
                sb.append(toJson(item));
            }
            sb.append("]");
            return sb.toString();
        }
        return "\"" + escapeJson(obj.toString()) + "\"";
    }

    private static String escapeJson(String raw) {
        if (raw == null) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < ' ') {
                        String hex = String.format("\\u%04x", (int) c);
                        sb.append(hex);
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    // Serve visualizer.html
    static class StaticFileHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            File htmlFile = new File("visualizer.html");
            if (!htmlFile.exists()) {
                sendResponse(exchange, 404, "<h1>404 visualizer.html Not Found</h1>", "text/html");
                return;
            }
            byte[] bytes = java.nio.file.Files.readAllBytes(htmlFile.toPath());
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }

    // Single-Node State Handler
    static class StateHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }

            StringBuilder json = new StringBuilder("{");
            json.append("\"dbDir\":\"").append(currentDbDir.replace("\\", "/")).append("\",");
            json.append("\"threshold\":").append(currentThreshold).append(",");
            json.append("\"sequenceNumber\":").append(engine.getSequenceNumber()).append(",");

            // MemTable
            json.append("\"memTable\":[");
            var entries = engine.getActiveMemTable().getEntries();
            int mIdx = 0;
            for (Map.Entry<String, Object> e : entries.entrySet()) {
                if (mIdx++ > 0) json.append(",");
                json.append("{\"key\":\"").append(e.getKey()).append("\",");
                json.append("\"value\":").append(e.getValue() == null ? "null" : "\"" + e.getValue() + "\"").append(",");
                json.append("\"isTombstone\":").append(e.getValue() == null).append("}");
            }
            json.append("],");

            // Cache Stats
            BlockCache cache = engine.getBlockCache();
            json.append("\"cache\":{");
            json.append("\"size\":").append(cache.size()).append(",");
            json.append("\"capacity\":").append(cache.getCapacity()).append(",");
            json.append("\"hits\":").append(cache.getHitCount()).append(",");
            json.append("\"misses\":").append(cache.getMissCount()).append(",");
            json.append("\"hitRatio\":").append(String.format(Locale.US, "%.2f", cache.getHitRatio() * 100));
            json.append("},");

            // Levels
            json.append("\"levels\":[");
            List<Level> levels = engine.getLevelManager().getLevels();
            for (int l = 0; l < levels.size(); l++) {
                if (l > 0) json.append(",");
                Level level = levels.get(l);
                json.append("{\"level\":").append(level.getLevelNumber()).append(",");
                json.append("\"tables\":[");
                List<SSTableReader> tables = level.getTables();
                for (int t = 0; t < tables.size(); t++) {
                    if (t > 0) json.append(",");
                    SSTableReader reader = tables.get(t);
                    File dataFile = new File(reader.getBasePath() + ".sb");
                    json.append("{");
                    json.append("\"name\":\"").append(dataFile.getName()).append("\",");
                    json.append("\"basePath\":\"").append(reader.getBasePath().replace("\\", "/")).append("\",");
                    json.append("\"minKey\":\"").append(reader.getMinKey()).append("\",");
                    json.append("\"maxKey\":\"").append(reader.getMaxKey()).append("\",");
                    json.append("\"blocks\":").append(reader.getBlockIndex().size()).append(",");
                    json.append("\"fileSize\":").append(dataFile.exists() ? dataFile.length() : 0);
                    json.append("}");
                }
                json.append("]}");
            }
            json.append("]}");

            sendResponse(exchange, 200, json.toString(), "application/json");
        }
    }

    static class PutHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            String body = readBody(exchange);
            String key = extractJsonField(body, "key");
            String val = extractJsonField(body, "value");

            if (key == null || key.isEmpty()) {
                sendResponse(exchange, 400, "{\"error\":\"Key required\"}", "application/json");
                return;
            }

            engine.put(key, val);
            sendResponse(exchange, 200, "{\"status\":\"ok\",\"key\":\"" + key + "\",\"value\":\"" + val + "\"}", "application/json");
        }
    }

    static class GetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            String key = null;
            String query = exchange.getRequestURI().getQuery();
            if (query != null) {
                for (String param : query.split("&")) {
                    String[] pair = param.split("=");
                    if (pair.length == 2 && "key".equalsIgnoreCase(pair[0])) {
                        key = java.net.URLDecoder.decode(pair[1], StandardCharsets.UTF_8);
                        break;
                    }
                }
            }
            if (key == null || key.isEmpty()) {
                String body = readBody(exchange);
                key = extractJsonField(body, "key");
            }

            if (key == null || key.isEmpty()) {
                sendResponse(exchange, 400, "{\"error\":\"Key required\"}", "application/json");
                return;
            }

            String location = "None";
            if (engine.getActiveMemTable().containsKey(key)) {
                location = "MemTable (RAM Fast Tier)";
            } else {
                for (Level level : engine.getLevelManager().getLevels()) {
                    for (SSTableReader table : level.getTables()) {
                        if (table.get(key) != null) {
                            location = "Level " + level.getLevelNumber() + " (" + new File(table.getBasePath() + ".sb").getName() + ")";
                            break;
                        }
                    }
                    if (!"None".equals(location)) break;
                }
            }

            Object value = engine.get(key);
            String valJson = (value == null) ? "null" : "\"" + value + "\"";
            boolean found = (value != null);

            StringBuilder resp = new StringBuilder("{");
            resp.append("\"key\":\"").append(key).append("\",");
            resp.append("\"value\":").append(valJson).append(",");
            resp.append("\"found\":").append(found).append(",");
            resp.append("\"location\":\"").append(location).append("\"");
            resp.append("}");

            sendResponse(exchange, 200, resp.toString(), "application/json");
        }
    }

    static class DeleteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            String body = readBody(exchange);
            String key = extractJsonField(body, "key");

            if (key == null || key.isEmpty()) {
                sendResponse(exchange, 400, "{\"error\":\"Key required\"}", "application/json");
                return;
            }

            engine.delete(key);
            sendResponse(exchange, 200, "{\"status\":\"ok\",\"deletedKey\":\"" + key + "\"}", "application/json");
        }
    }

    static class FlushHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            engine.flushMemTable();
            sendResponse(exchange, 200, "{\"status\":\"flushed\"}", "application/json");
        }
    }

    static class CompactHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            engine.triggerCompaction();
            sendResponse(exchange, 200, "{\"status\":\"compacted\"}", "application/json");
        }
    }

    static class SwitchDbHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            String body = readBody(exchange);
            String dir = extractJsonField(body, "dir");
            String threshStr = extractJsonField(body, "threshold");

            if (dir != null && !dir.isEmpty()) {
                currentDbDir = dir;
            }
            if (threshStr != null) {
                try {
                    currentThreshold = Integer.parseInt(threshStr);
                } catch (Exception ignored) {}
            }

            if (engine != null) {
                engine.close();
            }
            engine = new SidDBEngine(currentDbDir, currentThreshold);

            sendResponse(exchange, 200, "{\"status\":\"switched\",\"dir\":\"" + currentDbDir + "\",\"threshold\":" + currentThreshold + "}", "application/json");
        }
    }

    static class ResetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            if (engine != null) {
                engine.close();
            }
            File dir = new File(currentDbDir);
            if (dir.exists()) {
                File[] files = dir.listFiles();
                if (files != null) {
                    for (File f : files) f.delete();
                }
            }
            engine = new SidDBEngine(currentDbDir, currentThreshold);
            sendResponse(exchange, 200, "{\"status\":\"reset\"}", "application/json");
        }
    }

    // ==========================================
    // RAFT CLUSTER ENDPOINTS
    // ==========================================

    static class ClusterStatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }

            StringBuilder json = new StringBuilder("{");
            RaftNode leader = cluster.getLeader();
            int totalNodes = cluster.getNodes().size();
            int quorum = (totalNodes / 2) + 1;
            json.append("\"leader\":\"").append(leader != null ? leader.getNodeId() : "NONE").append("\",");
            json.append("\"nodeCount\":").append(totalNodes).append(",");
            json.append("\"quorum\":").append(quorum).append(",");
            json.append("\"messagesSent\":").append(cluster.getNetwork().getMessagesSent()).append(",");
            json.append("\"messagesDropped\":").append(cluster.getNetwork().getMessagesDropped()).append(",");

            json.append("\"nodes\":[");
            int nIdx = 0;
            for (RaftNode node : cluster.getNodes()) {
                if (nIdx++ > 0) json.append(",");
                boolean isIsolated = cluster.getNetwork().isIsolated(node.getNodeId());
                json.append("{");
                json.append("\"nodeId\":\"").append(node.getNodeId()).append("\",");
                json.append("\"role\":\"").append(node.getRole()).append("\",");
                json.append("\"term\":").append(node.getCurrentTerm()).append(",");
                json.append("\"votedFor\":").append(node.getVotedFor() != null ? "\"" + node.getVotedFor() + "\"" : "null").append(",");
                json.append("\"leaderId\":").append(node.getLeaderId() != null ? "\"" + node.getLeaderId() + "\"" : "null").append(",");
                json.append("\"commitIndex\":").append(node.getLog().getCommitIndex()).append(",");
                json.append("\"lastApplied\":").append(node.getLog().getLastApplied()).append(",");
                json.append("\"isIsolated\":").append(isIsolated).append(",");
                json.append("\"activeMemTableSize\":").append(node.getStateMachine() != null ? node.getStateMachine().getActiveMemTable().size() : 0).append(",");

                // Replicated log entries
                json.append("\"logEntries\":[");
                List<RaftLogEntry> entries = node.getLog().getEntries();
                int eIdx = 0;
                for (RaftLogEntry entry : entries) {
                    if (entry.getIndex() == 0) continue; // skip dummy
                    if (eIdx++ > 0) json.append(",");
                    boolean isCommitted = entry.getIndex() <= node.getLog().getCommitIndex();
                    json.append("{");
                    json.append("\"index\":").append(entry.getIndex()).append(",");
                    json.append("\"term\":").append(entry.getTerm()).append(",");
                    json.append("\"cmd\":\"").append(entry.getCommandType()).append("\",");
                    json.append("\"key\":").append(entry.getKey() != null ? "\"" + entry.getKey() + "\"" : "null").append(",");
                    json.append("\"val\":").append(entry.getValue() != null ? "\"" + entry.getValue() + "\"" : "null").append(",");
                    json.append("\"isCommitted\":").append(isCommitted);
                    json.append("}");
                }
                json.append("]");
                json.append("}");
            }
            json.append("]}");

            sendResponse(exchange, 200, json.toString(), "application/json");
        }
    }

    static class ClusterProposeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            String body = readBody(exchange);
            String cmd = extractJsonField(body, "cmd");
            String key = extractJsonField(body, "key");
            String val = extractJsonField(body, "value");

            if (cmd == null || cmd.isEmpty()) cmd = "PUT";
            if (key == null || key.isEmpty()) {
                sendResponse(exchange, 400, "{\"error\":\"Key required\"}", "application/json");
                return;
            }

            RaftNode leader = cluster.getLeader();
            if (leader == null) {
                sendResponse(exchange, 503, "{\"error\":\"No active Raft leader elected or cluster partitioned\"}", "application/json");
                return;
            }

            try {
                boolean committed = leader.propose(cmd, key, val).get(3, TimeUnit.SECONDS);
                if (committed) {
                    sendResponse(exchange, 200, "{\"status\":\"committed\",\"leader\":\"" + leader.getNodeId() + "\",\"term\":" + leader.getCurrentTerm() + ",\"key\":\"" + key + "\"}", "application/json");
                } else {
                    sendResponse(exchange, 500, "{\"error\":\"Failed to reach majority quorum for proposal\"}", "application/json");
                }
            } catch (Exception e) {
                sendResponse(exchange, 500, "{\"error\":\"Timeout/Failure replicating proposal: " + e.getMessage() + "\"}", "application/json");
            }
        }
    }

    static class ClusterPartitionHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            String body = readBody(exchange);
            String nodeId = extractJsonField(body, "nodeId");

            if (nodeId == null || nodeId.isEmpty()) {
                // Default: isolate current leader
                RaftNode leader = cluster.getLeader();
                if (leader != null) nodeId = leader.getNodeId();
                else nodeId = "node-1";
            }

            cluster.isolateNode(nodeId);
            sendResponse(exchange, 200, "{\"status\":\"isolated\",\"nodeId\":\"" + nodeId + "\"}", "application/json");
        }
    }

    static class ClusterHealHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            String body = readBody(exchange);
            String nodeId = extractJsonField(body, "nodeId");

            if (nodeId != null && !nodeId.isEmpty()) {
                cluster.healNode(nodeId);
                sendResponse(exchange, 200, "{\"status\":\"healed\",\"nodeId\":\"" + nodeId + "\"}", "application/json");
            } else {
                cluster.healAll();
                sendResponse(exchange, 200, "{\"status\":\"all_healed\"}", "application/json");
            }
        }
    }

    static class ClusterResetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            String body = readBody(exchange);
            String nodeCountStr = extractJsonField(body, "nodeCount");
            int nodeCount = 3;
            if (nodeCountStr != null) {
                try {
                    nodeCount = Integer.parseInt(nodeCountStr);
                    if (nodeCount < 1) nodeCount = 1;
                    if (nodeCount > 9) nodeCount = 9;
                } catch (NumberFormatException ignored) {}
            }

            if (cluster != null) {
                cluster.close();
            }
            File clusterDir = new File("data/cluster");
            if (clusterDir.exists()) {
                deleteDir(clusterDir);
            }

            List<String> clusterNodes = new ArrayList<>();
            for (int i = 1; i <= nodeCount; i++) {
                clusterNodes.add("node-" + i);
            }
            cluster = new RaftCluster("data/cluster", clusterNodes, currentThreshold);
            cluster.start();

            sendResponse(exchange, 200, "{\"status\":\"cluster_reset\",\"nodeCount\":" + nodeCount + "}", "application/json");
        }

        private void deleteDir(File dir) {
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

    // ==========================================
    // Chaos Engineering & Failure Handlers
    // ==========================================

    private static volatile ChaosReport lastChaosReport = null;

    static class ChaosStatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("hasReport", lastChaosReport != null);
            if (lastChaosReport != null) {
                resp.put("passedCount", lastChaosReport.getPassedCount());
                resp.put("totalCount", lastChaosReport.getScenarioResults().size());
                resp.put("allPassed", lastChaosReport.isAllPassed());
            }
            sendResponse(exchange, 200, toJson(resp), "application/json");
        }
    }

    static class ChaosRunHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            String body = readBody(exchange);
            String scenarioParam = extractJsonField(body, "scenario");
            if (scenarioParam == null || scenarioParam.isEmpty()) {
                scenarioParam = "all";
            }

            File chaosDir = new File("data/chaos-cluster");
            if (chaosDir.exists()) {
                deleteDir(chaosDir);
            }
            chaosDir.mkdirs();

            try {
                SimulatedNetwork simNet = new SimulatedNetwork();
                ChaoticTransport chaosTrans = new ChaoticTransport(simNet);
                List<String> nodeIds = Arrays.asList("node-1", "node-2", "node-3");
                RaftCluster chaosCluster = new RaftCluster(chaosDir.getAbsolutePath(), nodeIds, currentThreshold, chaosTrans);
                chaosCluster.start();

                ChaosClusterContext ctx = new ChaosClusterContext(chaosCluster, chaosTrans);
                ChaosEngine engine = new ChaosEngine();

                if ("packet_loss".equalsIgnoreCase(scenarioParam)) {
                    engine.register(new PacketLossScenario());
                } else if ("latency_jitter".equalsIgnoreCase(scenarioParam)) {
                    engine.register(new LatencyJitterScenario());
                } else if ("slow_follower".equalsIgnoreCase(scenarioParam)) {
                    engine.register(new SlowFollowerScenario());
                } else if ("leader_crash".equalsIgnoreCase(scenarioParam)) {
                    engine.register(new NodeCrashRestartScenario());
                } else if ("split_brain".equalsIgnoreCase(scenarioParam)) {
                    engine.register(new SplitBrainScenario());
                } else if ("message_reorder".equalsIgnoreCase(scenarioParam)) {
                    engine.register(new MessageReorderScenario());
                } else if ("correlated_crash".equalsIgnoreCase(scenarioParam)) {
                    engine.register(new CorrelatedCrashScenario());
                } else if ("flapping_node".equalsIgnoreCase(scenarioParam)) {
                    engine.register(new FlappingNodeScenario());
                } else if ("disk_fault".equalsIgnoreCase(scenarioParam)) {
                    engine.register(new DiskFaultScenario());
                } else {
                    engine = ChaosEngine.createStandardSuite();
                }

                lastChaosReport = engine.run(ctx);
                chaosCluster.close();
                deleteDir(chaosDir);

                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("status", "completed");
                resp.put("scenario", scenarioParam);
                resp.put("allPassed", lastChaosReport.isAllPassed());
                resp.put("passedCount", lastChaosReport.getPassedCount());
                resp.put("totalCount", lastChaosReport.getScenarioResults().size());
                resp.put("markdownReport", lastChaosReport.toMarkdown());

                sendResponse(exchange, 200, toJson(resp), "application/json");

            } catch (Exception e) {
                sendResponse(exchange, 500, "{\"status\":\"error\",\"message\":\"" + e.getMessage() + "\"}", "application/json");
            }
        }

        private void deleteDir(File dir) {
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

    static class ChaosReportHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            if (lastChaosReport != null) {
                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("markdown", lastChaosReport.toMarkdown());
                resp.put("allPassed", lastChaosReport.isAllPassed());
                sendResponse(exchange, 200, toJson(resp), "application/json");
            } else {
                File file = new File("ChaosReport.md");
                if (file.exists()) {
                    byte[] bytes = java.nio.file.Files.readAllBytes(file.toPath());
                    String content = new String(bytes, StandardCharsets.UTF_8);
                    Map<String, Object> resp = new LinkedHashMap<>();
                    resp.put("markdown", content);
                    resp.put("allPassed", true);
                    sendResponse(exchange, 200, toJson(resp), "application/json");
                } else {
                    sendResponse(exchange, 404, "{\"status\":\"not_found\",\"message\":\"No chaos report generated yet. Run a scenario first.\"}", "application/json");
                }
            }
        }
    }

    static class ChaosResetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "", "application/json");
                return;
            }
            lastChaosReport = null;
            sendResponse(exchange, 200, "{\"status\":\"chaos_reset\"}", "application/json");
        }
    }
}
