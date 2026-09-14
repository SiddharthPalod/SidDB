package server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import engine.SidDBEngine;
import level.Level;
import sstable.BlockCache;
import sstable.SSTableEntry;
import sstable.SSTableReader;
import tx.Snapshot;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class SidDBServer {

    private static int port = 8080;
    private static SidDBEngine engine;
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

        // API Endpoints
        server.createContext("/", new StaticFileHandler());
        server.createContext("/api/state", new StateHandler());
        server.createContext("/api/put", new PutHandler());
        server.createContext("/api/get", new GetHandler());
        server.createContext("/api/delete", new DeleteHandler());
        server.createContext("/api/flush", new FlushHandler());
        server.createContext("/api/compact", new CompactHandler());
        server.createContext("/api/db/switch", new SwitchDbHandler());
        server.createContext("/api/reset", new ResetHandler());

        server.setExecutor(null); // Default executor
        System.out.println("==========================================================");
        System.out.println(" [*] SidDB Live Server running at: http://localhost:" + port);
        System.out.println(" [*] Active Database Directory: " + new File(currentDbDir).getAbsolutePath());
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

    // JSON Helper
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

    // /api/state: Returns live engine state as JSON
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

    // /api/put: Executes real engine.put(key, val)
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

    // /api/get: Executes real engine.get(key) with storage tier telemetry
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

            // Determine where the key resides
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

    // /api/delete: Executes real engine.delete(key)
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

    // /api/flush: Manual flush to L0
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

    // /api/compact: Manual compaction
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

    // /api/db/switch: Switch or create custom database folder
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

            // Close old and open new engine
            if (engine != null) {
                engine.close();
            }
            engine = new SidDBEngine(currentDbDir, currentThreshold);

            sendResponse(exchange, 200, "{\"status\":\"switched\",\"dir\":\"" + currentDbDir + "\",\"threshold\":" + currentThreshold + "}", "application/json");
        }
    }

    // /api/reset: Clean active database directory
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
}
