package server;

import engine.SidDBEngine;
import network.SocketTransport;
import raft.RaftLogEntry;
import raft.RaftNode;
import raft.RaftRole;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Standalone Distributed Node Server running over real TCP sockets.
 * 
 * Usage:
 *   java -cp out server.DistributedNodeServer <nodeId> <tcpPort> <peersConfig> [dbDir]
 * 
 * Example:
 *   java -cp out server.DistributedNodeServer node-1 9001 node-2=127.0.0.1:9002,node-3=127.0.0.1:9003
 */
public class DistributedNodeServer {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("==================================================================");
            System.out.println("       SidDB Distributed Raft Node (Real TCP Sockets)            ");
            System.out.println("==================================================================");
            System.out.println("Usage:");
            System.out.println("  java -cp out server.DistributedNodeServer <nodeId> <tcpPort> [peersConfig] [dbDir]");
            System.out.println();
            System.out.println("Example (3 Nodes on Localhost):");
            System.out.println("  Terminal 1: java -cp out server.DistributedNodeServer node-1 9001 node-2=127.0.0.1:9002,node-3=127.0.0.1:9003");
            System.out.println("  Terminal 2: java -cp out server.DistributedNodeServer node-2 9002 node-1=127.0.0.1:9001,node-3=127.0.0.1:9003");
            System.out.println("  Terminal 3: java -cp out server.DistributedNodeServer node-3 9003 node-1=127.0.0.1:9001,node-2=127.0.0.1:9002");
            System.out.println("==================================================================");
            return;
        }

        String nodeId = args[0];
        int tcpPort = Integer.parseInt(args[1]);
        String peersConfig = args.length > 2 ? args[2] : "";
        String dbDir = args.length > 3 ? args[3] : "data/distributed_" + nodeId;
        String syncPolicyStr = args.length > 4 ? args[4] : "SYNC_EVERY_ENTRY";
        raft.SyncPolicy syncPolicy = raft.SyncPolicy.SYNC_EVERY_ENTRY;
        try {
            syncPolicy = raft.SyncPolicy.valueOf(syncPolicyStr.trim().toUpperCase());
        } catch (Exception ignored) {}

        List<String> peerIds = new ArrayList<>();
        Map<String, String> peerHostMap = new HashMap<>();
        Map<String, Integer> peerPortMap = new HashMap<>();

        if (!peersConfig.isEmpty() && !"none".equalsIgnoreCase(peersConfig)) {
            for (String peerDef : peersConfig.split(",")) {
                String[] parts = peerDef.trim().split("=");
                if (parts.length == 2) {
                    String pId = parts[0].trim();
                    String[] hostPort = parts[1].trim().split(":");
                    if (hostPort.length == 2) {
                        peerIds.add(pId);
                        peerHostMap.put(pId, hostPort[0]);
                        peerPortMap.put(pId, Integer.parseInt(hostPort[1]));
                    }
                }
            }
        }

        // 1. Initialize local persistent SidDB Storage Engine
        File dir = new File(dbDir);
        if (!dir.exists()) dir.mkdirs();
        SidDBEngine engine = new SidDBEngine(dir.getAbsolutePath(), 10);

        // 2. Initialize real TCP Socket Transport
        SocketTransport transport = new SocketTransport(nodeId, tcpPort);
        for (String pId : peerIds) {
            transport.registerPeer(pId, peerHostMap.get(pId), peerPortMap.get(pId));
        }
        transport.startServer();

        // 3. Initialize Raft State Machine
        List<String> allClusterNodes = new ArrayList<>(peerIds);
        allClusterNodes.add(nodeId);
        RaftNode raftNode = new RaftNode(nodeId, allClusterNodes, transport, engine, dir.getAbsolutePath(), 400, 800, 80, syncPolicy);
        raftNode.start();

        System.out.println("==================================================================");
        System.out.println(" [*] Raft Node '" + nodeId + "' listening on TCP Port: " + tcpPort);
        System.out.println(" [*] Persistent Storage Dir: " + dir.getAbsolutePath());
        System.out.println(" [*] Configured Peers: " + peerIds);
        System.out.println("==================================================================");
        System.out.println("Type 'status', 'put <key> <val>', 'get <key>', 'del <key>', or 'exit':");
        System.out.println();

        // Console REPL Loop for interactive testing
        try (BufferedReader console = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = console.readLine()) != null) {
                line = line.trim();
                if (line.equalsIgnoreCase("exit") || line.equalsIgnoreCase("quit")) {
                    break;
                }

                if (line.isEmpty()) continue;

                if (line.equalsIgnoreCase("status") || line.equalsIgnoreCase("info")) {
                    System.out.println("--------------------------------------------------");
                    System.out.println(" Node ID:       " + raftNode.getNodeId());
                    System.out.println(" Role:          " + raftNode.getRole());
                    System.out.println(" Current Term:  " + raftNode.getCurrentTerm());
                    System.out.println(" Leader ID:     " + (raftNode.getLeaderId() != null ? raftNode.getLeaderId() : "NONE"));
                    System.out.println(" Voted For:     " + (raftNode.getVotedFor() != null ? raftNode.getVotedFor() : "None"));
                    System.out.println(" Commit Index:  #" + raftNode.getLog().getCommitIndex());
                    System.out.println(" Last Applied:  #" + raftNode.getLog().getLastApplied());
                    System.out.println(" Replicated Log Entries (" + raftNode.getLog().getLastLogIndex() + "):");
                    for (RaftLogEntry entry : raftNode.getLog().getEntries()) {
                        if (entry.getIndex() == 0) continue;
                        boolean committed = entry.getIndex() <= raftNode.getLog().getCommitIndex();
                        System.out.println("   #" + entry.getIndex() + " [Term " + entry.getTerm() + "] " +
                                entry.getCommandType() + " " + entry.getKey() + " = " + entry.getValue() +
                                (committed ? " (Committed [OK])" : " (Pending [..])"));
                    }
                    System.out.println(" Local MemTable Keys: " + engine.keys());
                    System.out.println("--------------------------------------------------");
                    continue;
                }

                String[] parts = line.split("\\s+", 3);
                String cmd = parts[0].toUpperCase();

                if ("PUT".equals(cmd)) {
                    if (parts.length < 3) {
                        System.out.println("Usage: put <key> <val>");
                        continue;
                    }
                    String key = parts[1];
                    String val = parts[2];

                    if (raftNode.getRole() != RaftRole.LEADER) {
                        System.out.println(" [!] Cannot write: Current node is " + raftNode.getRole() + 
                                ". Please send write to Leader (" + (raftNode.getLeaderId() != null ? raftNode.getLeaderId() : "UNKNOWN") + ")");
                        continue;
                    }

                    System.out.println(" [*] Proposing PUT '" + key + "' = '" + val + "' to cluster over TCP...");
                    try {
                        boolean committed = raftNode.propose("PUT", key, val).get(3, TimeUnit.SECONDS);
                        if (committed) {
                            System.out.println(" [+] Successfully committed to quorum over TCP! CommitIndex: #" + raftNode.getLog().getCommitIndex());
                        } else {
                            System.out.println(" [-] Proposal failed: Majority quorum was not reached.");
                        }
                    } catch (Exception e) {
                        System.out.println(" [-] Error proposing write: " + e.getMessage());
                    }
                } else if ("DEL".equals(cmd) || "DELETE".equals(cmd)) {
                    if (parts.length < 2) {
                        System.out.println("Usage: del <key>");
                        continue;
                    }
                    String key = parts[1];
                    if (raftNode.getRole() != RaftRole.LEADER) {
                        System.out.println(" [!] Current node is not LEADER. Leader is: " + raftNode.getLeaderId());
                        continue;
                    }
                    try {
                        boolean committed = raftNode.propose("DELETE", key, null).get(3, TimeUnit.SECONDS);
                        if (committed) {
                            System.out.println(" [+] Successfully deleted '" + key + "' across cluster!");
                        } else {
                            System.out.println(" [-] Failed to delete: Quorum not reached.");
                        }
                    } catch (Exception e) {
                        System.out.println(" [-] Error: " + e.getMessage());
                    }
                } else if ("GET".equals(cmd)) {
                    if (parts.length < 2) {
                        System.out.println("Usage: get <key>");
                        continue;
                    }
                    String key = parts[1];
                    Object val = engine.get(key);
                    if (val != null) {
                        System.out.println(" [+] '" + key + "' = '" + val + "' (Local Node State)");
                    } else {
                        System.out.println(" [-] Key '" + key + "' not found in local database.");
                    }
                } else {
                    System.out.println("Unknown command. Supported commands: 'status', 'put <k> <v>', 'get <k>', 'del <k>', 'exit'");
                }
            }
        } finally {
            System.out.println("\n[*] Shutting down node '" + nodeId + "'...");
            raftNode.close();
            transport.close();
            System.out.println("[*] Shutdown complete.");
        }
    }
}
