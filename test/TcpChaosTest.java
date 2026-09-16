package test;

import network.ChaoticTransport;
import network.SocketTransport;
import raft.AppendEntriesArgs;
import raft.AppendEntriesReply;
import raft.RaftNode;
import raft.RequestVoteArgs;
import raft.RequestVoteReply;

import java.util.Arrays;

public class TcpChaosTest {

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        System.out.println("\n--- [Running TcpChaosTest: ChaoticTransport wrapping Real TCP Sockets] ---");

        int port1 = 9021;
        int port2 = 9022;

        try (SocketTransport rawTcp1 = new SocketTransport("node-1", port1);
             SocketTransport rawTcp2 = new SocketTransport("node-2", port2)) {

            rawTcp1.registerPeer("node-2", "127.0.0.1", port2);
            rawTcp2.registerPeer("node-1", "127.0.0.1", port1);

            rawTcp1.startServer();
            rawTcp2.startServer();

            // Wrap raw TCP transports with ChaoticTransport Decorator
            ChaoticTransport chaoticTcp1 = new ChaoticTransport(rawTcp1);
            ChaoticTransport chaoticTcp2 = new ChaoticTransport(rawTcp2);

            RaftNode node2 = new RaftNode("node-2", Arrays.asList("node-1"), chaoticTcp2, null);
            node2.start();

            // 1. Normal TCP RPC Delivery (0% Chaos)
            RequestVoteArgs voteArgs = new RequestVoteArgs(1, "node-1", 0, 0);
            RequestVoteReply voteReply = chaoticTcp1.sendRequestVote("node-1", "node-2", voteArgs);
            if (voteReply == null || !voteReply.isVoteGranted()) {
                throw new AssertionError("TCP RPC failed under 0% chaos");
            }
            System.out.println("  [+] Baseline: Clean TCP RPC round-trip on port " + port1 + " -> " + port2 + " succeeded.");

            // 2. TCP Chaos: 100% Packet Loss Injection
            chaoticTcp1.setPacketLossRate(1.0);
            AppendEntriesArgs appendArgs = new AppendEntriesArgs(1, "node-1", 0, 0, null, 0);
            AppendEntriesReply droppedReply = chaoticTcp1.sendAppendEntries("node-1", "node-2", appendArgs);
            if (droppedReply != null) {
                throw new AssertionError("Expected TCP packet to be dropped by ChaoticTransport, but got reply!");
            }
            System.out.println("  [+] TCP Packet Loss: Packet dropped before socket transmission (dropped count = " + chaoticTcp1.getMessagesDropped() + ")");

            // 3. TCP Chaos: Latency Jitter (150ms delay)
            chaoticTcp1.resetChaos();
            chaoticTcp1.setLatencyJitter(150, 150);
            long t0 = System.currentTimeMillis();
            AppendEntriesReply delayedReply = chaoticTcp1.sendAppendEntries("node-1", "node-2", appendArgs);
            long elapsed = System.currentTimeMillis() - t0;
            if (delayedReply == null || !delayedReply.isSuccess() || elapsed < 140) {
                throw new AssertionError("Expected TCP RPC to be delayed >= 140ms, took: " + elapsed + "ms");
            }
            System.out.println("  [+] TCP Latency Jitter: RPC delivered over TCP socket with " + elapsed + " ms transit delay.");

            // 4. TCP Chaos: Directional Partition
            chaoticTcp1.resetChaos();
            chaoticTcp1.blockTraffic("node-1", "node-2");
            AppendEntriesReply blockedReply = chaoticTcp1.sendAppendEntries("node-1", "node-2", appendArgs);
            if (blockedReply != null) {
                throw new AssertionError("Expected directional TCP block to drop message!");
            }
            System.out.println("  [+] TCP Directional Partition: 'node-1 -> node-2' blocked over TCP.");

            // 5. TCP Chaos: Heal Partition
            chaoticTcp1.unblockTraffic("node-1", "node-2");
            AppendEntriesReply healedReply = chaoticTcp1.sendAppendEntries("node-1", "node-2", appendArgs);
            if (healedReply == null || !healedReply.isSuccess()) {
                throw new AssertionError("Expected healed TCP RPC to succeed!");
            }
            System.out.println("  [+] TCP Healed: Real TCP socket traffic resumed immediately.");

            node2.stop();
            System.out.println("  [+] TcpChaosTest passed 100% successfully over real OS sockets!");
        }
    }
}
