package test;

import network.SocketTransport;
import raft.*;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class SocketTransportTest {

    public static void run() throws Exception {
        System.out.println("\n--- [Running SocketTransportTest (Real TCP Sockets RPC Transport)] ---");

        int port1 = 9011;
        int port2 = 9012;

        try (SocketTransport t1 = new SocketTransport("node-1", port1);
             SocketTransport t2 = new SocketTransport("node-2", port2)) {

            t1.registerPeer("node-2", "127.0.0.1", port2);
            t2.registerPeer("node-1", "127.0.0.1", port1);

            t1.startServer();
            t2.startServer();

            // Mock RaftNode on node-2
            RaftNode dummyNode2 = new RaftNode("node-2", Arrays.asList("node-1"), t2, null);
            dummyNode2.start();

            // Node 1 sends RequestVote over real TCP Socket to Node 2
            RequestVoteArgs voteArgs = new RequestVoteArgs(1, "node-1", 0, 0);
            RequestVoteReply voteReply = t1.sendRequestVote("node-1", "node-2", voteArgs);

            if (voteReply == null || !voteReply.isVoteGranted()) {
                throw new AssertionError("SocketTransport failed to deliver RequestVote RPC over TCP!");
            }
            System.out.println("  ✓ RequestVote RPC successfully sent & received over TCP socket (Port " + port1 + " -> " + port2 + ")");

            // Node 1 sends AppendEntries over real TCP Socket to Node 2
            AppendEntriesArgs appendArgs = new AppendEntriesArgs(1, "node-1", 0, 0, null, 0);
            AppendEntriesReply appendReply = t1.sendAppendEntries("node-1", "node-2", appendArgs);

            if (appendReply == null || !appendReply.isSuccess()) {
                throw new AssertionError("SocketTransport failed to deliver AppendEntries RPC over TCP!");
            }
            System.out.println("  ✓ AppendEntries (Heartbeat) RPC successfully sent & received over TCP socket!");

            dummyNode2.stop();
            System.out.println("  ✓ SocketTransportTest passed successfully!");
        }
    }
}
