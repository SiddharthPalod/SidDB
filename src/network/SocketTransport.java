package network;

import raft.AppendEntriesArgs;
import raft.AppendEntriesReply;
import raft.RaftNode;
import raft.RequestVoteArgs;
import raft.RequestVoteReply;

import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Real TCP/Socket transport for running separate multi-process instances of SidDBServer.
 * Uses TCP sockets to send serialized RPCs between distributed nodes.
 */
public class SocketTransport implements Transport, AutoCloseable {

    private final String localNodeId;
    private final int localPort;
    private final Map<String, InetSocketAddress> peerAddresses = new ConcurrentHashMap<>();
    private final Map<String, RaftNode> localNodes = new ConcurrentHashMap<>();

    private ServerSocket serverSocket;
    private final ExecutorService serverThreadPool = Executors.newCachedThreadPool();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public SocketTransport(String localNodeId, int localPort) {
        this.localNodeId = localNodeId;
        this.localPort = localPort;
    }

    public void registerPeer(String nodeId, String host, int port) {
        peerAddresses.put(nodeId, new InetSocketAddress(host, port));
    }

    public synchronized void startServer() throws IOException {
        if (running.compareAndSet(false, true)) {
            serverSocket = new ServerSocket(localPort);
            serverThreadPool.submit(this::listenLoop);
        }
    }

    private void listenLoop() {
        while (running.get() && serverSocket != null && !serverSocket.isClosed()) {
            try {
                Socket client = serverSocket.accept();
                serverThreadPool.submit(() -> handleIncomingConnection(client));
            } catch (IOException e) {
                if (!running.get()) break;
            }
        }
    }

    private void handleIncomingConnection(Socket socket) {
        try (socket;
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream())) {

            String rpcType = in.readUTF(); // "VOTE" or "APPEND"
            String targetNodeId = in.readUTF();

            RaftNode targetNode = localNodes.get(targetNodeId);
            if (targetNode == null) {
                out.writeObject(null);
                out.flush();
                return;
            }

            if ("VOTE".equals(rpcType)) {
                RequestVoteArgs args = (RequestVoteArgs) in.readObject();
                RequestVoteReply reply = targetNode.handleRequestVote(args);
                out.writeObject(reply);
            } else if ("APPEND".equals(rpcType)) {
                AppendEntriesArgs args = (AppendEntriesArgs) in.readObject();
                AppendEntriesReply reply = targetNode.handleAppendEntries(args);
                out.writeObject(reply);
            }
            out.flush();
        } catch (Exception ignored) {
        }
    }

    @Override
    public void registerNode(String nodeId, RaftNode node) {
        localNodes.put(nodeId, node);
    }

    @Override
    public void unregisterNode(String nodeId) {
        localNodes.remove(nodeId);
    }

    @Override
    public RequestVoteReply sendRequestVote(String fromNodeId, String targetNodeId, RequestVoteArgs args) {
        InetSocketAddress addr = peerAddresses.get(targetNodeId);
        if (addr == null) return null;

        try (Socket socket = new Socket()) {
            socket.connect(addr, 500); // 500ms connect timeout
            socket.setSoTimeout(500);  // 500ms read timeout

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeUTF("VOTE");
            out.writeUTF(targetNodeId);
            out.writeObject(args);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            return (RequestVoteReply) in.readObject();
        } catch (Exception e) {
            return null; // Target offline / partitioned
        }
    }

    @Override
    public AppendEntriesReply sendAppendEntries(String fromNodeId, String targetNodeId, AppendEntriesArgs args) {
        InetSocketAddress addr = peerAddresses.get(targetNodeId);
        if (addr == null) return null;

        try (Socket socket = new Socket()) {
            socket.connect(addr, 500); // 500ms connect timeout
            socket.setSoTimeout(500);  // 500ms read timeout

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeUTF("APPEND");
            out.writeUTF(targetNodeId);
            out.writeObject(args);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            return (AppendEntriesReply) in.readObject();
        } catch (Exception e) {
            return null; // Target offline / partitioned
        }
    }

    @Override
    public synchronized void close() {
        if (running.compareAndSet(true, false)) {
            try {
                if (serverSocket != null) serverSocket.close();
            } catch (IOException ignored) {}
            serverThreadPool.shutdownNow();
        }
    }
}
