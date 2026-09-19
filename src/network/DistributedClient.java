package network;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

/**
 * High-performance TCP client driver for communicating with SidDB distributed cluster nodes.
 */
public class DistributedClient {

    private final String host;
    private final int port;
    private final String targetNodeId;

    public DistributedClient(String host, int port, String targetNodeId) {
        this.host = host;
        this.port = port;
        this.targetNodeId = targetNodeId;
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public String getTargetNodeId() { return targetNodeId; }

    public boolean put(String key, Object value, long timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 2000);
            socket.setSoTimeout((int) timeoutMs + 1000);

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeUTF("CLIENT_PUT");
            out.writeUTF(targetNodeId);
            out.writeUTF(key);
            out.writeObject(value);
            out.writeLong(timeoutMs);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            return in.readBoolean();
        } catch (Exception e) {
            return false;
        }
    }

    public Object get(String key) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 2000);
            socket.setSoTimeout(3000);

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeUTF("CLIENT_GET");
            out.writeUTF(targetNodeId);
            out.writeUTF(key);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            return in.readObject();
        } catch (Exception e) {
            return null;
        }
    }

    public Object readLinearizable(String key, long timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 2000);
            socket.setSoTimeout((int) timeoutMs + 1000);

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeUTF("CLIENT_READ_LINEARIZABLE");
            out.writeUTF(targetNodeId);
            out.writeUTF(key);
            out.writeLong(timeoutMs);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            return in.readObject();
        } catch (Exception e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> getStatus() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            socket.setSoTimeout(1500);

            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeUTF("CLIENT_STATUS");
            out.writeUTF(targetNodeId);
            out.flush();

            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            return (Map<String, Object>) in.readObject();
        } catch (Exception e) {
            return null;
        }
    }
}