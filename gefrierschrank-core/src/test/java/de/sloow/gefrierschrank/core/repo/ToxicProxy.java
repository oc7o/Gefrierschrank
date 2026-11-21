package de.sloow.gefrierschrank.core.repo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A TCP proxy in front of an S3 endpoint that breaks responses mid-body: once a connection has
 * carried {@code resetAfterBytes} response bytes it is reset (RST), or with {@link #stall} it
 * stops forwarding and keeps the connection open, for the first {@code resets} such connections.
 * With {@code storm} the first trigger resets every connection carrying a response at that
 * moment.
 */
final class ToxicProxy implements AutoCloseable {

    private final ServerSocket server;
    private final String targetHost;
    private final int targetPort;
    private final long resetAfterBytes;
    private final AtomicInteger resetsLeft;
    private final boolean storm;
    volatile boolean stall;
    final AtomicInteger connections = new AtomicInteger();
    final AtomicInteger resetsDone = new AtomicInteger();
    final AtomicLong bytesDown = new AtomicLong();
    private final Set<Connection> open = ConcurrentHashMap.newKeySet();
    private volatile boolean closed;

    private final class Connection {
        final Socket client;
        final Socket upstream;
        final AtomicLong down = new AtomicLong();

        Connection(Socket client, Socket upstream) {
            this.client = client;
            this.upstream = upstream;
        }

        void reset() {
            try {
                client.setSoLinger(true, 0);
            } catch (IOException ignored) {
                // already closed
            }
            closeQuietly(client);
            closeQuietly(upstream);
        }
    }

    ToxicProxy(String targetHost, int targetPort, long resetAfterBytes, int resets, boolean storm) throws IOException {
        this.server = new ServerSocket();
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        this.targetHost = targetHost;
        this.targetPort = targetPort;
        this.resetAfterBytes = resetAfterBytes;
        this.resetsLeft = new AtomicInteger(resets);
        this.storm = storm;
        Thread acceptor = new Thread(this::acceptLoop, "toxic-accept");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    String endpoint() {
        return "http://127.0.0.1:" + server.getLocalPort();
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket client = server.accept();
                client.setTcpNoDelay(true);
                Socket upstream = new Socket(targetHost, targetPort);
                upstream.setTcpNoDelay(true);
                var connection = new Connection(client, upstream);
                connections.incrementAndGet();
                open.add(connection);
                start(() -> pump(connection, upstream, client, true));
                start(() -> pump(connection, client, upstream, false));
            } catch (IOException e) {
                // closed, or the upstream refused
            }
        }
    }

    private static void start(Runnable pump) {
        Thread thread = new Thread(pump, "toxic-pump");
        thread.setDaemon(true);
        thread.start();
    }

    private void pump(Connection connection, Socket from, Socket to, boolean downstream) {
        byte[] buffer = new byte[64 * 1024];
        try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
            int n;
            while ((n = in.read(buffer)) >= 0) {
                if (downstream && resetAfterBytes > 0 && connection.down.get() + n > resetAfterBytes
                        && resetsLeft.getAndDecrement() > 0) {
                    int keep = (int) Math.max(0, resetAfterBytes - connection.down.get());
                    out.write(buffer, 0, keep);
                    out.flush();
                    bytesDown.addAndGet(keep);
                    resetsDone.incrementAndGet();
                    if (stall) {
                        // The client only notices through its read timeout; its close ends the other pump,
                        // which closes this connection.
                        while (!closed && !connection.client.isClosed()) sleep(20);
                        return;
                    }
                    if (storm) {
                        for (Connection other : open) if (other.down.get() > 0) other.reset();
                    }
                    connection.reset();
                    return;
                }
                out.write(buffer, 0, n);
                if (downstream) {
                    connection.down.addAndGet(n);
                    bytesDown.addAndGet(n);
                }
            }
        } catch (IOException e) {
            // peer closed or reset
        } finally {
            closeQuietly(to);
            closeQuietly(from);
            open.remove(connection);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // already closed
        }
    }

    String stats() {
        return "connections=" + connections.get() + " resets=" + resetsDone.get() + " bytesDown=" + bytesDown.get();
    }

    @Override
    public void close() throws IOException {
        closed = true;
        server.close();
        for (Connection connection : open) connection.reset();
    }
}
