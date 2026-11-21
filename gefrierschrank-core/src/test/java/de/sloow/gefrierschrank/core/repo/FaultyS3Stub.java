package de.sloow.gefrierschrank.core.repo;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An S3 GET endpoint over plain sockets that can break a response body after a chosen number of
 * bytes: a reset (RST), a premature close (FIN) or a stall (no more bytes, connection kept
 * open). Every response closes its connection. It serves path-style {@code /bucket/<key>}.
 */
final class FaultyS3Stub implements AutoCloseable {

    enum Fault { RESET, CLOSE, STALL }

    /** How the stub labels a blob's version; NONE sends no ETag. */
    enum ETagMode { STRONG, WEAK, NONE }

    record Request(int number, String method, String key, String range, String ifMatch) {}

    /** A fault for one response body: break it after {@code afterBytes} body bytes. */
    record Injection(Fault fault, long afterBytes) {}

    interface Plan {
        /** Null serves the response whole. {@code bodyLength} is the length of the body about to be sent. */
        Injection next(Request request, long bodyLength);
    }

    private record Blob(byte[] content, String eTag) {}

    private static final Pattern RANGE = Pattern.compile("bytes=(\\d+)-(\\d+)");
    private static final String BUCKET = "bucket";

    private final ServerSocket server;
    private final Map<String, Blob> blobs = new ConcurrentHashMap<>();
    private final List<Request> requests = new ArrayList<>();
    private final Set<Socket> open = ConcurrentHashMap.newKeySet();
    final AtomicInteger activeConnections = new AtomicInteger();
    final AtomicInteger faultsInjected = new AtomicInteger();
    final AtomicLong bodyBytesSent = new AtomicLong();
    /** Body bytes sent before each injected fault, in injection order. */
    final List<Long> faultOffsets = new ArrayList<>();
    volatile Plan plan = (request, bodyLength) -> null;
    volatile ETagMode eTagMode = ETagMode.STRONG;
    volatile boolean ignoresIfMatch;
    /** How long a reset waits after its last bytes, so the client has read them before the RST. */
    volatile long resetDelayMillis = 300;
    private volatile boolean closed;

    FaultyS3Stub() throws IOException {
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread acceptor = new Thread(this::acceptLoop, "faulty-s3-accept");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    String endpoint() {
        return "http://127.0.0.1:" + server.getLocalPort();
    }

    S3BlobStore store(S3BlobStore.BodyResume resume, Duration socketTimeout) {
        return S3BlobStore.s3(endpoint(), BUCKET, "", StaticCredentialsProvider.create(
                AwsBasicCredentials.create("AKIDSTUB", "stub-secret")), "us-east-1", S3BlobStoreRetryTest.FAST,
            resume, socketTimeout);
    }

    void put(String key, byte[] content, String version) {
        blobs.put(key, new Blob(content, "\"" + version + "\""));
    }

    List<Request> requests() {
        synchronized (requests) {
            return List.copyOf(requests);
        }
    }

    List<Long> faultOffsets() {
        synchronized (faultOffsets) {
            return List.copyOf(faultOffsets);
        }
    }

    private void acceptLoop() {
        while (!closed) {
            try {
                Socket socket = server.accept();
                activeConnections.incrementAndGet();
                open.add(socket);
                Thread handler = new Thread(() -> {
                    try {
                        handle(socket);
                    } catch (IOException | RuntimeException ignored) {
                        // the client went away or the fault closed the socket
                    } finally {
                        closeQuietly(socket);
                        open.remove(socket);
                        activeConnections.decrementAndGet();
                    }
                }, "faulty-s3-conn");
                handler.setDaemon(true);
                handler.start();
            } catch (IOException e) {
                if (!closed) throw new IllegalStateException(e);
            }
        }
    }

    private void handle(Socket socket) throws IOException {
        InputStream in = new BufferedInputStream(socket.getInputStream());
        String requestLine = line(in);
        if (requestLine == null || requestLine.isEmpty()) return;
        Map<String, String> headers = new HashMap<>();
        for (String header = line(in); header != null && !header.isEmpty(); header = line(in)) {
            int colon = header.indexOf(':');
            if (colon > 0) headers.put(header.substring(0, colon).trim().toLowerCase(), header.substring(colon + 1).trim());
        }
        String[] parts = requestLine.split(" ");
        String path = parts[1];
        int query = path.indexOf('?');
        if (query >= 0) path = path.substring(0, query);
        String key = path.startsWith("/" + BUCKET + "/") ? path.substring(BUCKET.length() + 2) : path;
        Request request;
        synchronized (requests) {
            request = new Request(requests.size() + 1, parts[0], key, headers.get("range"), headers.get("if-match"));
            requests.add(request);
        }
        OutputStream out = socket.getOutputStream();
        Blob blob = blobs.get(key);
        if (blob == null) {
            error(out, 404, "NoSuchKey");
            return;
        }
        String eTag = switch (eTagMode) {
            case STRONG -> blob.eTag();
            case WEAK -> "W/" + blob.eTag();
            case NONE -> null;
        };
        if (request.ifMatch() != null && !ignoresIfMatch && !request.ifMatch().equals(eTag)) {
            error(out, 412, "PreconditionFailed");
            return;
        }
        byte[] content = blob.content();
        int first = 0;
        int last = content.length - 1;
        int status = 200;
        String contentRange = null;
        if (request.range() != null) {
            Matcher range = RANGE.matcher(request.range());
            if (!range.matches()) throw new IllegalStateException("unexpected Range " + request.range());
            first = Integer.parseInt(range.group(1));
            last = (int) Math.min(Long.parseLong(range.group(2)), content.length - 1L);
            if (first >= content.length) {
                error(out, 416, "InvalidRange");
                return;
            }
            status = 206;
            contentRange = "bytes " + first + "-" + last + "/" + content.length;
        }
        int bodyLength = last - first + 1;
        var head = new StringBuilder("HTTP/1.1 " + status + (status == 200 ? " OK" : " Partial Content") + "\r\n")
            .append("Content-Type: application/octet-stream\r\n")
            .append("Content-Length: ").append(bodyLength).append("\r\n")
            .append("Connection: close\r\n");
        if (eTag != null) head.append("ETag: ").append(eTag).append("\r\n");
        if (contentRange != null) head.append("Content-Range: ").append(contentRange).append("\r\n");
        head.append("\r\n");
        out.write(head.toString().getBytes(StandardCharsets.US_ASCII));
        if (!request.method().equals("GET")) {
            out.flush();
            return;
        }
        Injection injection = plan.next(request, bodyLength);
        if (injection == null || injection.afterBytes() >= bodyLength) {
            out.write(content, first, bodyLength);
            out.flush();
            bodyBytesSent.addAndGet(bodyLength);
            socket.shutdownOutput();
            drain(in);
            return;
        }
        int sent = (int) injection.afterBytes();
        out.write(content, first, sent);
        out.flush();
        bodyBytesSent.addAndGet(sent);
        synchronized (faultOffsets) {
            faultOffsets.add((long) sent);
        }
        faultsInjected.incrementAndGet();
        switch (injection.fault()) {
            case RESET -> {
                sleep(resetDelayMillis);
                socket.setSoLinger(true, 0);
                socket.close();
            }
            case CLOSE -> socket.close();
            case STALL -> drain(in);
        }
    }

    /** Blocks until the client closes its side or the stub shuts down. */
    private static void drain(InputStream in) {
        try {
            while (in.read() >= 0) {
                // a client never sends more on a GET connection
            }
        } catch (IOException ignored) {
            // the client aborted
        }
    }

    private static void error(OutputStream out, int status, String code) throws IOException {
        byte[] body = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" + code
            + "</Code><Message>stub</Message><RequestId>stub</RequestId></Error>").getBytes(StandardCharsets.UTF_8);
        out.write(("HTTP/1.1 " + status + " Error\r\nContent-Type: application/xml\r\nContent-Length: " + body.length
            + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
        out.write(body);
        out.flush();
    }

    private static String line(InputStream in) throws IOException {
        var line = new StringBuilder();
        int c;
        while ((c = in.read()) >= 0) {
            if (c == '\n') break;
            if (c != '\r') line.append((char) c);
        }
        return c < 0 && line.isEmpty() ? null : line.toString();
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

    @Override
    public void close() throws IOException {
        closed = true;
        server.close();
        for (Socket socket : open) closeQuietly(socket);
    }
}
