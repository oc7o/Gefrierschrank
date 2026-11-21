package de.sloow.gefrierschrank.core.repo;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.retries.api.AcquireInitialTokenRequest;
import software.amazon.awssdk.retries.api.RefreshRetryTokenRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The source client against a local stub S3 endpoint: its retry budget, with millisecond
 * backoffs so that exhausting ten attempts stays fast, and ranged reads from stores that cannot
 * pin a blob version.
 */
class S3BlobStoreRetryTest {

    static final SourceRetryConfig FAST = new SourceRetryConfig(10, Duration.ofMillis(1), Duration.ofMillis(5),
        Duration.ofMillis(1), Duration.ofMillis(200), Duration.ofSeconds(2), Duration.ofSeconds(10));

    interface Responder {
        void respond(HttpExchange exchange, int requestNumber) throws IOException;
    }

    private HttpServer server;
    private ExecutorService executor;
    private final List<String> requests = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger ifMatchRequests = new AtomicInteger();
    private volatile Responder responder;
    private S3BlobStore store;

    @BeforeEach
    void startStub() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
            int number;
            synchronized (requests) {
                requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
                number = requests.size();
            }
            if (exchange.getRequestHeaders().containsKey("If-Match")) ifMatchRequests.incrementAndGet();
            exchange.getRequestBody().readAllBytes();
            responder.respond(exchange, number);
            exchange.close();
        });
        server.start();
        store = store(FAST);
    }

    @AfterEach
    void stopStub() {
        store.close();
        server.stop(0);
        executor.shutdownNow();
    }

    private S3BlobStore store(SourceRetryConfig retry) {
        return S3BlobStore.s3("http://127.0.0.1:" + server.getAddress().getPort(), "bucket", "repo",
            StaticCredentialsProvider.create(AwsBasicCredentials.create("AKIDSTUB", "stub-secret")), "us-east-1", retry);
    }

    static byte[] content(int length) {
        byte[] bytes = new byte[length];
        new SplittableRandom(length).nextBytes(bytes);
        return bytes;
    }

    static void ok(HttpExchange exchange, byte[] body) throws IOException {
        exchange.getResponseHeaders().add("ETag", "\"stub-etag\"");
        exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
        exchange.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    static void error(HttpExchange exchange, int status, String code) throws IOException {
        byte[] body = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>" + code
            + "</Code><Message>stub</Message><RequestId>stub</RequestId></Error>").getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/xml");
        if (exchange.getRequestMethod().equals("HEAD")) {
            exchange.sendResponseHeaders(status, -1);
            return;
        }
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private static final Pattern RANGE = Pattern.compile("bytes=(\\d+)-(\\d+)");

    /** A 206 for the requested range, with {@code eTag} unless it is null. */
    static void partial(HttpExchange exchange, byte[] blob, String eTag) throws IOException {
        Matcher range = RANGE.matcher(exchange.getRequestHeaders().getFirst("Range"));
        assertTrue(range.matches());
        int first = Integer.parseInt(range.group(1));
        int last = Math.min(Integer.parseInt(range.group(2)), blob.length - 1);
        if (eTag != null) exchange.getResponseHeaders().add("ETag", eTag);
        exchange.getResponseHeaders().add("Content-Range", "bytes " + first + "-" + last + "/" + blob.length);
        exchange.sendResponseHeaders(206, last - first + 1);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(blob, first, last - first + 1);
        }
    }

    /** Closes the connection without any response, like a reset during failover. */
    static void drop(HttpExchange exchange) throws IOException {
        throw new IOException("stub drops the connection");
    }

    private long requestsFor(String method) {
        synchronized (requests) {
            return requests.stream().filter(r -> r.startsWith(method + " ")).count();
        }
    }

    @Test
    void slowDownFiveTimesThenSuccessReturnsExactBytesAfterSixRequests() throws IOException {
        byte[] blob = content(100_000);
        responder = (exchange, n) -> {
            if (n <= 5) error(exchange, 503, "SlowDown");
            else ok(exchange, blob);
        };
        try (var in = store.read("indices/x/0/__blob")) {
            assertArrayEquals(blob, in.readAllBytes());
        }
        assertEquals(6, requests.size());
        assertTrue(requests.get(0).startsWith("GET /bucket/repo/indices/x/0/__blob"), requests.get(0));
    }

    @Test
    void internalErrorsAndDroppedConnectionsAreRetried() throws IOException {
        byte[] blob = content(4096);
        responder = (exchange, n) -> {
            if (n == 1) error(exchange, 500, "InternalError");
            else if (n == 2) drop(exchange);
            else ok(exchange, blob);
        };
        try (var in = store.read("blob")) {
            assertArrayEquals(blob, in.readAllBytes());
        }
        assertTrue(requests.size() >= 3, requests.toString());
    }

    @Test
    void retryAfterIsCappedInsteadOfStallingForTheServerSuggestedMinute() throws IOException {
        byte[] blob = content(512);
        responder = (exchange, n) -> {
            if (n == 1) {
                exchange.getResponseHeaders().add("Retry-After", "60");
                error(exchange, 429, "SlowDown");
            } else {
                ok(exchange, blob);
            }
        };
        long started = System.nanoTime();
        try (var in = store.read("blob")) {
            assertArrayEquals(blob, in.readAllBytes());
        }
        long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
        assertEquals(2, requests.size());
        assertTrue(elapsedMillis < 5_000, "waited " + elapsedMillis + " ms");
    }

    @Test
    void cappedStrategyShortensOnlySuggestedDelaysAboveTheCap() {
        var strategy = SourceRetryConfig.DEFAULT.retryStrategy();
        var failure = S3Exception.builder().statusCode(503).message("stub").build();
        var token = strategy.acquireInitialToken(AcquireInitialTokenRequest.create("scope")).token();
        var capped = strategy.refreshRetryToken(RefreshRetryTokenRequest.builder()
            .token(token).failure(failure).suggestedDelay(Duration.ofSeconds(60)).build());
        assertTrue(capped.delay().compareTo(Duration.ofSeconds(20)) <= 0, capped.delay().toString());
        var passed = strategy.refreshRetryToken(RefreshRetryTokenRequest.builder()
            .token(capped.token()).failure(failure).suggestedDelay(Duration.ofSeconds(1)).build());
        assertTrue(passed.delay().compareTo(Duration.ofSeconds(1)) >= 0, passed.delay().toString());
        assertEquals(10, strategy.maxAttempts());
    }

    @Test
    void operatorsSetTheAttemptCountThroughASystemProperty() {
        String previous = System.getProperty(S3BlobStore.MAX_ATTEMPTS_PROPERTY);
        try {
            System.clearProperty(S3BlobStore.MAX_ATTEMPTS_PROPERTY);
            assertSame(SourceRetryConfig.DEFAULT, SourceRetryConfig.configured());
            System.setProperty(S3BlobStore.MAX_ATTEMPTS_PROPERTY, "0");
            assertEquals(1, SourceRetryConfig.configured().maxAttempts());
            System.setProperty(S3BlobStore.MAX_ATTEMPTS_PROPERTY, "20");
            var twenty = SourceRetryConfig.configured();
            assertEquals(20, twenty.retryStrategy().maxAttempts());
            assertEquals(Duration.ofMinutes(20), twenty.apiCallTimeout());
            assertEquals(SourceRetryConfig.DEFAULT.baseDelay(), twenty.baseDelay());

            System.setProperty(S3BlobStore.MAX_ATTEMPTS_PROPERTY, "2");
            responder = (exchange, n) -> error(exchange, 500, "InternalError");
            try (var configured = S3BlobStore.s3("http://127.0.0.1:" + server.getAddress().getPort(), "bucket", "repo",
                    StaticCredentialsProvider.create(AwsBasicCredentials.create("AKIDSTUB", "stub-secret")), "us-east-1")) {
                assertThrows(S3Exception.class, () -> configured.read("blob"));
            }
            assertEquals(2, requests.size());
        } finally {
            if (previous == null) System.clearProperty(S3BlobStore.MAX_ATTEMPTS_PROPERTY);
            else System.setProperty(S3BlobStore.MAX_ATTEMPTS_PROPERTY, previous);
        }
    }

    @Test
    void accessDeniedIsNotRetried() {
        responder = (exchange, n) -> error(exchange, 403, "InvalidAccessKeyId");
        var failure = assertThrows(S3Exception.class, () -> store.read("blob"));
        assertEquals(403, failure.statusCode());
        assertEquals(1, requests.size());
    }

    @Test
    void missingBlobsFailAfterOneRequest() {
        responder = (exchange, n) -> error(exchange, 404, "NoSuchKey");
        assertTrue(store.stat("missing").isEmpty());
        assertEquals(1, requestsFor("HEAD"));
        var failure = assertThrows(IOException.class, () -> store.read("missing"));
        assertEquals("Blob not found: missing", failure.getMessage());
        assertEquals(1, requestsFor("GET"));
    }

    @Test
    void exhaustedCallsDoNotDrainTheRetryBudgetOfLaterCalls() {
        responder = (exchange, n) -> error(exchange, 500, "InternalError");
        for (int call = 0; call < 40; call++) {
            assertThrows(S3Exception.class, () -> store.read("blob"));
        }
        assertEquals(400, requests.size());
        requests.clear();
        assertThrows(S3Exception.class, () -> store.read("blob"));
        assertEquals(10, requests.size(), "call 41 still gets every attempt");
    }

    @Test
    void sdkTimeoutsNeverCutOffAStreamedBody() throws Exception {
        var tight = new SourceRetryConfig(10, Duration.ofMillis(1), Duration.ofMillis(5), Duration.ofMillis(1),
            Duration.ofMillis(200), Duration.ofMillis(200), Duration.ofMillis(400));
        byte[] blob = content(30 * 1024);
        responder = (exchange, n) -> {
            exchange.getResponseHeaders().add("ETag", "\"stub-etag\"");
            exchange.sendResponseHeaders(200, blob.length);
            try (OutputStream out = exchange.getResponseBody()) {
                for (int offset = 0; offset < blob.length; offset += 1024) {
                    out.write(blob, offset, 1024);
                    out.flush();
                    sleep(40);
                }
            }
        };
        try (var slow = store(tight); var in = slow.read("slow")) {
            long started = System.nanoTime();
            assertArrayEquals(blob, in.readAllBytes());
            assertTrue((System.nanoTime() - started) / 1_000_000 > 3 * 200, "body must outlast the attempt timeout threefold");
        }
        assertEquals(1, requests.size());
    }

    @Test
    void listingRetriesAFailedPageAndReturnsEveryKey() {
        responder = (exchange, n) -> {
            String query = exchange.getRequestURI().getRawQuery();
            if (query == null || !query.contains("continuation-token")) {
                listing(exchange, "true", "<NextContinuationToken>page2</NextContinuationToken>",
                    "repo/indices/a", "repo/indices/b");
            } else if (n == 2) {
                error(exchange, 500, "InternalError");
            } else {
                listing(exchange, "false", "", "repo/indices/c");
            }
        };
        assertEquals(List.of("indices/a", "indices/b", "indices/c"), store.list("indices/"));
        assertEquals(3, requests.size());
    }

    @Test
    void aStoreIgnoringRangeStillServesTheRangeButCannotPinIt() throws IOException {
        byte[] blob = content(10_000);
        responder = (exchange, n) -> ok(exchange, blob);
        try (var range = store.openRange("blob", 1_000, 500, null)) {
            assertArrayEquals(Arrays.copyOfRange(blob, 1_000, 1_500), range.in().readAllBytes());
            assertNull(range.version());
            assertEquals(blob.length, range.objectLength());
        }
        var pinned = assertThrows(IOException.class, () -> store.openRange("blob", 1_000, 500, "\"stub-etag\""));
        assertEquals("object store did not honour the byte range", pinned.getMessage());
        var outside = assertThrows(IOException.class, () -> store.openRange("blob", 9_900, 200, null));
        assertEquals(BlobStore.RANGE_OUTSIDE_BLOB, outside.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "W/\"weak\""})
    void aRangeWithoutAStrongEtagIsServedUnpinned(String eTag) throws IOException {
        byte[] blob = content(10_000);
        responder = (exchange, n) -> partial(exchange, blob, eTag.isEmpty() ? null : eTag);
        try (var range = store.openRange("blob", 2_000, 3_000, null)) {
            assertArrayEquals(Arrays.copyOfRange(blob, 2_000, 5_000), range.in().readAllBytes());
            assertNull(range.version());
            assertEquals(blob.length, range.objectLength());
        }
    }

    /** 1.0.0 read these blobs whole; ranged download must not turn them into quarantined shards. */
    @ParameterizedTest
    @ValueSource(strings = {"ignores-range", "no-etag", "weak-etag"})
    void shardsFromStoresThatCannotPinRangesAreDownloadedWhole(String behaviour, @TempDir Path tmp) throws IOException {
        byte[] content = LuceneFooterVerifierTest.luceneFile(5_000, 7);
        responder = (exchange, n) -> {
            boolean ranged = exchange.getRequestHeaders().containsKey("Range");
            if (behaviour.equals("ignores-range") || !ranged) ok(exchange, content);
            else if (behaviour.equals("no-etag")) partial(exchange, content, null);
            else if (exchange.getRequestHeaders().containsKey("If-Match")) error(exchange, 412, "PreconditionFailed");
            else partial(exchange, content, "W/\"weak\"");
        };
        var file = new RepoFileInfo("__big", "_0.fdt", content.length, LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null);
        var shard = new ShardSnapshot("repo", "", "snap", 1, content.length, List.of(file));
        new ShardMaterializer(store, 4, 1_000, 512).materialize(shard, tmp.resolve("out"));
        assertArrayEquals(content, Files.readAllBytes(tmp.resolve("out/_0.fdt")));
        assertEquals(2, requests.size(), "one unpinned first range, then the whole blob: " + requests);
        assertEquals(0, ifMatchRequests.get());
    }

    private static void listing(HttpExchange exchange, String truncated, String token, String... keys) throws IOException {
        var xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>bucket</Name>"
            + "<Prefix>repo/indices/</Prefix><KeyCount>" + keys.length + "</KeyCount><MaxKeys>1000</MaxKeys>"
            + "<IsTruncated>" + truncated + "</IsTruncated>" + token);
        for (String key : keys) xml.append("<Contents><Key>").append(key).append("</Key><Size>1</Size></Contents>");
        xml.append("</ListBucketResult>");
        byte[] body = xml.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/xml");
        exchange.sendResponseHeaders(200, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    private static void sleep(long millis) throws IOException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
