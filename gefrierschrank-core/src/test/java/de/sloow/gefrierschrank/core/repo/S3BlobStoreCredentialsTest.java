package de.sloow.gefrierschrank.core.repo;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/** Credentials are resolved per request from the store's own provider, which the store closes. */
class S3BlobStoreCredentialsTest {

    private static final Pattern CREDENTIAL = Pattern.compile("Credential=([^/]+)/");

    private HttpServer server;
    private ExecutorService executor;
    private final List<String> authorizations = Collections.synchronizedList(new ArrayList<>());
    private final List<String> tokens = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean expiredToken;

    /** Hands out key-N/token-N for the current generation, like an STS role provider after a refresh. */
    static final class RotatingProvider implements AwsCredentialsProvider, SdkAutoCloseable {
        final AtomicInteger generation = new AtomicInteger(1);
        final AtomicInteger closed = new AtomicInteger();

        @Override
        public AwsCredentials resolveCredentials() {
            int n = generation.get();
            return AwsSessionCredentials.create("key-" + n, "secret-" + n, "token-" + n);
        }

        @Override
        public void close() {
            closed.incrementAndGet();
        }
    }

    @BeforeEach
    void startStub() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        server.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            var matcher = CREDENTIAL.matcher(String.valueOf(exchange.getRequestHeaders().getFirst("Authorization")));
            authorizations.add(matcher.find() ? matcher.group(1) : "none");
            tokens.add(String.valueOf(exchange.getRequestHeaders().getFirst("X-Amz-Security-Token")));
            byte[] body;
            int status;
            if (expiredToken) {
                status = 400;
                body = "<Error><Code>ExpiredToken</Code><Message>stub</Message></Error>".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
            } else {
                status = 200;
                body = "blob".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("ETag", "\"e\"");
            }
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void stopStub() {
        server.stop(0);
        executor.shutdownNow();
    }

    private S3BlobStore store(AwsCredentialsProvider provider) {
        return S3BlobStore.s3("http://127.0.0.1:" + server.getAddress().getPort(), "bucket", "",
            provider, "us-east-1", S3BlobStoreRetryTest.FAST);
    }

    @Test
    void everyRequestIsSignedWithTheProvidersCurrentCredentials() throws IOException {
        var provider = new RotatingProvider();
        try (var store = store(provider)) {
            store.read("a").readAllBytes();
            provider.generation.set(2);
            store.read("b").readAllBytes();
        }
        assertEquals(List.of("key-1", "key-2"), authorizations);
        assertEquals(List.of("token-1", "token-2"), tokens);
    }

    /** SDK 2.31 clients also close a supplied provider; the store does not rely on that. */
    @Test
    void theStoreClosesTheProviderItOwns() {
        var provider = new RotatingProvider();
        var store = store(provider);
        assertEquals(0, provider.closed.get());
        store.close();
        assertTrue(provider.closed.get() >= 1);
    }

    @Test
    void anExpiredTokenIsNotRetriedWithTheSameCredentials() {
        expiredToken = true;
        var provider = new RotatingProvider();
        try (var store = store(provider)) {
            var failure = assertThrows(S3Exception.class, () -> store.read("a"));
            assertEquals(400, failure.statusCode());
        }
        assertEquals(1, authorizations.size(), "retrying would re-sign with the same expired token");
    }

    @Test
    void thePublicFactoryUsesTheProviderToo() throws IOException {
        var provider = new RotatingProvider();
        try (var store = S3BlobStore.s3("http://127.0.0.1:" + server.getAddress().getPort(), "bucket", "",
                provider, "us-east-1")) {
            store.read("a").readAllBytes();
        }
        assertEquals(List.of("key-1"), authorizations);
        assertTrue(provider.closed.get() >= 1);
    }
}
