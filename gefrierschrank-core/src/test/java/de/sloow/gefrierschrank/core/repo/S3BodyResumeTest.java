package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A GET body that breaks mid-way (reset, premature close, stall) is continued from the first
 * byte not yet delivered, pinned to the ETag of the first response, for whole-blob reads and for
 * byte ranges, against a socket-level stub that injects the faults.
 */
class S3BodyResumeTest {

    static final S3BlobStore.BodyResume FAST_RESUME =
        new S3BlobStore.BodyResume(S3BlobStore.MAX_BODY_RESUMES, Duration.ofMillis(2), Duration.ofMillis(20));
    static final Duration SOCKET_TIMEOUT = Duration.ofMillis(500);
    static final String KEY = "indices/x/0/__blob";
    static final int LENGTH = 3_000_000;

    private FaultyS3Stub stub;
    private S3BlobStore store;
    private byte[] blob;

    @BeforeEach
    void start() throws IOException {
        stub = new FaultyS3Stub();
        store = stub.store(FAST_RESUME, SOCKET_TIMEOUT);
        blob = content(LENGTH, 1);
        stub.put(KEY, blob, "v1");
    }

    @AfterEach
    void stop() throws IOException {
        store.close();
        stub.close();
    }

    static byte[] content(int length, long seed) {
        byte[] bytes = new byte[length];
        new SplittableRandom(seed).nextBytes(bytes);
        return bytes;
    }

    /** Faults the first {@code times} responses, each after a different number of body bytes. */
    private void faultFirst(int times, FaultyS3Stub.Fault fault) {
        stub.plan = (request, bodyLength) -> request.number() <= times
            ? new FaultyS3Stub.Injection(fault, 50_000 + 20_011L * request.number()) : null;
    }

    private byte[] readWhole() throws IOException {
        try (InputStream in = store.read(KEY)) {
            return in.readAllBytes();
        }
    }

    private byte[] readRange(long offset, long length) throws IOException {
        try (var range = store.openRange(KEY, offset, length, null)) {
            return range.in().readAllBytes();
        }
    }

    @ParameterizedTest
    @CsvSource({"RESET,1", "RESET,3", "CLOSE,1", "CLOSE,3", "STALL,1", "STALL,3"})
    void aBrokenWholeBlobBodyContinuesAtTheExactOffsetPinnedToItsEtag(FaultyS3Stub.Fault fault, int times) throws IOException {
        faultFirst(times, fault);
        assertArrayEquals(blob, readWhole());
        assertContinuations(0, LENGTH, times, fault);
    }

    @ParameterizedTest
    @CsvSource({"RESET,1", "RESET,3", "CLOSE,1", "CLOSE,3", "STALL,1", "STALL,3"})
    void aBrokenRangeBodyContinuesAtTheExactOffsetPinnedToItsEtag(FaultyS3Stub.Fault fault, int times) throws IOException {
        faultFirst(times, fault);
        long offset = 123_457;
        long length = 2_500_000;
        assertArrayEquals(Arrays.copyOfRange(blob, (int) offset, (int) (offset + length)), readRange(offset, length));
        assertContinuations(offset, length, times, fault);
    }

    /**
     * The first request has no If-Match; each continuation asks for the rest of the body from
     * where the previous response broke off, with If-Match on the first response's ETag. A reset
     * discards whatever the client had not read from its socket yet, so after a reset the
     * continuation may start earlier than the bytes sent; the content check proves the splice.
     */
    private void assertContinuations(long start, long length, int times, FaultyS3Stub.Fault fault) {
        List<FaultyS3Stub.Request> requests = stub.requests();
        assertEquals(times + 1, requests.size(), requests.toString());
        assertEquals(times, store.bodyResumes());
        assertNull(requests.get(0).ifMatch());
        long end = start + length - 1;
        long previous = start;
        List<Long> offsets = stub.faultOffsets();
        for (int i = 1; i <= times; i++) {
            long sent = previous + offsets.get(i - 1);
            var continuation = requests.get(i);
            assertEquals("\"v1\"", continuation.ifMatch(), continuation.toString());
            String range = continuation.range();
            assertTrue(range.startsWith("bytes=") && range.endsWith("-" + end), continuation.toString());
            long from = Long.parseLong(range.substring("bytes=".length(), range.indexOf('-')));
            if (fault == FaultyS3Stub.Fault.RESET) assertTrue(from > previous && from <= sent, continuation + " after " + sent);
            else assertEquals(sent, from, continuation.toString());
            previous = from;
        }
    }

    @Test
    void singleByteReadsAcrossAResumeDeliverEveryByteOnce() throws IOException {
        faultFirst(2, FaultyS3Stub.Fault.CLOSE);
        int prefix = 200_000;
        try (InputStream in = store.read(KEY)) {
            byte[] head = new byte[prefix];
            for (int i = 0; i < prefix; i++) {
                int b = in.read();
                assertTrue(b >= 0, "early end at " + i);
                head[i] = (byte) b;
                assertTrue(in.available() >= 0);
            }
            assertArrayEquals(Arrays.copyOf(blob, prefix), head);
            assertArrayEquals(Arrays.copyOfRange(blob, prefix, LENGTH), in.readAllBytes());
            assertEquals(-1, in.read());
        }
        assertEquals(2, store.bodyResumes());
    }

    @Test
    void aBlobReplacedBeforeTheContinuationIsBlobChanged() {
        stub.plan = (request, bodyLength) -> {
            if (request.number() != 1) return null;
            stub.put(KEY, content(LENGTH, 2), "v2");
            return new FaultyS3Stub.Injection(FaultyS3Stub.Fault.RESET, 500_000);
        };
        var failure = assertThrows(IOException.class, this::readWhole);
        assertEquals(BlobStore.BLOB_CHANGED, failure.getMessage());
        assertEquals(2, stub.requests().size());
        assertEquals("\"v1\"", stub.requests().get(1).ifMatch());
    }

    @Test
    void aStoreIgnoringIfMatchStillCannotSpliceTwoVersions() {
        stub.ignoresIfMatch = true;
        stub.plan = (request, bodyLength) -> {
            if (request.number() != 1) return null;
            stub.put(KEY, content(LENGTH, 3), "v2");
            return new FaultyS3Stub.Injection(FaultyS3Stub.Fault.CLOSE, 500_000);
        };
        var failure = assertThrows(IOException.class, () -> readRange(10, 2_000_000));
        assertEquals(BlobStore.BLOB_CHANGED, failure.getMessage());
    }

    @ParameterizedTest
    @EnumSource(value = FaultyS3Stub.ETagMode.class, names = {"WEAK", "NONE"})
    void withoutAStrongEtagABrokenBodyFailsAsBefore(FaultyS3Stub.ETagMode mode) {
        stub.eTagMode = mode;
        faultFirst(1, FaultyS3Stub.Fault.CLOSE);
        assertThrows(IOException.class, this::readWhole);
        assertEquals(1, stub.requests().size());
        assertEquals(0, store.bodyResumes());

        faultFirst(2, FaultyS3Stub.Fault.CLOSE);
        assertThrows(IOException.class, () -> readRange(0, 2_000_000));
        assertEquals(2, stub.requests().size(), "no continuation: " + stub.requests());
    }

    @Test
    void theSixthFailureOfOneBodyFailsTheRead() {
        faultFirst(S3BlobStore.MAX_BODY_RESUMES + 1, FaultyS3Stub.Fault.RESET);
        assertThrows(IOException.class, this::readWhole);
        assertEquals(S3BlobStore.MAX_BODY_RESUMES + 1, stub.requests().size());
        assertEquals(S3BlobStore.MAX_BODY_RESUMES, store.bodyResumes());
    }

    @Test
    void fiveFailuresOfOneBodyAreStillAbsorbed() throws IOException {
        faultFirst(S3BlobStore.MAX_BODY_RESUMES, FaultyS3Stub.Fault.CLOSE);
        assertArrayEquals(blob, readWhole());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closingDuringTheBackoffMakesNoFurtherRequestAndLeavesNoConnection(boolean ranged) throws Exception {
        try (var slow = stub.store(new S3BlobStore.BodyResume(5, Duration.ofSeconds(4), Duration.ofSeconds(4)), SOCKET_TIMEOUT)) {
            faultFirst(1, FaultyS3Stub.Fault.RESET);
            InputStream in = ranged ? slow.openRange(KEY, 0, LENGTH, null).in() : slow.read(KEY);
            var reading = CompletableFuture.supplyAsync(() -> {
                try {
                    return in.readAllBytes();
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
            awaitResumeStarted(slow);
            long closedAt = System.nanoTime();
            in.close();
            var failure = assertThrows(ExecutionException.class, () -> reading.get(1, TimeUnit.SECONDS));
            assertInstanceOf(IOException.class, failure.getCause().getCause());
            assertTrue((System.nanoTime() - closedAt) / 1_000_000 < 1_000, "close wakes the backoff");
            Thread.sleep(4_500);
            assertEquals(1, stub.requests().size(), stub.requests().toString());
            assertEquals(0, stub.activeConnections.get());
        }
    }

    @Test
    void anInterruptDuringTheBackoffIsNeverRetried() throws Exception {
        try (var slow = stub.store(new S3BlobStore.BodyResume(5, Duration.ofSeconds(4), Duration.ofSeconds(4)), SOCKET_TIMEOUT)) {
            faultFirst(1, FaultyS3Stub.Fault.CLOSE);
            var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            var interruptedAfter = new AtomicBoolean();
            Thread reader = new Thread(() -> {
                try (InputStream in = slow.read(KEY)) {
                    in.readAllBytes();
                } catch (Throwable e) {
                    failure.set(e);
                }
                interruptedAfter.set(Thread.currentThread().isInterrupted());
            });
            reader.start();
            awaitResumeStarted(slow);
            reader.interrupt();
            reader.join(1_000);
            assertFalse(reader.isAlive(), "the interrupt ends the backoff");
            assertInstanceOf(IOException.class, failure.get());
            assertTrue(interruptedAfter.get(), "the interrupt status is kept");
            Thread.sleep(4_500);
            assertEquals(1, stub.requests().size());
        }
    }

    @Test
    void anInterruptedStalledReadIsNeverRetried() throws Exception {
        faultFirst(1, FaultyS3Stub.Fault.STALL);
        var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        Thread reader = new Thread(() -> {
            try (InputStream in = store.read(KEY)) {
                in.readAllBytes();
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        reader.start();
        while (stub.faultsInjected.get() == 0) Thread.sleep(5);
        Thread.sleep(100);
        reader.interrupt();
        reader.join(5_000);
        assertFalse(reader.isAlive());
        assertNotNull(failure.get());
        assertEquals(0, store.bodyResumes());
        assertEquals(1, stub.requests().size());
    }

    /** A read timeout is an InterruptedIOException, yet it means a stalled connection, not an interrupt. */
    @Test
    void aReadTimeoutIsResumed() throws IOException {
        faultFirst(1, FaultyS3Stub.Fault.STALL);
        long started = System.nanoTime();
        assertArrayEquals(blob, readWhole());
        assertTrue((System.nanoTime() - started) / 1_000_000 >= SOCKET_TIMEOUT.toMillis(), "resumed after the read timeout");
        assertEquals(1, store.bodyResumes());
    }

    @Test
    void anUnbrokenBodyIsOneRequestWithoutIfMatch() throws IOException {
        assertArrayEquals(blob, readWhole());
        assertArrayEquals(Arrays.copyOfRange(blob, 5, 105), readRange(5, 100));
        assertEquals(2, stub.requests().size());
        assertTrue(stub.requests().stream().allMatch(r -> r.ifMatch() == null));
        assertEquals(0, store.bodyResumes());
        assertEquals(LENGTH + 100, stub.bodyBytesSent.get());
    }

    @Test
    void theMetadataReadsOfTheRepositoryAreCoveredToo() throws IOException {
        byte[] indexN = content(200_000, 9);
        stub.put("index-7", indexN, "idx");
        var counter = new AtomicInteger();
        stub.plan = (request, bodyLength) -> counter.incrementAndGet() == 1
            ? new FaultyS3Stub.Injection(FaultyS3Stub.Fault.RESET, 50_000) : null;
        try (InputStream in = store.read("index-7")) {
            assertArrayEquals(indexN, in.readAllBytes());
        }
        assertEquals(1, store.bodyResumes());
    }

    private void awaitResumeStarted(S3BlobStore store) throws InterruptedException {
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (store.bodyResumes() == 0 && System.nanoTime() < deadline) Thread.sleep(5);
        assertEquals(1, store.bodyResumes(), "the body failed and entered its backoff");
        Thread.sleep(100);
    }
}
