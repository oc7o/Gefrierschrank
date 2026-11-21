package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Mid-body resets and stalls between the source client and MinIO, injected by a TCP proxy, over
 * a matrix of operations and faults: whole-blob and range reads, and shard
 * materialization at the default concurrency, sequentially and in ranges, each with one reset,
 * three resets, a storm that resets every open connection at once, and (sequentially) a stall.
 * Every configuration must materialize byte-identical, CRC-verified files while the proxy
 * carries about the shard size, and a blob replaced before the continuation must fail with
 * {@link BlobStore#BLOB_CHANGED}. Skipped without MinIO; holds the full slot.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3BodyResumeMinioTest {

    private static final String ENDPOINT = "http://localhost:9000";
    private static final String PREFIX = "indices/i/0/";
    private static final long RESET_AFTER = 2L << 20;
    /**
     * The proxy occasionally fails to deliver its RST (a race between its two pump threads), and
     * the client then sees a stall instead, which the store also resumes after the read timeout.
     * A short timeout keeps that case from costing the SDK's 30 s.
     */
    private static final Duration SOCKET_TIMEOUT = Duration.ofSeconds(3);

    private final String bucket = "gs-resume-test-" + UUID.randomUUID();
    private S3BlobStore direct;
    private boolean bucketCreated;
    private ShardSnapshot shard;
    private RepoFileInfo biggest;
    private Map<String, String> reference;

    @BeforeAll
    void start(@TempDir Path tmp) throws Exception {
        direct = S3BlobStore.minio(ENDPOINT, bucket, "", "minioadmin", "minioadmin");
        try (var admin = S3RangedReadTest.admin()) {
            admin.createBucket(b -> b.bucket(bucket));
            bucketCreated = true;
        } catch (Exception e) {
            return;
        }
        var random = new SplittableRandom(2026);
        var files = new ArrayList<RepoFileInfo>();
        long total = 0;
        for (int i = 0; i < 14; i++) {
            int length = i == 0 ? 12_000_003 : i == 1 ? 6_500_001 : random.nextInt(20_000, 1_500_000);
            byte[] content = LuceneFooterVerifierTest.luceneFile(length, 900 + i);
            long partSize = i == 1 ? 2_500_000 : 0;
            var file = new RepoFileInfo("__m" + i, "_" + i + (i == 0 ? ".cfs" : ".fdt"), length,
                LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null, partSize);
            long size = file.partCount() == 1 ? length : partSize;
            for (int part = 0; part < file.partCount(); part++) {
                int from = (int) (part * size);
                direct.put(PREFIX + file.partName(part), Arrays.copyOfRange(content, from, (int) Math.min(length, from + size)));
            }
            files.add(file);
            total += length;
        }
        shard = new ShardSnapshot("repo", PREFIX, "snap", 1, total, files);
        biggest = files.get(0);
        Path dir = tmp.resolve("reference");
        new ShardMaterializer(direct, 1).materialize(shard, dir);
        reference = digests(dir);
    }

    @AfterAll
    void cleanup() {
        if (bucketCreated) S3RangedReadTest.deleteBucket(bucket);
        if (direct != null) direct.close();
    }

    private S3BlobStore through(ToxicProxy proxy, Duration socketTimeout) {
        return S3BlobStore.s3(proxy.endpoint(), bucket, "", StaticCredentialsProvider.create(
                AwsBasicCredentials.create("minioadmin", "minioadmin")), "us-east-1", SourceRetryConfig.DEFAULT,
            S3BlobStore.BodyResume.DEFAULT, socketTimeout);
    }

    private static ToxicProxy proxy(int resets, boolean storm) throws IOException {
        return new ToxicProxy("127.0.0.1", 9000, RESET_AFTER, resets, storm);
    }

    @ParameterizedTest(name = "{0} {1}")
    @CsvSource({
        "default, 1 reset", "default, 3 resets", "default, storm",
        "c1, 1 reset", "c1, 3 resets", "c1, storm", "c1, stall",
        "ranged, 1 reset", "ranged, 3 resets", "ranged, storm"})
    void aShardMaterializesByteIdenticalThroughMidBodyFaults(String config, String fault, @TempDir Path tmp) throws Exception {
        assumeTrue(bucketCreated, "MinIO not reachable");
        int resets = fault.equals("3 resets") ? 3 : 1;
        try (var proxy = proxy(resets, fault.equals("storm"))) {
            proxy.stall = fault.equals("stall");
            try (var store = through(proxy, SOCKET_TIMEOUT)) {
                var materializer = switch (config) {
                    case "default" -> new ShardMaterializer(store, ShardMaterializer.DEFAULT_CONCURRENCY);
                    case "c1" -> new ShardMaterializer(store, 1);
                    default -> new ShardMaterializer(store, 8, 4L << 20, 1 << 20);
                };
                Path dir = tmp.resolve("out");
                materializer.materialize(shard, dir);
                assertEquals(reference, digests(dir), proxy.stats());
                assertTrue(proxy.resetsDone.get() >= 1, proxy.stats());
                assertTrue(store.bodyResumes() >= 1, "resumes: " + store.bodyResumes());
            }
            assertTrue(proxy.bytesDown.get() < shard.totalSize() * 105 / 100 + (2L << 20),
                "nothing is downloaded twice: " + proxy.stats() + " for " + shard.totalSize() + " shard bytes");
        }
    }

    @Test
    void wholeBlobAndRangeReadsResumeAfterAReset() throws Exception {
        assumeTrue(bucketCreated, "MinIO not reachable");
        byte[] expected;
        try (InputStream in = direct.read(PREFIX + biggest.partName(0))) {
            expected = in.readAllBytes();
        }
        try (var proxy = proxy(1, false); var store = through(proxy, SOCKET_TIMEOUT)) {
            try (InputStream in = store.read(PREFIX + biggest.partName(0))) {
                assertArrayEquals(expected, in.readAllBytes());
            }
            assertEquals(1, store.bodyResumes(), proxy.stats());
        }
        try (var proxy = proxy(1, false); var store = through(proxy, SOCKET_TIMEOUT)) {
            try (var range = store.openRange(PREFIX + biggest.partName(0), 1_000, expected.length - 2_000, null)) {
                assertArrayEquals(Arrays.copyOfRange(expected, 1_000, expected.length - 1_000), range.in().readAllBytes());
            }
            assertEquals(1, store.bodyResumes(), proxy.stats());
        }
    }

    @Test
    void aBlobReplacedBeforeTheContinuationIsBlobChanged() throws Exception {
        assumeTrue(bucketCreated, "MinIO not reachable");
        String key = "scratch/changing-blob";
        byte[] v1 = new byte[8 << 20];
        new SplittableRandom(1).nextBytes(v1);
        byte[] v2 = v1.clone();
        v2[v2.length - 1] ^= 1;
        direct.put(key, v1);
        try (var proxy = new ToxicProxy("127.0.0.1", 9000, 4L << 20, 1, false); var store = through(proxy, SOCKET_TIMEOUT)) {
            try (InputStream in = store.read(key)) {
                in.readNBytes(1 << 20);
                direct.put(key, v2);
                var failure = assertThrows(IOException.class, in::readAllBytes);
                assertEquals(BlobStore.BLOB_CHANGED, failure.getMessage(), proxy.stats());
            }
        }
    }

    static Map<String, String> digests(Path dir) throws Exception {
        var result = new TreeMap<String, String>();
        try (var list = Files.list(dir)) {
            for (Path file : list.toList()) {
                byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file));
                result.put(file.getFileName().toString(), HexFormat.of().formatHex(digest));
            }
        }
        return result;
    }
}
