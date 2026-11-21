package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Version-pinned byte ranges against MinIO, in a bucket of its own. Skipped without MinIO. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3RangedReadTest {

    private static final String ENDPOINT = "http://localhost:9000";

    private final String bucket = "gs-range-test-" + UUID.randomUUID();
    private S3BlobStore store;
    private boolean bucketCreated;

    @BeforeAll
    void start() {
        store = S3BlobStore.minio(ENDPOINT, bucket, "repo", "minioadmin", "minioadmin");
        try (var admin = admin()) {
            admin.createBucket(b -> b.bucket(bucket));
            bucketCreated = true;
        } catch (Exception e) {
            // MinIO not reachable: every test is skipped
        }
    }

    @AfterAll
    void cleanup() {
        if (bucketCreated) deleteBucket(bucket);
        if (store != null) store.close();
    }

    static S3Client admin() {
        return S3Client.builder()
            .endpointOverride(URI.create(ENDPOINT))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("minioadmin", "minioadmin")))
            .forcePathStyle(true)
            .build();
    }

    static void deleteBucket(String bucket) {
        try (var admin = admin()) {
            admin.listObjectsV2Paginator(b -> b.bucket(bucket)).contents()
                .forEach(o -> admin.deleteObject(b -> b.bucket(bucket).key(o.key())));
            admin.deleteBucket(b -> b.bucket(bucket));
        }
    }

    private byte[] blob(String key, int length, long seed) {
        assumeTrue(bucketCreated, "MinIO not reachable");
        byte[] content = new byte[length];
        new SplittableRandom(seed).nextBytes(content);
        store.put(key, content);
        return content;
    }

    @Test
    void rangesReturnExactBytesPinnedToTheObjectsEtag() throws IOException {
        byte[] content = blob("big.bin", 5 * 1024 * 1024 + 3, 1);
        String version;
        try (var first = store.openRange("big.bin", 0, 1024, null)) {
            assertArrayEquals(Arrays.copyOfRange(content, 0, 1024), first.in().readAllBytes());
            assertEquals(content.length, first.objectLength());
            version = first.version();
        }
        assertNotNull(version);
        try (var middle = store.openRange("big.bin", 2_000_000, 1_500_000, version)) {
            assertArrayEquals(Arrays.copyOfRange(content, 2_000_000, 3_500_000), middle.in().readAllBytes());
            assertEquals(version, middle.version());
        }
        try (var last = store.openRange("big.bin", content.length - 1, 1, version)) {
            assertArrayEquals(new byte[]{content[content.length - 1]}, last.in().readAllBytes());
        }
    }

    @Test
    void rangesOutsideTheObjectAreErrors() {
        byte[] content = blob("small.bin", 100, 2);
        var pastEnd = assertThrows(IOException.class, () -> BlobStoreContractTest.readRange(store, "small.bin", 100, 1, null));
        assertEquals(BlobStore.RANGE_OUTSIDE_BLOB, pastEnd.getMessage());
        var overlapping = assertThrows(IOException.class, () -> BlobStoreContractTest.readRange(store, "small.bin", 90, 20, null));
        assertEquals(BlobStore.RANGE_OUTSIDE_BLOB, overlapping.getMessage());
        assertEquals(100, content.length);
    }

    @Test
    void anObjectOverwrittenBetweenTwoRangesIsAnError() throws IOException {
        blob("changing.bin", 4096, 3);
        String version;
        try (var first = store.openRange("changing.bin", 0, 2048, null)) {
            first.in().readAllBytes();
            version = first.version();
        }
        blob("changing.bin", 4096, 4);
        var failure = assertThrows(IOException.class, () -> BlobStoreContractTest.readRange(store, "changing.bin", 2048, 2048, version));
        assertEquals(BlobStore.BLOB_CHANGED, failure.getMessage());
    }

    @Test
    void aShardDownloadedInRangesIsByteIdenticalToASequentialDownload(@TempDir Path tmp) throws IOException {
        assumeTrue(bucketCreated, "MinIO not reachable");
        byte[] large = LuceneFooterVerifierTest.luceneFile(1_700_003, 5);
        byte[] small = LuceneFooterVerifierTest.luceneFile(5_000, 6);
        var parted = new RepoFileInfo("__parted", "_0.fdt", large.length, LuceneFooterVerifierTest.checksumOf(large), "10.3.2", null, 600_000);
        var single = new RepoFileInfo("__single", "_0.fdx", small.length, LuceneFooterVerifierTest.checksumOf(small), "10.3.2", null);
        for (int part = 0; part < parted.partCount(); part++) {
            int from = part * 600_000;
            store.put("indices/i/0/" + parted.partName(part), Arrays.copyOfRange(large, from, Math.min(large.length, from + 600_000)));
        }
        store.put("indices/i/0/__single", small);
        var shard = new ShardSnapshot("repo", "indices/i/0/", "snap", 1, large.length + small.length, List.of(parted, single));

        new ShardMaterializer(store, 8, 256 * 1024, 100_000).materialize(shard, tmp.resolve("ranged"));
        new ShardMaterializer(store, 1).materialize(shard, tmp.resolve("sequential"));
        for (String dir : List.of("ranged", "sequential")) {
            assertArrayEquals(large, Files.readAllBytes(tmp.resolve(dir).resolve("_0.fdt")), dir);
            assertArrayEquals(small, Files.readAllBytes(tmp.resolve(dir).resolve("_0.fdx")), dir);
        }
    }

    @Test
    void aMissingObjectIsBlobNotFound() {
        assumeTrue(bucketCreated, "MinIO not reachable");
        var failure = assertThrows(IOException.class, () -> store.openRange("absent.bin", 0, 1, null));
        assertTrue(failure.getMessage().startsWith("Blob not found"));
    }
}
