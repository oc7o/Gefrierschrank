package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * info, shards and files on s3:// repositories (MinIO, skipped without it; hold the full slot).
 * demo-repo holds snapshots taken by ES 7.17.9, 8.17.0 and 9.3.0; frozen-repo a mounted index.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InspectCommandsS3Test {

    private static final String ENDPOINT = "http://localhost:9000";
    private static final Path CORE = SurveyCommandTest.CORE_FIXTURES;
    private static final Map<String, String> MINIO_KEYS = Map.of("AWS_ACCESS_KEY_ID", "minioadmin",
        "AWS_SECRET_ACCESS_KEY", "minioadmin", "AWS_REGION", "us-east-1");

    private final String bucket = "gs-catalog-test-" + UUID.randomUUID();
    private boolean bucketCreated;

    @BeforeAll
    void upload() {
        try (var admin = admin()) {
            admin.createBucket(b -> b.bucket(bucket));
            bucketCreated = true;
            for (String repository : List.of("demo-repo", "frozen-repo")) {
                Path root = CORE.resolve(repository);
                try (var files = Files.walk(root)) {
                    for (Path file : files.filter(Files::isRegularFile).toList()) {
                        String key = repository + "/" + root.relativize(file).toString().replace('\\', '/');
                        admin.putObject(b -> b.bucket(bucket).key(key), RequestBody.fromFile(file));
                    }
                }
            }
        } catch (Exception e) {
            // MinIO not reachable: the S3 tests are skipped.
        }
    }

    @AfterAll
    void deleteBucket() {
        if (!bucketCreated) return;
        try (var admin = admin()) {
            admin.listObjectsV2Paginator(b -> b.bucket(bucket)).contents()
                .forEach(o -> admin.deleteObject(b -> b.bucket(bucket).key(o.key())));
            admin.deleteBucket(b -> b.bucket(bucket));
        }
    }

    @Test
    void infoOnS3PrintsIndexVersionsAndStateNamesAndClosesTheStore() {
        assumeTrue(bucketCreated, "MinIO not reachable");
        var closed = new boolean[1];
        var command = new InfoCommand() {
            @Override
            BlobStore openStore() {
                return closeTracking(super.openStore(), closed);
            }
        };
        command.environment = MINIO_KEYS::get;

        var result = SurveyCommandTest.execute(command, "s3://" + bucket + "/demo-repo", "--source-endpoint", ENDPOINT);

        assertEquals(0, result.exitCode(), result.err());
        assertTrue(closed[0], "the S3 store must be closed");
        assertInfo(result.out());
    }

    @Test
    void shardsOnS3NamesTheBackingOfAMountAndFilesListsAShard() {
        assumeTrue(bucketCreated, "MinIO not reachable");
        var closed = new boolean[2];
        var shards = new ShardsCommand() {
            @Override
            BlobStore openStore() {
                return closeTracking(super.openStore(), closed, 0);
            }
        };
        shards.environment = MINIO_KEYS::get;
        var mount = SurveyCommandTest.execute(shards, "s3://" + bucket + "/frozen-repo", "partial-frozen-docs-f5ffca09",
            "--endpoint", ENDPOINT);
        assertEquals(0, mount.exitCode(), mount.err());
        assertTrue(mount.out().startsWith(
            "mounted: backing full-export-test-f5ffca09/-qDma32ZRT-aOibqhqfTMw/frozen-docs-f5ffca09\n"), mount.out());
        assertTrue(closed[0], "the S3 store must be closed");

        var files = new FilesCommand() {
            @Override
            BlobStore openStore() {
                return closeTracking(super.openStore(), closed, 1);
            }
        };
        files.environment = MINIO_KEYS::get;
        var shard = SurveyCommandTest.execute(files, "s3://" + bucket + "/demo-repo", "test_data_7_17_9", "0",
            "--source-endpoint", ENDPOINT);
        assertEquals(0, shard.exitCode(), shard.err());
        assertTrue(shard.out().contains("_0.cfs"), shard.out());
        assertTrue(!shard.out().contains("mounted:"), shard.out());
        assertTrue(closed[1], "the S3 store must be closed");
    }

    @Test
    void s3ErrorsNameTheExceptionClass() {
        assumeTrue(bucketCreated, "MinIO not reachable");
        var command = new InfoCommand();
        command.environment = MINIO_KEYS::get;
        var result = SurveyCommandTest.execute(command, "s3://" + bucket + "/no-repository-here", "--source-endpoint", ENDPOINT);
        assertEquals(1, result.exitCode());
        assertTrue(result.err().matches("(?s)error: \\w+: .*"), result.err());
    }

    /** The same output from a local path, so the format is covered without MinIO too. */
    @Test
    void localRepositoriesPrintTheSame() {
        var command = new InfoCommand() {
            @Override
            BlobStore openStore() {
                return new FileSystemBlobStore(CORE.resolve("demo-repo"));
            }
        };
        var result = SurveyCommandTest.capture(command);
        assertEquals(0, result.exitCode(), result.err());
        assertInfo(result.out());

        var shards = new ShardsCommand() {
            @Override
            BlobStore openStore() {
                return new FileSystemBlobStore(CORE.resolve("frozen-repo"));
            }
        };
        var mount = SurveyCommandTest.capture(shards, "partial-frozen-docs-f5ffca09");
        assertTrue(mount.out().startsWith("mounted: backing full-export-test-f5ffca09/"), mount.out());
        var original = SurveyCommandTest.capture(shards, "frozen-docs-f5ffca09");
        assertTrue(original.out().startsWith("SHARD"), original.out());
        var unknown = SurveyCommandTest.capture(shards, "nope");
        assertEquals(1, unknown.exitCode());
        assertEquals("error: IOException: Index not in repository: nope", unknown.err().strip());
    }

    /** shards and files diagnose broken indices: an unreadable meta-*.dat must not withhold the listing. */
    @Test
    void shardsAndFilesStillListAnIndexWhoseMetadataBlobIsMissing(@TempDir Path repo) throws IOException {
        SurveyCommandTest.copy(CORE.resolve("demo-repo"), repo);
        String indexId = new RepositoryInspector(new FileSystemBlobStore(repo)).index("test_data_8_17_0").repoUuid();
        try (var metas = Files.newDirectoryStream(repo.resolve("indices").resolve(indexId), "meta-*.dat")) {
            for (Path meta : metas) Files.delete(meta);
        }
        String note = "note: index metadata unreadable (NoSuchFileException), so whether the index is mounted is unknown";

        var shards = new ShardsCommand() {
            @Override
            BlobStore openStore() {
                return new FileSystemBlobStore(repo);
            }
        };
        var listing = SurveyCommandTest.capture(shards, "test_data_8_17_0");
        assertEquals(0, listing.exitCode(), listing.err());
        assertTrue(listing.out().startsWith("SHARD"), listing.out());
        assertTrue(listing.out().lines().anyMatch(l -> l.matches("0\\s+\\d+\\s+\\d+\\s+\\d+\\.\\d+\\.\\d+")), listing.out());
        assertEquals(note, listing.err().strip());

        var files = new FilesCommand() {
            @Override
            BlobStore openStore() {
                return new FileSystemBlobStore(repo);
            }
        };
        var shard = SurveyCommandTest.capture(files, "test_data_8_17_0", "0");
        assertEquals(0, shard.exitCode(), shard.err());
        assertTrue(shard.out().startsWith("Shard test_data_8_17_0/0"), shard.out());
        assertTrue(shard.out().contains("BLOB NAME"), shard.out());
        assertEquals(note, shard.err().strip());
    }

    private static void assertInfo(String out) {
        assertTrue(out.contains("SNAPSHOT"), out);
        assertTrue(line(out, "snap_8_17_0").matches(".*SUCCESS\\s+8\\.11\\.0\\s+8521000"), out);
        assertTrue(line(out, "snap_9_3_0").matches(".*SUCCESS\\s+8\\.11\\.0\\s+9060000"), out);
        assertTrue(line(out, "snap_7_17_9").matches(".*SUCCESS\\s+7\\.17\\.9\\s+-"), out);
    }

    private static String line(String out, String prefix) {
        return out.lines().filter(l -> l.startsWith(prefix + " ")).findFirst().orElseThrow().strip();
    }

    @Test
    void stateNamesFollowElasticsearchSnapshotState() {
        assertEquals(List.of("IN_PROGRESS", "SUCCESS", "FAILED", "PARTIAL", "INCOMPATIBLE", "7"),
            List.of(0, 1, 2, 3, 4, 7).stream().map(InfoCommand::stateName).toList());
    }

    private static BlobStore closeTracking(BlobStore delegate, boolean[] closed) {
        return closeTracking(delegate, closed, 0);
    }

    private static BlobStore closeTracking(BlobStore delegate, boolean[] closed, int slot) {
        return new BlobStore() {
            @Override
            public Optional<BlobMetadata> stat(String key) {
                return delegate.stat(key);
            }

            @Override
            public InputStream read(String key) throws IOException {
                return delegate.read(key);
            }

            @Override
            public List<String> list(String prefix) {
                return delegate.list(prefix);
            }

            @Override
            public List<BlobMetadata> listWithSizes(String prefix) {
                return delegate.listWithSizes(prefix);
            }

            @Override
            public void put(String key, byte[] content) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                closed[slot] = true;
                delegate.close();
            }
        };
    }

    private S3Client admin() {
        return S3Client.builder().endpointOverride(URI.create(ENDPOINT)).region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("minioadmin", "minioadmin")))
            .forcePathStyle(true).build();
    }
}
