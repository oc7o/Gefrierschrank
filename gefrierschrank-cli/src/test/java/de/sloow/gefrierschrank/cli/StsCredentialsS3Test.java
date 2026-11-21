package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.delta.DeltaTableWriter;
import de.sloow.gefrierschrank.delta.FullIndexExporter;
import de.sloow.gefrierschrank.delta.RunReportListener;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import io.delta.standalone.DeltaLog;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Temporary (STS AssumeRole) credentials from MinIO on both sides, and an ambient target
 * authenticated through a named profile, in a bucket of its own. Skipped without MinIO. Hold the
 * full slot: it shares the MinIO instance with other suites.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StsCredentialsS3Test {

    private static final String ENDPOINT = "http://localhost:9000";
    private static final Path FROZEN = Path.of("../gefrierschrank-core/src/test/resources/fixtures/frozen-repo");

    private final String bucket = "gs-sts-test-" + UUID.randomUUID();
    private Credentials sts;
    private boolean bucketCreated;

    @BeforeAll
    void setUp() throws Exception {
        try (var admin = admin()) {
            admin.createBucket(b -> b.bucket(bucket));
            bucketCreated = true;
            try (var files = Files.walk(FROZEN)) {
                for (Path file : files.filter(Files::isRegularFile).toList()) {
                    String key = "repo/" + FROZEN.relativize(file).toString().replace('\\', '/');
                    admin.putObject(b -> b.bucket(bucket).key(key), RequestBody.fromFile(file));
                }
            }
        } catch (Exception e) {
            return;
        }
        try (var client = StsClient.builder().region(Region.US_EAST_1).endpointOverride(URI.create(ENDPOINT))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("minioadmin", "minioadmin")))
                .build()) {
            sts = client.assumeRole(b -> b.roleArn("arn:aws:iam::000000000000:role/gefrierschrank-test")
                .roleSessionName("gefrierschrank-test").durationSeconds(900)).credentials();
        }
    }

    @AfterAll
    void tearDown() {
        if (!bucketCreated) return;
        try (var admin = admin()) {
            admin.listObjectsV2Paginator(b -> b.bucket(bucket)).contents()
                .forEach(o -> admin.deleteObject(b -> b.bucket(bucket).key(o.key())));
            admin.deleteBucket(b -> b.bucket(bucket));
        }
    }

    private static S3Client admin() {
        return S3Client.builder().endpointOverride(URI.create(ENDPOINT)).region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("minioadmin", "minioadmin")))
            .forcePathStyle(true).build();
    }

    /** A fresh S3A instance per test: the FileSystem cache would otherwise return the first configuration. */
    private static Configuration uncached(Configuration conf) {
        conf.setBoolean("fs.s3a.impl.disable.cache", true);
        return conf;
    }

    private Map<String, String> stsTarget() {
        return Map.of("TARGET_AWS_ACCESS_KEY_ID", sts.accessKeyId(), "TARGET_AWS_SECRET_ACCESS_KEY", sts.secretAccessKey(),
            "TARGET_AWS_SESSION_TOKEN", sts.sessionToken());
    }

    @Test
    void anS3aTargetWritesAndReadsWithTemporaryCredentials() throws Exception {
        assumeTrue(sts != null, "MinIO STS not reachable");
        String table = "s3a://" + bucket + "/delta/sts-" + UUID.randomUUID();
        var conf = uncached(DumpToDeltaCommand.s3aConf(table, ENDPOINT, stsTarget()::get));
        assertEquals(2, new DeltaTableWriter(table).write(conf, List.of("{\"a\":1}", "{\"a\":2}").iterator()));
        try (var rows = DeltaLog.forTable(conf, table).snapshot().open()) {
            int count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
            assertEquals(2, count);
        }
    }

    /**
     * S3A must accept the whole ambient provider list, and with TARGET_AWS_PROFILE the profile is
     * what authenticates: web identity and the instance role are unavailable here, and a wrong
     * secret in the profile fails the write.
     */
    @Test
    void anAmbientS3aTargetAuthenticatesThroughTheNamedProfile(@org.junit.jupiter.api.io.TempDir Path tmp) throws Exception {
        assumeTrue(bucketCreated, "MinIO not reachable");
        Path credentials = Files.writeString(tmp.resolve("credentials"),
            "[gs-target]\naws_access_key_id = minioadmin\naws_secret_access_key = minioadmin\n"
                + "[gs-wrong]\naws_access_key_id = minioadmin\naws_secret_access_key = not-the-secret\n");
        String table = "s3a://" + bucket + "/delta/profile-" + UUID.randomUUID();
        var conf = uncached(DumpToDeltaCommand.s3aConf(table, ENDPOINT, CredentialSources.Mode.AMBIENT,
            Map.of("TARGET_AWS_PROFILE", "gs-target")::get));
        conf.set("fs.s3a.auth.profile.file", credentials.toString());
        assertNull(conf.get("fs.s3a.access.key"));
        assertEquals(2, new DeltaTableWriter(table).write(conf, List.of("{\"a\":1}", "{\"a\":2}").iterator()));
        try (var rows = DeltaLog.forTable(conf, table).snapshot().open()) {
            int count = 0;
            while (rows.hasNext()) {
                rows.next();
                count++;
            }
            assertEquals(2, count);
        }

        var wrong = uncached(DumpToDeltaCommand.s3aConf(table + "-wrong", ENDPOINT, CredentialSources.Mode.AMBIENT,
            Map.of("TARGET_AWS_PROFILE", "gs-wrong")::get));
        wrong.set("fs.s3a.auth.profile.file", credentials.toString());
        wrong.set("fs.s3a.retry.limit", "1");
        wrong.set("fs.s3a.attempts.maximum", "1");
        assertThrows(java.nio.file.AccessDeniedException.class,
            () -> new DeltaTableWriter(table + "-wrong").write(wrong, List.of("{\"a\":1}").iterator()));
    }

    /** What 1.0.0 configured: the keys without the token, signed by SimpleAWSCredentialsProvider. */
    @Test
    void theSameTargetFailsWhenTheTokenIsDropped() {
        assumeTrue(sts != null, "MinIO STS not reachable");
        String table = "s3a://" + bucket + "/delta/tokenless-" + UUID.randomUUID();
        var conf = uncached(new Configuration());
        conf.set("fs.s3a.access.key", sts.accessKeyId());
        conf.set("fs.s3a.secret.key", sts.secretAccessKey());
        conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        conf.set("fs.s3a.endpoint", ENDPOINT);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.retry.limit", "1");
        conf.set("fs.s3a.attempts.maximum", "1");
        assertThrows(Exception.class, () -> new DeltaTableWriter(table).write(conf, List.of("{\"a\":1}").iterator()));
    }

    @Test
    void aSourceRepositoryReadsWithTemporaryCredentialsAndFailsFastWithoutTheToken() throws Exception {
        assumeTrue(sts != null, "MinIO STS not reachable");
        var env = Map.of("AWS_ACCESS_KEY_ID", sts.accessKeyId(), "AWS_SECRET_ACCESS_KEY", sts.secretAccessKey(),
            "AWS_SESSION_TOKEN", sts.sessionToken());
        try (BlobStore store = CredentialSources.openSourceStore("s3://" + bucket + "/repo", ENDPOINT,
                CredentialSources.Mode.ENV, null, env::get)) {
            assertFalse(store.list("").isEmpty());
            assertTrue(store.stat("index.latest").isPresent());
            assertEquals(2, new RepositoryInspector(store).overview().indices().size());
        }
        var tokenless = Map.of("AWS_ACCESS_KEY_ID", sts.accessKeyId(), "AWS_SECRET_ACCESS_KEY", sts.secretAccessKey());
        try (BlobStore store = CredentialSources.openSourceStore("s3://" + bucket + "/repo", ENDPOINT,
                CredentialSources.Mode.ENV, null, tokenless::get)) {
            long started = System.nanoTime();
            var failure = assertThrows(S3Exception.class, () -> store.read("index.latest"));
            assertEquals(403, failure.statusCode());
            assertTrue((System.nanoTime() - started) / 1_000_000 < 3_000, "a 403 must not be retried");
        }
    }

    /**
     * A source on an STS session and a target on long-term keys in one process: each side keeps
     * its own credentials.
     */
    @Test
    void anStsSourceAndALongTermKeyTargetWorkInOneProcess() throws Exception {
        assumeTrue(sts != null, "MinIO STS not reachable");
        var env = Map.of(
            "AWS_ACCESS_KEY_ID", sts.accessKeyId(), "AWS_SECRET_ACCESS_KEY", sts.secretAccessKey(),
            "AWS_SESSION_TOKEN", sts.sessionToken(),
            "TARGET_AWS_ACCESS_KEY_ID", "minioadmin", "TARGET_AWS_SECRET_ACCESS_KEY", "minioadmin");
        String table = "s3a://" + bucket + "/delta/separated-" + UUID.randomUUID();
        var conf = uncached(DumpToDeltaCommand.s3aConf(table, ENDPOINT, env::get));
        assertEquals("minioadmin", conf.get("fs.s3a.access.key"));
        assertNull(conf.get("fs.s3a.session.token"));
        try (BlobStore store = CredentialSources.openSourceStore("s3://" + bucket + "/repo", ENDPOINT,
                CredentialSources.Mode.ENV, null, env::get)) {
            var sources = new RepositorySources(new RepositorySources.Source("primary", store));
            var report = new FullIndexExporter(sources, conf, (shard, dir) -> new Lucene10ShardReader(dir))
                .run("frozen-docs-f5ffca09", table, RunReportListener.NO_OP);
            assertEquals(0, report.quarantinedCount());
            assertEquals(report.totalShards(), report.committedCount());
            assertTrue(report.rowsWrittenThisRun() > 0);
        }
    }
}
