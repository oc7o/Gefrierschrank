package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Delta writes to MinIO via the S3A filesystem (hadoop-aws). Skipped when MinIO
 * is not reachable. This is the streaming path: no local staging of table data.
 */
class DeltaS3WriterTest {

    private static final String ENDPOINT = "http://localhost:9000";
    private static final String BUCKET = "gefrierschrank-delta-test";

    @BeforeAll
    static void createBucket() {
        getConf(); // forces S3A init; fails fast if MinIO unreachable
        var s3 = software.amazon.awssdk.services.s3.S3Client.builder()
            .endpointOverride(java.net.URI.create(ENDPOINT))
            .region(software.amazon.awssdk.regions.Region.US_EAST_1)
            .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("minioadmin", "minioadmin")))
            .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                .pathStyleAccessEnabled(true).build())
            .build();
        try {
            s3.createBucket(b -> b.bucket(BUCKET));
        } catch (software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException e) {
            // fine
        }
        s3.close();
    }

    private static org.apache.hadoop.conf.Configuration hadoopConf() {
        var conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.s3a.access.key", "minioadmin");
        conf.set("fs.s3a.secret.key", "minioadmin");
        conf.set("fs.s3a.endpoint", ENDPOINT);
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        return conf;
    }

    private static void getConf() {
        hadoopConf();
    }

    @Test
    void writesDeltaTableToMinio() throws Exception {
        String tableUri = "s3a://" + BUCKET + "/test-" + UUID.randomUUID();
        List<String> docs = List.of(
            "{\"id\": 1, \"title\": \"S3 row A\"}",
            "{\"id\": 2, \"title\": \"S3 row B\"}");

        long count = new DeltaTableWriter(tableUri).write(hadoopConf(), docs.iterator());
        assertEquals(2, count);

        // read back via delta-standalone over S3A
        var log = io.delta.standalone.DeltaLog.forTable(hadoopConf(), tableUri);
        assertTrue(log.tableExists());
        try (var it = log.snapshot().open()) {
            assertTrue(it.hasNext());
            assertEquals("S3 row A", it.next().getString("title"));
        }
    }

    @Test
    void zstdTableReadsBackOverS3a() throws Exception {
        String tableUri = "s3a://" + BUCKET + "/zstd-" + UUID.randomUUID();
        var context = new ArchiveContext("idx", "rid", "suuid", "snap", "idx", "suuid");
        List<String> sources = DeltaTableWriterTest.logLikeSources(2_000, 43);
        var conf = hadoopConf();
        assertEquals(sources.size(), new DeltaTableWriter(tableUri).write(conf,
            DeltaTableWriterTest.archiveRows(context, sources).iterator(), ArchiveContext.mapping(), context.key(0)));

        for (var footer : ParquetFooters.read(conf, tableUri)) {
            assertEquals(java.util.Set.of(org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD), footer.codecs());
        }
        assertEquals(sources, DeltaTableWriterTest.readSourcesByDoc(conf, tableUri));
    }

    /** S3SingleDriverLogStore serializes commits per JVM; the writer must not add conflicts or orphans on top. */
    @Test
    void concurrentDistinctShardCommitsLandWithoutOrphans() throws Exception {
        String prefix = "concurrent-" + UUID.randomUUID();
        String tableUri = "s3a://" + BUCKET + "/" + prefix;
        var conf = hadoopConf();
        var keys = List.of("idx#0#snap", "idx#1#snap");
        var outcomes = DeltaTableWriterTest.writeConcurrently(i -> new DeltaTableWriter(tableUri), conf, keys);
        for (String key : keys) assertEquals(3L, outcomes.get(key), key);
        assertEquals(java.util.Set.copyOf(keys), new DeltaTableWriter(tableUri).committedShardKeys(conf));
        assertEquals(6, DeltaTableWriterTest.countRows(conf, tableUri));

        var dataObjects = new java.util.TreeSet<String>();
        try (var s3 = s3Client()) {
            s3.listObjectsV2Paginator(b -> b.bucket(BUCKET).prefix(prefix + "/")).contents().forEach(object -> {
                String name = object.key().substring(prefix.length() + 1);
                if (name.endsWith(".parquet")) dataObjects.add(name);
            });
        }
        assertEquals(ParquetFooters.addFilePaths(conf, tableUri), dataObjects);
    }

    private static software.amazon.awssdk.services.s3.S3Client s3Client() {
        return software.amazon.awssdk.services.s3.S3Client.builder()
            .endpointOverride(java.net.URI.create(ENDPOINT))
            .region(software.amazon.awssdk.regions.Region.US_EAST_1)
            .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("minioadmin", "minioadmin")))
            .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                .pathStyleAccessEnabled(true).build())
            .build();
    }

    @AfterAll
    static void cleanup() throws Exception {
        // S3A cleanup: delete objects via AWS SDK if MinIO still up
        try {
            var s3 = software.amazon.awssdk.services.s3.S3Client.builder()
                .endpointOverride(java.net.URI.create(ENDPOINT))
                .region(software.amazon.awssdk.regions.Region.US_EAST_1)
                .credentialsProvider(software.amazon.awssdk.auth.credentials.StaticCredentialsProvider.create(
                    software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create("minioadmin", "minioadmin")))
                .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                    .pathStyleAccessEnabled(true).build())
                .build();
            var listing = s3.listObjectsV2Paginator(b -> b.bucket(BUCKET));
            var ids = new java.util.ArrayList<software.amazon.awssdk.services.s3.model.ObjectIdentifier>();
            listing.contents().forEach(o -> ids.add(software.amazon.awssdk.services.s3.model.ObjectIdentifier.builder()
                .key(o.key()).build()));
            if (!ids.isEmpty()) {
                s3.deleteObjects(b -> b.bucket(BUCKET)
                    .delete(software.amazon.awssdk.services.s3.model.Delete.builder().objects(ids).build()));
            }
            s3.deleteBucket(b -> b.bucket(BUCKET));
            s3.close();
        } catch (Exception ignored) {
            // MinIO down: nothing to clean
        }
    }
}
