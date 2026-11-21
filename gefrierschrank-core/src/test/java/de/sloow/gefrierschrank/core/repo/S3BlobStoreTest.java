package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import java.net.URI;
import java.util.UUID;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Runs the BlobStore contract against MinIO through S3BlobStore.minio. The hand-built client
 * only manages the bucket. Skipped when MinIO is not reachable so `mvn test` stays green on
 * machines without Docker.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class S3BlobStoreTest extends BlobStoreContractTest {

    private static final String ENDPOINT = "http://localhost:9000";
    private static final String BUCKET = "gefrierschrank-contract-test";

    private S3Client client;
    private String prefix;

    @BeforeAll
    void start() {
        client = s3Client();
        try {
            client.listBuckets();
        } catch (Exception e) {
            client = null;  // MinIO not reachable: tests will be skipped
            return;
        }
        client.createBucket(b -> b.bucket(BUCKET));
        prefix = "test-" + UUID.randomUUID() + "/";
    }

    @Override
    protected String root() {
        return prefix;
    }

    private final java.util.List<S3BlobStore> stores = new java.util.ArrayList<>();

    /** The production client configuration (retry budget, HTTP pool), not a hand-built client. */
    @Override
    protected BlobStore create(String root) {
        org.junit.jupiter.api.Assumptions.assumeTrue(client != null, "MinIO not reachable");
        var store = S3BlobStore.minio(ENDPOINT, BUCKET, root, "minioadmin", "minioadmin");
        stores.add(store);
        return store;
    }

    private S3Client s3Client() {
        return S3Client.builder()
            .endpointOverride(URI.create(ENDPOINT))
            .region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create("minioadmin", "minioadmin")))
            .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build())
            .build();
    }

    @AfterAll
    void cleanup() {
        stores.forEach(S3BlobStore::close);
        if (client == null) return;
        var listing = client.listObjectsV2Paginator(b -> b.bucket(BUCKET));
        var toDelete = software.amazon.awssdk.services.s3.model.ObjectIdentifier.builder();
        var objects = software.amazon.awssdk.services.s3.model.Delete.builder();
        java.util.List<software.amazon.awssdk.services.s3.model.ObjectIdentifier> ids =
            new java.util.ArrayList<>();
        listing.contents().forEach(o ->
            ids.add(software.amazon.awssdk.services.s3.model.ObjectIdentifier.builder()
                .key(o.key()).build()));
        if (!ids.isEmpty()) {
            client.deleteObjects(b -> b.bucket(BUCKET)
                .delete(software.amazon.awssdk.services.s3.model.Delete.builder()
                    .objects(ids).build()));
        }
        client.deleteBucket(b -> b.bucket(BUCKET));
        client.close();
    }
}
