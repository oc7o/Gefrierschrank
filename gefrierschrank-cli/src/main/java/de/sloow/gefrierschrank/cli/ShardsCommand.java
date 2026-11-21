package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;
import java.util.function.Function;

@Command(name = "shards", description = "List shards of an index with file counts and sizes")
public class ShardsCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Repository: local dir or s3://bucket/prefix")
    private String repoUri;

    @Parameters(index = "1", description = "Index name (e.g. test_data_7_17_9)")
    private String indexName;

    @CommandLine.Option(names = {"--source-endpoint", "--endpoint"},
        description = "S3 endpoint for s3:// repositories")
    private String sourceEndpoint;

    @CommandLine.Option(names = "--source-credentials", paramLabel = "env|ambient", converter = CredentialSources.ModeConverter.class,
        description = "env (default): AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (optional AWS_SESSION_TOKEN); ambient: " +
            "AWS_PROFILE, web identity, container or instance role, never environment keys.")
    private CredentialSources.Mode sourceCredentials = CredentialSources.Mode.ENV;

    /** Where credential variables are read from; tests substitute their own. */
    Function<String, String> environment = System::getenv;

    @Override
    public Integer call() {
        try (BlobStore store = openStore()) {
            var inspector = new RepositoryInspector(store);
            var index = inspector.index(indexName);
            printMountedBacking(inspector, indexName);

            System.out.printf("%-8s %-12s %-12s %s%n", "SHARD", "FILES", "SIZE", "LUCENE VERSIONS");
            for (int shard = 0; shard < index.shardGenerations().size(); shard++) {
                ShardSnapshot snap = inspector.shardSnapshot(indexName, shard);
                String versions = snap.files().stream()
                    .map(f -> f.writtenBy() == null ? "?" : f.writtenBy())
                    .distinct()
                    .sorted()
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("-");
                System.out.printf("%-8d %-12d %-12d %s%n",
                    shard, snap.files().size(), snap.totalSize(), versions);
            }
            return 0;
        } catch (Exception e) {
            System.err.println("error: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            return 1;
        }
    }

    /** Package-private so tests can observe the store. */
    BlobStore openStore() {
        return CredentialSources.openSourceStore(repoUri, sourceEndpoint, sourceCredentials, null, environment);
    }

    /**
     * A mounted searchable snapshot holds no Lucene files of its own; name the backing snapshot
     * dump-to-delta exports instead. Best effort: these commands diagnose broken indices, so an
     * unreadable metadata blob is a note on stderr, never a reason to withhold the listing.
     */
    static void printMountedBacking(RepositoryInspector inspector, String indexName) {
        JsonNode indexMetadata;
        try {
            indexMetadata = inspector.indexMetadata(indexName);
        } catch (Exception e) {
            System.err.println("note: index metadata unreadable (" + e.getClass().getSimpleName()
                + "), so whether the index is mounted is unknown");
            return;
        }
        String snapshot = RepositorySources.setting(indexMetadata, "index.store.snapshot.snapshot_uuid");
        if (snapshot == null && !"snapshot".equals(RepositorySources.setting(indexMetadata, "index.store.type"))) return;
        System.out.printf("mounted: backing %s/%s/%s%n",
            orUnknown(RepositorySources.setting(indexMetadata, "index.store.snapshot.repository_name")),
            orUnknown(snapshot), orUnknown(RepositorySources.setting(indexMetadata, "index.store.snapshot.index_name")));
    }

    private static String orUnknown(String value) {
        return value == null ? "?" : value;
    }
}
