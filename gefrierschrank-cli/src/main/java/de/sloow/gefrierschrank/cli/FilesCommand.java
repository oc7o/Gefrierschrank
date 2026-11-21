package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;
import java.util.function.Function;

@Command(name = "files", description = "List files of one shard: blob name, physical name, size, Lucene version")
public class FilesCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Repository: local dir or s3://bucket/prefix")
    private String repoUri;

    @Parameters(index = "1", description = "Index name")
    private String indexName;

    @Parameters(index = "2", defaultValue = "0", description = "Shard id (default: 0)")
    private int shardId;

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
            var shard = inspector.shardSnapshot(indexName, shardId);
            ShardsCommand.printMountedBacking(inspector, indexName);

            System.out.printf("Shard %s/%d — snapshot '%s', index version %d, total %d bytes%n%n",
                indexName, shardId, shard.snapshotName(), shard.indexVersion(), shard.totalSize());
            System.out.printf("%-30s %-16s %-12s %-10s %s%n",
                "BLOB NAME", "PHYSICAL", "SIZE", "LUCENE", "EMBEDDED");
            for (var f : shard.files()) {
                System.out.printf("%-30s %-16s %-12d %-10s %s%n",
                    f.blobName(), f.physicalName(), f.length(),
                    f.writtenBy() == null ? "?" : f.writtenBy(),
                    f.metaHash() != null ? "yes" : "-");
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
}
