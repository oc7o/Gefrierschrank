package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositoryOverview;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;
import java.util.function.Function;

@Command(name = "info", description = "Overview of a snapshot repository: snapshots, indices, shard generations")
public class InfoCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Repository: local dir or s3://bucket/prefix")
    private String repoUri;

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
            print(new RepositoryInspector(store).overview());
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

    private void print(RepositoryOverview o) {
        System.out.printf("Repository generation : %d%n", o.latestGeneration());
        System.out.printf("Minimum ES version    : %s%n", o.minVersion());
        System.out.printf("Snapshots             : %d%n", o.snapshots().size());
        System.out.println();
        System.out.printf("%-24s %-30s %-13s %-10s %s%n", "SNAPSHOT", "UUID", "STATE", "VERSION", "INDEX VERSION");
        boolean anyIndexVersion = false;
        for (var snap : o.snapshots()) {
            anyIndexVersion |= snap.indexVersion() >= 0;
            System.out.printf("%-24s %-30s %-13s %-10s %s%n", snap.name(), snap.uuid(), stateName(snap.state()), snap.version(),
                snap.indexVersion() < 0 ? "-" : Long.toString(snap.indexVersion()));
        }
        if (anyIndexVersion) {
            System.out.println("(Elasticsearch records VERSION 8.11.0 for every snapshot since 8.11; INDEX VERSION identifies");
            System.out.println(" the release, e.g. 8521000 = 8.17, 9060000 = 9.3)");
        }
        System.out.println();
        System.out.printf("%-24s %-30s %-10s %s%n", "INDEX", "REPO UUID", "SHARDS", "SNAPSHOTS");
        for (var index : o.indices()) {
            System.out.printf("%-24s %-30s %-10d %s%n",
                index.name(), index.repoUuid(), index.shardGenerations().size(), index.snapshotUuids().size());
        }
    }

    /** {@code SnapshotState} byte values as Elasticsearch writes them into index-N. */
    static String stateName(int state) {
        return switch (state) {
            case 0 -> "IN_PROGRESS";
            case 1 -> "SUCCESS";
            case 2 -> "FAILED";
            case 3 -> "PARTIAL";
            case 4 -> "INCOMPATIBLE";
            default -> Integer.toString(state);
        };
    }
}
