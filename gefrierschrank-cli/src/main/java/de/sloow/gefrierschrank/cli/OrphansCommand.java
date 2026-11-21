package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Finds blobs physically present in the repository's blob store that are NOT reachable by
 * walking the CURRENT index-N metadata (every index -&gt; every shard -&gt; every retained
 * snapshot's file list - see {@link RepositoryInspector#reachableBlobKeys()}). Read-only: never
 * deletes anything, only reports.
 *
 * <p>A bulk object-store listing (e.g. {@code rclone size}) can report more physical bytes than
 * {@code survey}'s logically-reachable total. This command explains the difference without ever
 * needing to look at document content: the diff between "everything in the bucket" and "everything
 * index-N still references" is either (a) genuine garbage - blobs even Elasticsearch's own
 * {@code POST _snapshot/<repo>/_cleanup} API would remove, left behind by interrupted deletions,
 * historical bugs, or races - or (b) a sign this tool's own reachability walk has a gap. This
 * command computes exactly that diff so the two can be told apart.
 *
 * <p>Designed to scale with large repositories: one paginated bulk listing pass
 * over the blob store (see {@link BlobStore#listWithSizes}, backed by S3's ListObjectsV2
 * paginator - no per-object HEAD calls) plus one metadata-only walk of index-N/meta-*.dat/
 * shard index-* files (no segment data ever downloaded), then a single in-memory set-membership
 * pass. No network round trip is made per blob.
 */
@Command(name = "orphans",
    description = "List blobs physically present in the repository but not referenced by any current snapshot (metadata-only reachability walk + one bulk listing pass)")
public class OrphansCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Repository: local dir or s3://bucket/prefix")
    private String repoUri;

    @CommandLine.Option(names = {"--source-endpoint", "--endpoint"},
        description = "S3 endpoint for s3:// repositories")
    private String sourceEndpoint;

    @CommandLine.Option(names = "--source-credentials", paramLabel = "env|ambient", converter = CredentialSources.ModeConverter.class,
        description = "env (default): AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (optional AWS_SESSION_TOKEN); ambient: " +
            "AWS_PROFILE, web identity, container or instance role, never environment keys.")
    private CredentialSources.Mode sourceCredentials = CredentialSources.Mode.ENV;

    @CommandLine.Option(names = {"--csv"}, description = "Emit CSV (one orphaned key per line) instead of a summary")
    private boolean csv;

    @CommandLine.Option(names = {"--limit"}, defaultValue = "50",
        description = "Max number of individual orphaned keys to print in summary mode (default: 50; use --csv to list all)")
    private int limit;

    record Orphan(String key, long size) {}

    @Override
    public Integer call() {
        try (BlobStore store = openStore(repoUri)) {
            var inspector = new RepositoryInspector(store);

            System.err.println("Reading current index-N metadata (reachability walk)...");
            Set<String> reachable = inspector.reachableBlobKeys();
            System.err.printf("  %d blob keys reachable from the current repository generation%n", reachable.size());

            System.err.println("Listing physical blobs in the repository (one bulk paginated pass)...");
            List<BlobMetadata> physical = store.listWithSizes("");
            System.err.printf("  %d objects physically present%n", physical.size());

            List<Orphan> orphans = new ArrayList<>();
            long totalPhysicalBytes = 0;
            for (BlobMetadata blob : physical) {
                totalPhysicalBytes += blob.size();
                if (!reachable.contains(blob.key())) {
                    orphans.add(new Orphan(blob.key(), blob.size()));
                }
            }
            orphans.sort(Comparator.comparingLong(Orphan::size).reversed());

            long orphanBytes = orphans.stream().mapToLong(Orphan::size).sum();

            if (csv) {
                printCsv(orphans);
            } else {
                printSummary(physical.size(), totalPhysicalBytes, reachable.size(), orphans, orphanBytes);
            }
            return 0;
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null) root = root.getCause();
            System.err.println("error: " + root.getMessage());
            return 1;
        }
    }

    private void printSummary(int physicalCount, long physicalBytes, int reachableCount,
                               List<Orphan> orphans, long orphanBytes) {
        System.out.println();
        System.out.printf("Physical objects in repository : %,d (%s)%n", physicalCount, humanBytes(physicalBytes));
        System.out.printf("Blob keys reachable from index-N: %,d%n", reachableCount);
        System.out.printf("Orphaned (unreferenced) objects : %,d (%s)%n", orphans.size(), humanBytes(orphanBytes));
        if (physicalBytes > 0) {
            System.out.printf("Orphaned share of physical bytes: %.1f%%%n", 100.0 * orphanBytes / physicalBytes);
        }
        System.out.println();
        if (orphans.isEmpty()) {
            System.out.println("No orphaned blobs found: every physical object is reachable from the current "
                + "index-N view. If a bulk listing tool (e.g. rclone size) still reports a much larger total "
                + "than this repository's reachable bytes, re-check that both tools are pointed at exactly the "
                + "same bucket/prefix.");
            return;
        }
        System.out.println("Likely explanation: these are blobs Elasticsearch's own "
            + "POST _snapshot/<repo>/_cleanup API would also remove - accumulated garbage from interrupted "
            + "snapshot deletions, races, or historical bugs, not data any current or past retained snapshot "
            + "can still restore. This is a normal, expected characteristic of a long-lived SLM-managed "
            + "repository, not a bug in this tool's counting.");
        System.out.println();
        System.out.printf("Largest orphaned keys (showing up to %d; use --csv for the full list):%n", limit);
        System.out.printf("%14s  %s%n", "SIZE", "KEY");
        orphans.stream().limit(limit).forEach(o ->
            System.out.printf("%14s  %s%n", humanBytes(o.size()), o.key()));
    }

    private void printCsv(List<Orphan> orphans) {
        System.out.println("key,bytes");
        for (Orphan o : orphans) {
            System.out.printf("%s,%d%n", o.key(), o.size());
        }
    }

    private static String humanBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        String[] units = {"KiB", "MiB", "GiB", "TiB", "PiB"};
        double value = bytes;
        int unit = -1;
        while (value >= 1024 && unit < units.length - 1) {
            value /= 1024;
            unit++;
        }
        return String.format("%.1f %s", value, units[unit]);
    }

    private BlobStore openStore(String uri) {
        return CredentialSources.openSourceStore(uri, sourceEndpoint, sourceCredentials, null, System::getenv);
    }
}
