package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.IndexOverview;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.delta.ExportPreflight;
import de.sloow.gefrierschrank.delta.SourceVectorsPlan;
import de.sloow.gefrierschrank.syntheticsource.SourceVectors;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Classifies every index by what {@code dump-to-delta} will do with it, reading only metadata -
 * no shard data blobs are downloaded, so it is cheap enough to run against a multi-terabyte
 * repository before committing to a full export.
 *
 * <p>Verdicts come from {@link ExportPreflight}, the predicates the export itself applies, so the
 * two cannot drift: stored and synthetic indices export (synthetic rows are reconstructed, not
 * byte-exact), mounted searchable snapshots export from their backing snapshot, and indices the
 * export refuses or quarantines are named with the reason. Anything that cannot be evaluated is
 * UNKNOWN, never exportable. Sizes are the shard bytes the export would download.
 *
 * <p>It also lists exact-identity groups: names that resolve to the same backing index, snapshot
 * and shard count, typically an original and its mounted {@code partial-}/{@code restored-} shell.
 * {@code dump-to-delta --all-indices} exports each member into its own table, so their data is
 * read once per member.
 */
@Command(name = "survey", description = "Classify every index by what dump-to-delta will do with it, and size it (metadata only, no shard downloads)")
public class SurveyCommand implements Callable<Integer> {

    /** Concurrent metadata GETs; survey is latency-bound on S3 (one GET per shard plus one per index). */
    static final int FETCH_THREADS = 16;

    @Parameters(index = "0", description = "Repository: local dir or s3://bucket/prefix")
    private String repoUri;

    @CommandLine.Option(names = {"--source-endpoint", "--endpoint"},
        description = "S3 endpoint for s3:// repositories")
    private String sourceEndpoint;

    @CommandLine.Option(names = "--source-credentials", paramLabel = "env|ambient", converter = CredentialSources.ModeConverter.class,
        description = "env (default): AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (optional AWS_SESSION_TOKEN); ambient: " +
            "AWS_PROFILE, web identity, container or instance role, never environment keys.")
    private CredentialSources.Mode sourceCredentials = CredentialSources.Mode.ENV;

    @CommandLine.Option(names = {"--csv"}, description = "Emit CSV instead of a table")
    private boolean csv;

    @CommandLine.Option(names = "--no-sizes",
        description = "Skip the per-shard metadata that sizes each index: about one GET per index instead of one per shard")
    private boolean noSizes;

    int fetchThreads = FETCH_THREADS;
    SourceVectorsPlan.Prober vectorProbe = SourceVectors::probe;

    /** What dump-to-delta will do with an index. */
    enum Verdict {
        EXPORTABLE("stored _source, exported byte-exact"),
        EXPORTABLE_SYNTHETIC("synthetic source, reconstructed (not byte-exact)"),
        MOUNTED("mounted searchable snapshot, exported from its backing snapshot"),
        NOT_EXPORTABLE("refused or quarantined by dump-to-delta"),
        WARN_SOURCE_DISABLED_ES8("_source disabled on an ES 8+ snapshot: documents without stored _source are reconstructed where possible"),
        UNKNOWN("could not classify (metadata or probe error)");

        final String description;

        Verdict(String description) {
            this.description = description;
        }

        String label() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** {@code bytes} is -1 when not measured; {@code identity} is null when the name did not resolve. */
    record IndexSurvey(String name, Verdict verdict, String reason, int shards, long bytes, String backing,
                       String note, RepositorySources.Identity identity) {
        String verdictLabel() {
            return reason == null ? verdict.label() : verdict.label() + "(" + reason + ")";
        }
    }

    record ShellGroup(RepositorySources.Identity identity, List<String> members, long bytesPerMember) {}

    @Override
    public Integer call() {
        try (BlobStore store = openStore()) {
            var inspector = new RepositoryInspector(store);
            // Warm the catalog here, before any fetch thread uses the inspector.
            List<IndexOverview> indices = inspector.overview().indices();
            var sources = new RepositorySources(new RepositorySources.Source("primary", store, inspector));
            List<IndexSurvey> results;
            // The vector probe's isolated Elasticsearch world can print warnings to stdout, which
            // would corrupt --csv; stdout carries only the report.
            PrintStream stdout = System.out;
            System.setOut(System.err);
            try {
                results = survey(sources, indices);
            } finally {
                System.setOut(stdout);
            }

            List<ShellGroup> groups = shellGroups(results);
            Map<String, String> groupOf = new LinkedHashMap<>();
            for (ShellGroup group : groups) group.members().forEach(member -> groupOf.put(member, group.identity().label()));
            results.sort(Comparator.comparingLong(IndexSurvey::bytes).reversed());
            if (csv) {
                printCsv(results, groupOf);
            } else {
                printTable(results, groups);
            }
            return 0;
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null) root = root.getCause();
            System.err.println("error: " + root.getClass().getSimpleName() + ": " + root.getMessage());
            return 1;
        }
    }

    /**
     * One task per index on a bounded pool; results come back in catalog order whatever the pool
     * size. Waits on each task's own future, which a finishing task completes without allocating:
     * a completion queue allocates per finished task, and an OutOfMemoryError there would leave
     * the survey waiting forever.
     */
    List<IndexSurvey> survey(RepositorySources sources, List<IndexOverview> indices) throws InterruptedException {
        int total = indices.size();
        List<IndexSurvey> results = new ArrayList<>(total);
        if (total == 0) return results;
        AtomicInteger threadIds = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, Math.min(fetchThreads, total)), task -> {
            Thread thread = new Thread(task, "survey-" + threadIds.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        try {
            List<Future<IndexSurvey>> futures = new ArrayList<>(total);
            SourceVectorsPlan.Prober probe = firstCallAlone(vectorProbe);
            for (IndexOverview index : indices) futures.add(pool.submit(() -> surveyIndex(sources, index, probe)));
            // Silence during a long run looks indistinguishable from a hang.
            for (int done = 1; done <= total; done++) {
                results.add(futures.get(done - 1).get());
                if (done % 10 == 0 || done == total) System.err.printf("  surveyed %d/%d indices...%n", done, total);
            }
            return results;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * The first vector probe builds the isolated Elasticsearch loader (about 1.5 s); running it
     * alone keeps the other fetch threads from each building a loader only to discard it.
     */
    static SourceVectorsPlan.Prober firstCallAlone(SourceVectorsPlan.Prober probe) {
        var done = new AtomicBoolean();
        Object lock = new Object();
        return (indexName, metadata) -> {
            if (!done.get()) {
                synchronized (lock) {
                    if (!done.get()) {
                        try {
                            return probe.probe(indexName, metadata);
                        } finally {
                            done.set(true);
                        }
                    }
                }
            }
            return probe.probe(indexName, metadata);
        };
    }

    /** Never throws: any failure is UNKNOWN with the exception's class name. */
    IndexSurvey surveyIndex(RepositorySources sources, IndexOverview index, SourceVectorsPlan.Prober probe) {
        String name = index.name();
        int shards = index.shardGenerations().size();
        RepositoryInspector inspector = sources.primary().inspector();
        String snapshotUuid;
        RepositorySources.Resolved resolved;
        try {
            snapshotUuid = inspector.latestSnapshotUuid(name);
            resolved = sources.resolve(name, snapshotUuid);
            shards = resolved.shardCount();
        } catch (RepositorySources.UnresolvedSnapshotException e) {
            return unresolvedMount(inspector, index, shards, e);
        } catch (Exception e) {
            return unknown(name, shards, e);
        }

        boolean mounted = resolved.referenceChain().size() > 1;
        String backing = mounted ? resolved.referenceChain().get(resolved.referenceChain().size() - 1) : "";
        RepositorySources.Identity identity;
        long bytes = noSizes ? -1 : 0;
        try {
            var backingInspector = resolved.source().inspector();
            identity = RepositorySources.identity(resolved);
            for (int shardId = 0; !noSizes && shardId < shards; shardId++) {
                bytes += backingInspector.shardSnapshot(resolved.indexName(), shardId, resolved.snapshotUuid()).totalSize();
            }
        } catch (Exception e) {
            return unknown(name, shards, e);
        }

        ExportPreflight.Verdict verdict = ExportPreflight.classify(name, resolved, probe);
        String note = verdict.detail();
        if (mounted) note = ("backing " + backing + (note.isEmpty() ? "" : "; " + note));
        return switch (verdict.kind()) {
            case EXPORTABLE, EXPORTABLE_SYNTHETIC -> mounted
                ? new IndexSurvey(name, Verdict.MOUNTED, verdict.kind() == ExportPreflight.Kind.EXPORTABLE ? "stored" : "synthetic",
                    shards, bytes, backing, note, identity)
                : new IndexSurvey(name, verdict.kind() == ExportPreflight.Kind.EXPORTABLE ? Verdict.EXPORTABLE
                    : Verdict.EXPORTABLE_SYNTHETIC, null, shards, bytes, backing, note, identity);
            case NOT_EXPORTABLE -> new IndexSurvey(name, Verdict.NOT_EXPORTABLE, verdict.reason(), shards, bytes, backing, note, identity);
            case WARN_SOURCE_DISABLED_ES8 -> new IndexSurvey(name, Verdict.WARN_SOURCE_DISABLED_ES8, null, shards, bytes, backing,
                note, identity);
            case UNKNOWN -> new IndexSurvey(name, Verdict.UNKNOWN, verdict.reason(), shards, bytes, backing, note, identity);
        };
    }

    /**
     * A mount whose backing snapshot this repository does not hold. When the mount records this
     * repository's own {@code repository_uuid}, the backing was deleted here (ILM's
     * {@code delete_searchable_snapshot} while older snapshots still hold the shell) and
     * dump-to-delta fails the index. When it records another UUID the backing lives in that
     * repository and exports with {@code dump-to-delta --repository NAME=URI}; without a UUID
     * (repositories older than 7.12) the two cannot be told apart. Any other unresolvable
     * reference is UNKNOWN.
     */
    private static IndexSurvey unresolvedMount(RepositoryInspector inspector, IndexOverview index, int shards,
                                               RepositorySources.UnresolvedSnapshotException failure) {
        try {
            JsonNode metadata = inspector.indexMetadata(index.name());
            String repository = RepositorySources.setting(metadata, "index.store.snapshot.repository_name");
            String snapshot = RepositorySources.setting(metadata, "index.store.snapshot.snapshot_uuid");
            String backingIndex = RepositorySources.setting(metadata, "index.store.snapshot.index_name");
            if (repository != null && snapshot != null && backingIndex != null && failure.getMessage().startsWith("Backing snapshot unavailable")) {
                String backing = repository + "/" + snapshot + "/" + backingIndex;
                String recordedUuid = RepositorySources.setting(metadata, "index.store.snapshot.repository_uuid");
                boolean recorded = recordedUuid != null && !recordedUuid.isBlank() && !recordedUuid.equals("_na_");
                String ownUuid = inspector.repositoryUuid();
                if (recorded && recordedUuid.equals(ownUuid)) {
                    return new IndexSurvey(index.name(), Verdict.NOT_EXPORTABLE, "backing_missing", shards, -1, backing,
                        "backing snapshot is gone from this repository (the mount names its repository_uuid); "
                            + "dump-to-delta fails the index", null);
                }
                String hint = "survey it there, export with dump-to-delta --repository " + repository + "=URI";
                return new IndexSurvey(index.name(), Verdict.MOUNTED, "other_repository", shards, -1, backing,
                    recorded && ownUuid != null
                        ? "backing snapshot is in repository '" + repository + "': " + hint
                        : "backing snapshot is not in this repository and no repository_uuid tells whether '" + repository
                            + "' is another one or it was deleted; if another: " + hint, null);
            }
        } catch (Exception e) {
            return unknown(index.name(), shards, e);
        }
        return new IndexSurvey(index.name(), Verdict.UNKNOWN, "unresolved_mount", shards, -1, "",
            failure.getClass().getSimpleName(), null);
    }

    /** Unsized: bytes summed before a failure are not the index's size. */
    private static IndexSurvey unknown(String name, int shards, Exception failure) {
        return new IndexSurvey(name, Verdict.UNKNOWN, "metadata_unreadable", shards, -1, "",
            failure.getClass().getSimpleName(), null);
    }

    /**
     * Names whose exact identity another name shares; the members' shard data is the same. The
     * name equal to the backing index leads, the others follow in catalog order.
     */
    static List<ShellGroup> shellGroups(List<IndexSurvey> results) {
        Map<RepositorySources.Identity, List<IndexSurvey>> byIdentity = new LinkedHashMap<>();
        for (IndexSurvey survey : results) {
            if (survey.identity() != null) byIdentity.computeIfAbsent(survey.identity(), id -> new ArrayList<>()).add(survey);
        }
        List<ShellGroup> groups = new ArrayList<>();
        byIdentity.forEach((identity, members) -> {
            if (members.size() < 2) return;
            List<String> names = new ArrayList<>();
            members.stream().map(IndexSurvey::name).filter(identity.index()::equals).forEach(names::add);
            members.stream().map(IndexSurvey::name).filter(name -> !name.equals(identity.index())).forEach(names::add);
            groups.add(new ShellGroup(identity, List.copyOf(names), members.get(0).bytes()));
        });
        return groups;
    }

    private void printTable(List<IndexSurvey> results, List<ShellGroup> groups) {
        System.out.printf("%-26s %-28s %7s %14s  %-38s %s%n", "VERDICT", "REASON", "SHARDS", "SIZE", "INDEX", "NOTE");
        for (IndexSurvey s : results) {
            System.out.printf("%-26s %-28s %7d %14s  %-38s %s%n",
                s.verdict().label(), s.reason() == null ? "" : s.reason(), s.shards(), size(s.bytes()),
                truncate(s.name(), 38), s.note());
        }

        System.out.println();
        System.out.println("Totals by verdict:");
        Map<String, List<IndexSurvey>> byLabel = new LinkedHashMap<>();
        for (Verdict v : Verdict.values()) {
            results.stream().filter(s -> s.verdict() == v).map(IndexSurvey::verdictLabel).sorted().distinct()
                .forEach(label -> byLabel.put(label, new ArrayList<>()));
        }
        results.forEach(s -> byLabel.get(s.verdictLabel()).add(s));
        long grandTotal = results.stream().mapToLong(s -> Math.max(0, s.bytes())).sum();
        for (var entry : byLabel.entrySet()) {
            List<IndexSurvey> matching = entry.getValue();
            long bytes = matching.stream().mapToLong(s -> Math.max(0, s.bytes())).sum();
            int shards = matching.stream().mapToInt(IndexSurvey::shards).sum();
            System.out.printf("  %-44s %5d indices, %6d shards, %12s  (%5.1f%%)  %s%n",
                entry.getKey(), matching.size(), shards, noSizes ? "-" : humanBytes(bytes),
                grandTotal == 0 ? 0.0 : (100.0 * bytes / grandTotal), matching.get(0).verdict().description);
        }
        System.out.printf("  %-44s %5d indices, %6d shards, %12s%n", "TOTAL", results.size(),
            results.stream().mapToInt(IndexSurvey::shards).sum(), noSizes ? "-" : humanBytes(grandTotal));

        System.out.println();
        if (groups.isEmpty()) {
            System.out.println("Exact-identity groups: none (no two names export the same backing data)");
            return;
        }
        System.out.println("Exact-identity groups (same backing index, snapshot and shard count; dump-to-delta --all-indices");
        System.out.println("exports each member into its own table, reading the same shard data once per member):");
        long duplicated = 0;
        for (ShellGroup group : groups) {
            long extra = Math.max(0, group.bytesPerMember()) * (group.members().size() - 1);
            duplicated += extra;
            System.out.printf("  %s (%d shard%s, %s each): %s%n", group.identity().label(), group.identity().shardCount(),
                group.identity().shardCount() == 1 ? "" : "s", size(group.bytesPerMember()), String.join(", ", group.members()));
        }
        System.out.printf("  %d groups, %s read more than once%n", groups.size(), noSizes ? "-" : humanBytes(duplicated));
    }

    private void printCsv(List<IndexSurvey> results, Map<String, String> groupOf) {
        System.out.println("index,verdict,reason,shards,bytes,backing,group,note");
        for (IndexSurvey s : results) {
            System.out.println(String.join(",", csvField(s.name()), s.verdict().label(),
                s.reason() == null ? "" : csvField(s.reason()), Integer.toString(s.shards()),
                s.bytes() < 0 ? "" : Long.toString(s.bytes()), csvField(s.backing()),
                csvField(groupOf.getOrDefault(s.name(), "")), csvField(s.note())));
        }
    }

    private static String csvField(String value) {
        if (value.indexOf(',') < 0 && value.indexOf('"') < 0 && value.indexOf('\n') < 0) return value;
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    private String size(long bytes) {
        return bytes < 0 ? "-" : humanBytes(bytes);
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
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

    /** Package-private so tests can observe the store's reads. */
    BlobStore openStore() {
        return CredentialSources.openSourceStore(repoUri, sourceEndpoint, sourceCredentials, null, System::getenv);
    }
}
