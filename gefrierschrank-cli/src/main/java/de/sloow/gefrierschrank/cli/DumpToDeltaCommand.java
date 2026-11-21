package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.IndexOverview;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.S3BlobStore;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.delta.DumpToDeltaPipeline;
import de.sloow.gefrierschrank.delta.FullIndexExporter;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.delta.RunReport;
import de.sloow.gefrierschrank.delta.RunReportListener;
import de.sloow.gefrierschrank.delta.TargetStorageException;
import org.apache.hadoop.conf.Configuration;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

@Command(name = "dump-to-delta", description = "Dump an index into a Delta Lake table (local path or s3a://bucket/prefix)")
public class DumpToDeltaCommand implements Callable<Integer> {

    private static final ObjectMapper MANIFEST_MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    /**
     * Runs the whole per-index pipeline for one index, end to end. Exists as a seam between this
     * class and {@link DumpToDeltaPipeline} purely so tests can simulate one index throwing
     * (a total, pre-quarantine failure) among several without needing three real indices in a
     * fixture repository, and without re-plumbing this class's S3/report wiring for testability.
     */
    interface IndexProcessor {
        RunReport process(String indexName) throws Exception;

        /** {@link #process}, plus the identity of the data it exported when known (null otherwise). */
        default IndexOutcome processWithIdentity(String indexName) throws Exception {
            return new IndexOutcome(process(indexName), null);
        }
    }

    record IndexOutcome(RunReport report, RepositorySources.Identity identity) {}

    /** Threads that plan --mounted-shells skip-if-original, as survey's metadata fetches. */
    static final int PLAN_THREADS = 16;

    @Parameters(index = "0", description = "Repository: local dir or s3://bucket/prefix")
    private String repoUri;

    static class IndexSelection {
        @CommandLine.Option(names = {"--index"}, description = "Index to dump")
        String indexName;

        @CommandLine.Option(names = {"--all-indices"},
            description = "Dump every index in the repository in one command. -o/--output becomes " +
                "a table root (each index lands at <root>/<indexName> instead of exactly at the " +
                "given path) and --report becomes a directory (each index writes " +
                "<dir>/<indexName>.json, plus one aggregate <dir>/manifest.json summarizing the " +
                "whole run).")
        boolean allIndices;
    }

    @CommandLine.ArgGroup(multiplicity = "1")
    IndexSelection indexSelection;

    @CommandLine.Option(names = {"-o", "--output"}, required = true,
        description = "Delta table location: local path or s3a://bucket/prefix. With --all-indices, " +
            "this is a table ROOT: each index gets its own table at <root>/<indexName>.")
    private String tableUri;

    // "--endpoint" kept as a deprecated alias so existing scripts/docs referring to it keep working.
    @CommandLine.Option(names = {"--source-endpoint", "--endpoint"},
        description = "S3 endpoint for the source s3:// repository (e.g. http://localhost:9000)")
    private String sourceEndpoint;

    @CommandLine.Option(names = "--download-concurrency", paramLabel = "N", converter = DownloadConcurrency.class,
        description = "Concurrent download streams per shard, 1-32 (default: 8). 1 downloads a shard's files one after another.")
    private Integer downloadConcurrency;

    @CommandLine.Option(names = "--source-credentials", paramLabel = "env|ambient", converter = CredentialSources.ModeConverter.class,
        description = "Source repository credentials: env (default) reads AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY " +
            "(optional AWS_SESSION_TOKEN), which cannot refresh during the run; ambient uses a refreshing source " +
            "instead: AWS_PROFILE (role_arn, credential_process, SSO), web identity (IRSA), container or " +
            "instance role. Ambient must not be combined with AWS_ACCESS_KEY_ID. A backing repository with " +
            "--repository-env NAME=PREFIX uses PREFIX_AWS_* keys or the profile named by PREFIX_AWS_PROFILE instead.")
    private CredentialSources.Mode sourceCredentials = CredentialSources.Mode.ENV;

    @CommandLine.Option(names = "--target-credentials", paramLabel = "env|ambient", converter = CredentialSources.ModeConverter.class,
        description = "Target s3a:// credentials: env (default) reads TARGET_AWS_* (falling back to AWS_*) keys " +
            "and session token; ambient uses TARGET_AWS_PROFILE if set, web identity or the instance/container " +
            "role, never environment keys. Ambient must not be combined with TARGET_AWS_* keys.")
    private CredentialSources.Mode targetCredentials = CredentialSources.Mode.ENV;

    @CommandLine.Option(names = "--repository", paramLabel = "NAME=URI",
        description = "Additional backing repository, keyed by its ES repository name. Repeat for multiple repositories. Uses source credentials.")
    private java.util.Map<String, String> repositories = new java.util.LinkedHashMap<>();

    @CommandLine.Option(names = "--repository-endpoint", paramLabel = "NAME=URL",
        description = "S3 endpoint override for an additional --repository; otherwise uses --source-endpoint.")
    private java.util.Map<String, String> repositoryEndpoints = new java.util.LinkedHashMap<>();

    @CommandLine.Option(names = "--repository-env", paramLabel = "NAME=PREFIX",
        description = "Credentials for a backing repository from PREFIX_AWS_ACCESS_KEY_ID / PREFIX_AWS_SECRET_ACCESS_KEY (optional SESSION_TOKEN and REGION).")
    private java.util.Map<String, String> repositoryEnvs = new java.util.LinkedHashMap<>();

    @CommandLine.Mixin ExportOrchestrationOptions orchestration;
    @CommandLine.Mixin MountedShellsOption mountedShells = new MountedShellsOption();
    @CommandLine.Spec CommandLine.Model.CommandSpec spec;

    // Inspectors of the --repository sources; concurrent indices share them (see indexConcurrency).
    private final List<RepositoryInspector> backingInspectors = new ArrayList<>();

    // Source and target can live on different S3 services, accounts or regions (e.g. an ES
    // snapshot repo on one provider, a Delta lake on another). No fallback to --source-endpoint
    // is offered: silently reusing it would write a whole export to the wrong endpoint whenever
    // the two differ. The user must say explicitly where the target lives.
    @CommandLine.Option(names = {"--target-endpoint"},
        description = "S3 endpoint for the target s3a:// Delta table (e.g. http://localhost:9001). " +
            "Required for s3a:// outputs; not inherited from --source-endpoint since source and " +
            "target are often different S3 services.")
    private String targetEndpoint;

    @CommandLine.Mixin SourceDisabledAcceptOption sourceDisabledAccept = new SourceDisabledAcceptOption();
    @CommandLine.Mixin ParquetOutputOptions parquetOutput = new ParquetOutputOptions();
    @CommandLine.Mixin IntraShardOptions intraShard = new IntraShardOptions();
    @CommandLine.Mixin SelectiveFetchOption selectiveFetch = new SelectiveFetchOption();

    // Local filesystem path, deliberately not s3://: reports must be readable without the
    // source/target S3 credentials. Optional so existing scripts relying on the one-line stdout
    // summary keep working.
    @CommandLine.Option(names = {"--report"},
        description = "Local file path to write a structured JSON run report to, updated after " +
            "every shard. The report never contains document content, only shard identity, " +
            "counts, byte sizes, timings, and sanitized error info. If omitted, only a one-line " +
            "summary is printed to stdout. With --all-indices, this is a DIRECTORY: each index " +
            "writes <dir>/<indexName>.json, plus one aggregate <dir>/manifest.json.")
    private Path reportPath;

    @Override
    public Integer call() throws Exception {
        if (mountedShells.skipIfOriginal() && !indexSelection.allIndices)
            throw new CommandLine.ParameterException(spec.commandLine(), "--mounted-shells skip-if-original requires --all-indices");
        if (downloadConcurrency != null) System.setProperty(ShardMaterializer.CONCURRENCY_PROPERTY, downloadConcurrency.toString());
        System.out.println(BuildInfo.describe());
        parquetOutput.apply();
        intraShard.apply();
        selectiveFetch.apply();
        LuceneRuntimeSelector.requireInProcessRuntime(spec);
        backingInspectors.clear();
        var additionalStores = new ArrayList<BlobStore>();
        try (BlobStore store = openStore(repoUri)) {
            var inspector = new RepositoryInspector(store);
            var sources = new RepositorySources(new RepositorySources.Source("primary", store, inspector));
            for (var entry : repositories.entrySet()) {
                BlobStore backing = openStore(entry.getValue(), repositoryEndpoints.getOrDefault(entry.getKey(), sourceEndpoint),
                    repositoryEnvs.get(entry.getKey()));
                additionalStores.add(backing);
                var source = new RepositorySources.Source(entry.getKey(), backing);
                backingInspectors.add(source.inspector());
                sources.add(source);
            }
            var exporter = new FullIndexExporter(sources, s3aConf(), runtimeOpener());
            exporter.configure(orchestration.settings(indexSelection.allIndices), tableUri);
            if (indexSelection.allIndices) {
                return runAllIndices(inspector, sources, exporter);
            }
            RunReportListener listener = reportPath != null ? new JsonReportWriter(reportPath) : RunReportListener.NO_OP;
            RunReport report = exporter.run(indexSelection.indexName, tableUri, listener);
            printSummary(tableUri, report);
            return exitCode(report);
        } catch (Exception e) {
            System.err.println("error: " + safeFailure(e));
            return 1;
        } finally {
            for (BlobStore backing : additionalStores) backing.close();
        }
    }

    private Integer runAllIndices(RepositoryInspector inspector, RepositorySources sources, FullIndexExporter exporter)
            throws IOException, InterruptedException {
        List<String> indexNames = inspector.overview().indices().stream()
            .map(IndexOverview::name)
            .toList();
        IndexProcessor processor = new IndexProcessor() {
            @Override
            public RunReport process(String name) throws Exception {
                return processWithIdentity(name).report();
            }

            @Override
            public IndexOutcome processWithIdentity(String name) throws Exception {
                String indexTableUri = resolveIndexTableUri(tableUri, name);
                RunReportListener listener = RunReportListener.NO_OP;
                if (reportPath != null) {
                    listener = new JsonReportWriter(resolvePerIndexReportPath(reportPath, name));
                }
                var exported = exporter.runWithIdentity(name, indexTableUri, listener);
                return new IndexOutcome(exported.report(), exported.identity());
            }
        };
        int concurrency = indexConcurrency();
        MountedShellPlan shells = MountedShellPlan.NONE;
        if (mountedShells.skipIfOriginal()) {
            long started = System.nanoTime();
            shells = planMountedShells(indexNames, sources, exporter, tableUri, catalogsLoaded() ? PLAN_THREADS : 1);
            printMountedShellPlan(shells, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
        }
        return runAllIndices(indexNames, processor, reportPath, concurrency,
            (name, report) -> printSummary(resolveIndexTableUri(tableUri, name), report), shells);
    }

    /**
     * Plans --mounted-shells skip-if-original in two phases, so that a large catalog costs one
     * metadata read per name and Delta-log reads only where a shell may be skipped. Phase 1
     * resolves every name on its latest snapshot; names whose identity is shared with an original
     * become candidates. Phase 2 plans only the candidates exactly as their export will select
     * their snapshot ({@link FullIndexExporter#plan}). A name that cannot be planned is never
     * grouped, and exports (and fails, if it does) as in the default mode. Phase 1 can only miss a
     * group, never invent one: a miss costs a table, never a skipped shell.
     */
    static MountedShellPlan planMountedShells(List<String> names, RepositorySources sources, FullIndexExporter exporter,
                                              String root, int threads) throws InterruptedException {
        RepositoryInspector inspector = sources.primary().inspector();
        List<MountedShellPlan.Planned> latest = inParallel(names, threads, name -> {
            var resolved = sources.resolve(name, inspector.latestSnapshotUuid(name));
            return new MountedShellPlan.Planned(name, RepositorySources.identity(resolved),
                resolved.referenceChain().size() > 1, false);
        });
        var candidates = new java.util.LinkedHashSet<String>();
        var byIdentity = new java.util.LinkedHashMap<RepositorySources.Identity, List<MountedShellPlan.Planned>>();
        for (var planned : latest) {
            if (planned.identity() != null) byIdentity.computeIfAbsent(planned.identity(), id -> new ArrayList<>()).add(planned);
        }
        byIdentity.forEach((identity, group) -> {
            if (group.size() > 1 && group.stream().anyMatch(planned -> !planned.mounted()))
                group.forEach(planned -> candidates.add(planned.name()));
        });
        List<String> candidateNames = names.stream().filter(candidates::contains).toList();
        return MountedShellPlan.of(inParallel(candidateNames, threads, name -> {
            var plan = exporter.plan(name, resolveIndexTableUri(root, name));
            return new MountedShellPlan.Planned(name, plan.identity(), plan.mounted(), plan.tableStarted());
        }));
    }

    private interface Planner {
        MountedShellPlan.Planned plan(String name) throws Exception;
    }

    /** Plans every name, results in {@code names}' order; a name whose planning fails gets a null identity. */
    private static List<MountedShellPlan.Planned> inParallel(List<String> names, int threads, Planner planner)
            throws InterruptedException {
        java.util.function.Function<String, MountedShellPlan.Planned> safely = name -> {
            try {
                return planner.plan(name);
            } catch (Exception unplannable) {
                return new MountedShellPlan.Planned(name, null, false, false);
            }
        };
        if (threads <= 1 || names.size() <= 1) return names.stream().map(safely).toList();
        AtomicInteger threadIds = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(Math.min(threads, names.size()), task -> {
            Thread thread = new Thread(task, "gefrierschrank-plan-" + threadIds.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        try {
            List<Future<MountedShellPlan.Planned>> futures = new ArrayList<>(names.size());
            for (String name : names) futures.add(pool.submit(() -> safely.apply(name)));
            List<MountedShellPlan.Planned> planned = new ArrayList<>(names.size());
            for (var future : futures) {
                try {
                    planned.add(future.get());
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof Error error) throw error;
                    throw new IllegalStateException(e.getCause());
                }
            }
            return planned;
        } finally {
            pool.shutdownNow();
        }
    }

    private static void printMountedShellPlan(MountedShellPlan shells, long millis) {
        System.out.printf("Mounted shells: %d exact-identity group(s); %d shell(s) are written as aliases if their "
            + "original completes (planned in %d ms)%n", shells.groups(), shells.members(), millis);
        for (String shell : shells.startedShells())
            System.out.println("Mounted shell " + shell + " already has a table; exporting it");
    }

    /**
     * Up to --parallelism indices at once. Index threads share the repository inspectors, which
     * are safe for concurrent readers only once their catalog is loaded, so every inspector loads
     * it here, on this thread, first (the primary's was loaded to list the indices). A backing
     * repository whose catalog cannot be read now keeps the indices sequential, so each one fails
     * or succeeds exactly as in a sequential run.
     */
    private int indexConcurrency() {
        int requested = orchestration.parallelism;
        if (requested <= 1) return 1;
        Exception unloaded = unloadedCatalog();
        if (unloaded != null) {
            System.err.println("warning: cannot read the catalog of a --repository (" + unloaded.getClass().getSimpleName()
                + "); indices are exported one at a time");
            return 1;
        }
        return requested;
    }

    /** Whether every --repository inspector has its catalog loaded, so threads may share them. */
    private boolean catalogsLoaded() {
        return unloadedCatalog() == null;
    }

    private Exception unloadedCatalog() {
        for (RepositoryInspector backing : backingInspectors) {
            try {
                backing.overview();
            } catch (IOException | RuntimeException e) {
                return e;
            }
        }
        return null;
    }

    /**
     * Core loop for --all-indices: isolates each index's {@link IndexProcessor} call so one
     * index's total failure (thrown before that index's own per-shard quarantine logic even gets
     * a chance to run) doesn't abort the rest - the same isolation principle
     * {@link DumpToDeltaPipeline} already applies one level down, per shard. Package-private and
     * seam-injected so tests can simulate "index B throws, A and C still get processed" without
     * needing three real indices in a fixture repository.
     *
     * <p>A {@link TargetStorageException} from any index ends the run instead: every later index
     * would download its shards only to fail the same way. That index is FAILED, no further index
     * starts, the rest stay pending in the manifest, and the exit code is 1.
     */
    Integer runAllIndices(List<String> indexNames, IndexProcessor processor, Path reportPath) {
        return runIndicesInOrder(indexNames, processor, reportPath, (name, report) -> {}, MountedShellPlan.NONE);
    }

    /**
     * {@code concurrency} 1 is {@link #runAllIndices(List, IndexProcessor, Path)}. Above 1, up to
     * that many indices run at once, each on its own thread, while this thread alone calls
     * {@code onCompleted}, prints, records outcomes and writes the manifest (see
     * {@link #runIndicesConcurrently}). Every index's shards still pass through the one run-wide
     * shard pool, so --parallelism stays the bound on shards in flight.
     */
    Integer runAllIndices(List<String> indexNames, IndexProcessor processor, Path reportPath, int concurrency,
                          BiConsumer<String, RunReport> onCompleted) throws InterruptedException {
        return runAllIndices(indexNames, processor, reportPath, concurrency, onCompleted, MountedShellPlan.NONE);
    }

    /**
     * The loops above with --mounted-shells skip-if-original: a member of {@code shells} starts
     * only after its leader's entry is recorded, and then either becomes an ALIAS_OF entry (no
     * table, no thread) or starts next. Held members stay pending in the manifest until then, and
     * also when the run stops early.
     */
    Integer runAllIndices(List<String> indexNames, IndexProcessor processor, Path reportPath, int concurrency,
                          BiConsumer<String, RunReport> onCompleted, MountedShellPlan shells) throws InterruptedException {
        return concurrency <= 1 ? runIndicesInOrder(indexNames, processor, reportPath, onCompleted, shells)
            : runIndicesConcurrently(indexNames, processor, reportPath, concurrency, onCompleted, shells);
    }

    private Integer runIndicesInOrder(List<String> indexNames, IndexProcessor processor, Path reportPath,
                                      BiConsumer<String, RunReport> onCompleted, MountedShellPlan shells) {
        var entries = new AllIndicesManifest.IndexEntry[indexNames.size()];
        var schedule = new IndexSchedule(indexNames, shells);
        if (reportPath != null) writeManifest(reportPath, buildManifest(List.of(), indexNames, shells.active()));
        TargetStorageException storageFailure = null;
        while (storageFailure == null && schedule.hasNext()) {
            int position = schedule.next();
            String name = indexNames.get(position);
            AllIndicesManifest.IndexEntry entry;
            RepositorySources.Identity exported = null;
            try {
                IndexOutcome outcome = processor.processWithIdentity(name);
                onCompleted.accept(name, outcome.report());
                entry = completedEntry(name, outcome.report(), reportPath);
                exported = outcome.identity();
            } catch (Exception e) {
                entry = failedEntry(name, e);
                storageFailure = targetStorageFailure(e);
            }
            entries[position] = entry;
            if (reportPath != null) {
                writeManifest(reportPath, buildManifest(recorded(entries), indexNames, shells.active()));
            }
            recordAliases(schedule.release(name, entry.outcome(), exported), entries, indexNames, reportPath, shells);
        }
        return finishAllIndices(recorded(entries), indexNames, storageFailure, shells);
    }

    /** Records the alias decisions of a leader's release; the others are already admitted by the schedule. */
    private static void recordAliases(List<IndexSchedule.Decision> decisions, AllIndicesManifest.IndexEntry[] entries,
                                      List<String> indexNames, Path reportPath, MountedShellPlan shells) {
        boolean recorded = false;
        for (var decision : decisions) {
            if (!decision.alias()) continue;
            entries[decision.position()] = AllIndicesManifest.IndexEntry.alias(decision.name(), decision.aliasOf());
            System.out.println("Index " + decision.name() + ": alias of " + decision.aliasOf()
                + " (same backing index, snapshot and shard count); no table written");
            recorded = true;
        }
        if (recorded && reportPath != null) writeManifest(reportPath, buildManifest(recorded(entries), indexNames, shells.active()));
    }

    /**
     * Starts indices in planned order while fewer than {@code concurrency} are in flight, on fresh
     * daemon threads carrying this thread's context class loader: the shared isolated
     * Elasticsearch loader derives its classpath from the context loader of the thread that builds
     * it first. Outcomes are handled here as they finish; the manifest lists them in planned order
     * and keeps indices still in flight pending. An {@link Error} from an index is rethrown at once,
     * with the indices still in flight left pending, as when a sequential run crashes.
     */
    private Integer runIndicesConcurrently(List<String> indexNames, IndexProcessor processor, Path reportPath,
                                           int concurrency, BiConsumer<String, RunReport> onCompleted,
                                           MountedShellPlan shells) throws InterruptedException {
        var entries = new AllIndicesManifest.IndexEntry[indexNames.size()];
        var schedule = new IndexSchedule(indexNames, shells);
        var finished = new IndexResults();
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        if (reportPath != null) writeManifest(reportPath, buildManifest(List.of(), indexNames, shells.active()));
        TargetStorageException storageFailure = null;
        int inFlight = 0;
        while (inFlight > 0 || (storageFailure == null && schedule.hasNext())) {
            while (storageFailure == null && inFlight < concurrency && schedule.hasNext()) {
                int next = schedule.next();
                startIndex(next, indexNames.get(next), processor, contextLoader, finished);
                inFlight++;
            }
            IndexResult result = finished.take();
            inFlight--;
            if (result.failure() instanceof Error error) throw error;
            AllIndicesManifest.IndexEntry entry;
            try {
                if (result.failure() != null) throw result.failure() instanceof Exception exception ? exception
                    : new ExecutionException(result.failure());
                onCompleted.accept(result.indexName(), result.report());
                entry = completedEntry(result.indexName(), result.report(), reportPath);
            } catch (Exception e) {
                entry = failedEntry(result.indexName(), e);
                if (storageFailure == null) storageFailure = targetStorageFailure(e);
            }
            entries[result.position()] = entry;
            if (reportPath != null) writeManifest(reportPath, buildManifest(recorded(entries), indexNames, shells.active()));
            recordAliases(schedule.release(result.indexName(), entry.outcome(), entry.outcome() == AllIndicesManifest.Outcome.FAILED
                ? null : result.identity()), entries, indexNames, reportPath, shells);
        }
        return finishAllIndices(recorded(entries), indexNames, storageFailure, shells);
    }

    private record IndexResult(int position, String indexName, RunReport report, RepositorySources.Identity identity,
                               Throwable failure) {}

    /**
     * Results on their way from index threads to the reporting thread. Handing one over allocates,
     * so an OutOfMemoryError can lose it; the index thread then records that Error instead, and
     * the reporting thread, which never waits unboundedly, rethrows it.
     */
    private static final class IndexResults {
        private static final long POLL_MILLIS = 200;

        private final BlockingQueue<IndexResult> queue = new LinkedBlockingQueue<>();
        private volatile Throwable lost;

        void add(IndexResult result) {
            queue.add(result);
        }

        void lose(Throwable failure) {
            if (lost == null) lost = failure;
        }

        IndexResult take() throws InterruptedException {
            while (true) {
                IndexResult next = queue.poll(POLL_MILLIS, TimeUnit.MILLISECONDS);
                if (next != null) return next;
                Throwable failure = lost;
                if (failure instanceof Error error) throw error;
                if (failure != null) throw new AssertionError("an index's result was lost", failure);
            }
        }
    }

    private static void startIndex(int position, String indexName, IndexProcessor processor, ClassLoader contextLoader,
                                   IndexResults finished) {
        Thread thread = new Thread(() -> {
            try {
                IndexOutcome outcome = null;
                Throwable failure = null;
                try {
                    outcome = processor.processWithIdentity(indexName);
                } catch (Throwable e) {
                    failure = e;
                }
                finished.add(outcome != null ? new IndexResult(position, indexName, outcome.report(), outcome.identity(), null)
                    : new IndexResult(position, indexName, null, null, failure));
            } catch (Throwable lost) {
                finished.lose(lost);
            }
        }, "gefrierschrank-index-" + position);
        thread.setDaemon(true);
        thread.setContextClassLoader(contextLoader);
        thread.start();
    }

    private static List<AllIndicesManifest.IndexEntry> recorded(AllIndicesManifest.IndexEntry[] entries) {
        return java.util.Arrays.stream(entries).filter(java.util.Objects::nonNull).toList();
    }

    private static AllIndicesManifest.IndexEntry completedEntry(String name, RunReport report, Path reportPath) {
        var outcome = exitCode(report) == 0
            ? AllIndicesManifest.Outcome.COMPLETED
            : AllIndicesManifest.Outcome.COMPLETED_WITH_QUARANTINE;
        String reportFile = reportPath != null ? name + ".json" : null;
        var snapshot = report.snapshot();
        return new AllIndicesManifest.IndexEntry(name, outcome, reportFile, null, null,
            snapshot != null ? snapshot.snapshotUuid() : null,
            snapshot != null ? snapshot.newerSnapshotAvailable() : null);
    }

    private static AllIndicesManifest.IndexEntry failedEntry(String name, Exception e) {
        Throwable root = de.sloow.gefrierschrank.core.FailureDiagnostics.rootCause(e);
        TargetStorageException storageFailure = targetStorageFailure(e);
        String errorClass = (storageFailure != null ? storageFailure : root).getClass().getSimpleName();
        String errorMessage = safeFailure(e);
        // A fixed message already starts with its class.
        System.out.println("Index " + name + " failed outright: "
            + (errorMessage.startsWith(errorClass + ": ") ? errorMessage : errorClass + ": " + errorMessage));
        return new AllIndicesManifest.IndexEntry(name, AllIndicesManifest.Outcome.FAILED, null, errorClass, errorMessage);
    }

    private int finishAllIndices(List<AllIndicesManifest.IndexEntry> entries, List<String> indexNames,
                                 TargetStorageException storageFailure, MountedShellPlan shells) {
        printAggregateSummary(buildManifest(entries, indexNames, shells.active()));
        if (storageFailure != null) {
            System.err.println("error: " + safeFailure(storageFailure));
            return 1;
        }
        return aggregateExitCode(entries);
    }

    /**
     * The {@link TargetStorageException} in {@code failure}'s causes or suppressed failures, or
     * null: an index whose first failure was another shard's refusal still ends the run when one
     * of its shards found the target full.
     */
    static TargetStorageException targetStorageFailure(Throwable failure) {
        return de.sloow.gefrierschrank.core.FailureDiagnostics.find(failure, TargetStorageException.class);
    }

    private void printSummary(String tableUri, RunReport report) {
        System.out.printf("Index %s: %d/%d shards processed this run - %d committed, %d skipped " +
                "(already committed), %d quarantined%n",
            report.indexName(), report.shards().size(), report.totalShards(),
            report.committedCount(), report.skippedCount(), report.quarantinedCount());
        System.out.printf("Wrote %d documents (%,d source shard bytes) into Delta table %s in %dms%n",
            report.rowsWrittenThisRun(), report.bytesProcessedThisRun(), tableUri, report.elapsedMillis());
        if (report.quarantinedCount() > 0) {
            System.out.println("Quarantined shards:");
            for (RunReport.ShardReport s : report.shards()) {
                if (s.outcome() == RunReport.Outcome.QUARANTINED) {
                    System.out.printf("  shard %d (%s): %s: %s%n",
                        s.shardId(), s.snapshotName(), s.errorClass(), s.errorMessage());
                }
            }
        }
    }

    private void printAggregateSummary(AllIndicesManifest manifest) {
        System.out.printf("All-indices run: %d indices - %d completed, %d completed with quarantined " +
                "shards, %d failed outright%s%n",
            manifest.totalIndices(), manifest.completedCount(), manifest.completedWithQuarantineCount(),
            manifest.failedCount(), manifest.aliasCount() != null ? ", " + manifest.aliasCount() + " aliases of exported tables" : "");
    }

    /**
     * Exit code contract for unattended orchestration, which acts on the process exit code
     * rather than on log output:
     * 0 = every shard is now committed; 3 = the run finished cleanly but left one or more shards
     * quarantined ("partial success" - go look at the report); 1 (thrown-exception path above,
     * unchanged) = a run-level failure that isn't shard-specific (repository unreachable, index
     * not found). 3 was picked as a code that isn't already a common shell/exec convention (0/1
     * are POSIX success/failure, 2 is common for CLI usage errors, 126-165 are reserved by most
     * shells for exec/signal conditions).
     *
     * <p>Checking {@code quarantinedCount() == 0} on THIS run's report is sufficient to detect
     * quarantines left over from an earlier partial run too, not just new ones: every shard that
     * isn't already committed is reattempted on every call (see DumpToDeltaPipeline's resumability
     * loop), so a previously-quarantined shard always shows up in this run's report as either
     * newly committed or still quarantined - it can never silently disappear.
     */
    static int exitCode(RunReport report) {
        return report.quarantinedCount() == 0
            && report.committedCount() + report.skippedCount() == report.totalShards()
            && report.shards().stream().noneMatch(s -> s.outcome() == RunReport.Outcome.SKIPPED_NO_DATA) ? 0 : 3;
    }

    /**
     * Exit code contract for --all-indices, extending the single-index contract above one level
     * (per-index outcomes aggregated across the whole run): 0 = every index fully complete, no
     * quarantined shards anywhere, no index failed outright; 3 = same meaning as the single-index
     * code, now aggregated - no index failed outright, but at least one has a quarantined shard;
     * 4 (new) = at least one index failed outright before its own per-shard quarantine logic even
     * ran, producing no usable report at all for that index - worse than "some shards
     * quarantined", since the operator needs to tell "go check specific shards" apart from "an
     * entire index wasn't processed". 4 is the next code not already claimed by this project (0/1
     * are POSIX conventions, 2 is a common CLI usage-error code, 3 is already the single-index
     * quarantine code above). 1 (thrown-exception path in {@link #call()}, unchanged) still means
     * a run-level failure that isn't index-specific, e.g. the source repository itself can't be
     * opened - that can happen before iteration over indices even starts. An ALIAS_OF entry is
     * neutral: its original's own entry carries the original's 3 or 4.
     */
    static int aggregateExitCode(List<AllIndicesManifest.IndexEntry> entries) {
        boolean anyFailed = entries.stream().anyMatch(e -> e.outcome() == AllIndicesManifest.Outcome.FAILED);
        if (anyFailed) return 4;
        boolean anyQuarantined = entries.stream()
            .anyMatch(e -> e.outcome() == AllIndicesManifest.Outcome.COMPLETED_WITH_QUARANTINE);
        return anyQuarantined ? 3 : 0;
    }

    static AllIndicesManifest buildManifest(List<AllIndicesManifest.IndexEntry> entries) {
        return buildManifest(entries, entries.stream().map(AllIndicesManifest.IndexEntry::indexName).toList(), false);
    }

    /** {@code aliasMode}: --mounted-shells skip-if-original, the only mode whose manifest has {@code aliasCount}. */
    static AllIndicesManifest buildManifest(List<AllIndicesManifest.IndexEntry> entries, List<String> planned,
                                            boolean aliasMode) {
        int completed = 0, completedWithQuarantine = 0, failed = 0, aliases = 0;
        for (AllIndicesManifest.IndexEntry entry : entries) {
            switch (entry.outcome()) {
                case COMPLETED -> completed++;
                case COMPLETED_WITH_QUARANTINE -> completedWithQuarantine++;
                case FAILED -> failed++;
                case ALIAS_OF -> aliases++;
            }
        }
        var processed = entries.stream().map(AllIndicesManifest.IndexEntry::indexName).collect(java.util.stream.Collectors.toSet());
        var pending = planned.stream().filter(name -> !processed.contains(name)).toList();
        int behindLatest = (int) entries.stream().filter(e -> Boolean.TRUE.equals(e.newerSnapshotAvailable())).count();
        return new AllIndicesManifest(planned.size(), completed, completedWithQuarantine, failed, List.copyOf(entries),
            pending, pending.isEmpty() && failed == 0 && completedWithQuarantine == 0, behindLatest,
            aliasMode ? aliases : null);
    }

    /**
     * Like {@link JsonReportWriter}, never propagates an I/O failure: losing the manifest must not
     * abort a long extraction that is otherwise succeeding.
     */
    private static void writeManifest(Path reportDir, AllIndicesManifest manifest) {
        try {
            Files.createDirectories(reportDir);
            Path manifestPath = reportDir.resolve("manifest.json").toAbsolutePath();
            Path tmp = Files.createTempFile(manifestPath.getParent(), "manifest", ".tmp");
            MANIFEST_MAPPER.writeValue(tmp.toFile(), manifest);
            Files.move(tmp, manifestPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException e) {
            System.err.println("warning: cannot write manifest to " + reportDir
                + " (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ") - the dump continues");
        }
    }

    /**
     * Only the ROOT's scheme (s3a:// vs local) matters for {@link #s3aConf()} - every per-index
     * sub-table lives under the same root and therefore the same scheme - so this is plain string
     * derivation of the full per-index table URI, not a second scheme detection.
     */
    static String resolveIndexTableUri(String root, String indexName) {
        if (indexName.isBlank() || indexName.equals(".") || indexName.equals("..")
            || indexName.contains("/") || indexName.contains("\\"))
            throw new IllegalArgumentException("Invalid index name in repository");
        String base = root.endsWith("/") ? root.substring(0, root.length() - 1) : root;
        return base + "/" + indexName;
    }

    /**
     * Elasticsearch index names can't contain "/" and reject leading "-"/"_"/"+" and "..", which
     * likely already rules out path traversal here - but this check is cheap defense in depth
     * anyway, since the name is coming from repository metadata this process doesn't fully
     * control, not from a CLI argument the operator typed themselves.
     */
    static Path resolvePerIndexReportPath(Path reportDir, String indexName) {
        Path dir = reportDir.toAbsolutePath().normalize();
        Path resolved = dir.resolve(indexName + ".json").normalize();
        if (!resolved.startsWith(dir)) {
            throw new IllegalArgumentException("Index name produces an invalid report path: " + indexName);
        }
        return resolved;
    }

    // S3A's own retry settings stay at their defaults (fs.s3a.retry.limit=7, interval 500 ms,
    // throttle limit 20): target traffic includes the non-idempotent Delta commit PUT. The source
    // client retries up to 10 attempts with 0.5-20 s full-jitter backoff, no circuit breaker and
    // Retry-After capped at 20 s (SourceRetryConfig); tune both sides together.
    private Configuration s3aConf() {
        return s3aConf(tableUri, targetEndpoint, targetCredentials, System::getenv);
    }

    static Configuration s3aConf(String tableUri, String targetEndpoint, Function<String, String> env) {
        return s3aConf(tableUri, targetEndpoint, CredentialSources.Mode.ENV, env);
    }

    /** Hadoop configuration for the target table; see CredentialSources.applyTargetCredentials. */
    static Configuration s3aConf(String tableUri, String targetEndpoint, CredentialSources.Mode targetMode,
                                 Function<String, String> env) {
        var conf = new Configuration();
        if (tableUri.startsWith("s3a://")) {
            CredentialSources.applyTargetCredentials(conf, targetMode, env);
            conf.set("fs.s3a.endpoint", resolveTargetEndpoint(targetEndpoint));
            conf.set("fs.s3a.path.style.access", "true");
            conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            S3aTargetDefaults.apply(conf);
        }
        return conf;
    }

    private BlobStore openStore(String uri) {
        return openStore(uri, sourceEndpoint, null);
    }

    /** {@code repositoryPrefix}: the --repository-env prefix of a backing repository, or null. */
    private BlobStore openStore(String uri, String endpoint, String repositoryPrefix) {
        return CredentialSources.openSourceStore(uri, endpoint, sourceCredentials, repositoryPrefix, System::getenv);
    }

    private static DumpToDeltaPipeline.ShardReaderOpener runtimeOpener() {
        return new DumpToDeltaPipeline.ShardReaderOpener() {
            @Override
            public de.sloow.gefrierschrank.core.lucene.ShardReader open(
                de.sloow.gefrierschrank.core.repo.ShardSnapshot shard, Path shardDir) throws Exception {
                return LuceneRuntimeSelector.openLucene10(shard, shardDir);
            }

            @Override
            public de.sloow.gefrierschrank.core.repo.CommitPoint readCommitPoint(
                de.sloow.gefrierschrank.core.repo.ShardSnapshot shard, Path shardDir) throws Exception {
                LuceneRuntimeSelector.requireWrittenBy(shard);
                return de.sloow.gefrierschrank.lucene10.CommitPointReader.read(shardDir);
            }

            @Override
            public de.sloow.gefrierschrank.core.lucene.ShardReader openPruned(
                de.sloow.gefrierschrank.core.repo.ShardSnapshot shard, Path shardDir,
                com.fasterxml.jackson.databind.JsonNode rawIndexMetadata) throws Exception {
                LuceneRuntimeSelector.requireWrittenBy(shard);
                return de.sloow.gefrierschrank.lucene10.Lucene10ShardReader.openPruned(shardDir);
            }
        };
    }

    /**
     * Source credentials: only the standard AWS env vars (read through {@code env}, which is
     * prefixed for a backing repository), matching DumpCommand's source-only setup.
     */
    static Credentials resolveSourceCredentials(Function<String, String> env) {
        String accessKey = env.apply("AWS_ACCESS_KEY_ID");
        String secretKey = env.apply("AWS_SECRET_ACCESS_KEY");
        if (accessKey == null || secretKey == null) {
            throw new CredentialSources.CredentialConfigurationException(
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (or PREFIX_AWS_* for --repository-env) must be set for s3:// repositories");
        }
        return new Credentials(accessKey, secretKey, token(env.apply("AWS_SESSION_TOKEN")));
    }

    /**
     * Target credentials: prefer TARGET_AWS_ACCESS_KEY_ID/TARGET_AWS_SECRET_ACCESS_KEY so a
     * second S3 account can be used, but fall back to the standard AWS_* vars so the common
     * single-account case (source and target on the same service) needs no extra setup. A
     * session token is only paired with keys of its own namespace.
     */
    static Credentials resolveTargetCredentials(Function<String, String> env) {
        String accessKey = env.apply("TARGET_AWS_ACCESS_KEY_ID");
        String secretKey = env.apply("TARGET_AWS_SECRET_ACCESS_KEY");
        if (accessKey != null && secretKey != null) {
            return new Credentials(accessKey, secretKey, token(env.apply("TARGET_AWS_SESSION_TOKEN")));
        }
        if (token(env.apply("TARGET_AWS_SESSION_TOKEN")) != null) {
            // Falling back to AWS_* here would silently drop the target token and use other keys.
            throw new CredentialSources.CredentialConfigurationException(
                "TARGET_AWS_SESSION_TOKEN is set, but TARGET_AWS_ACCESS_KEY_ID and TARGET_AWS_SECRET_ACCESS_KEY are not");
        }
        accessKey = env.apply("AWS_ACCESS_KEY_ID");
        secretKey = env.apply("AWS_SECRET_ACCESS_KEY");
        if (accessKey == null || secretKey == null) {
            throw new CredentialSources.CredentialConfigurationException(
                "Missing S3 credentials for target: set TARGET_AWS_ACCESS_KEY_ID/TARGET_AWS_SECRET_ACCESS_KEY " +
                    "(or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY as a fallback) for s3a:// targets");
        }
        return new Credentials(accessKey, secretKey, token(env.apply("AWS_SESSION_TOKEN")));
    }

    private static String token(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    static String resolveTargetEndpoint(String targetEndpoint) {
        if (targetEndpoint == null) {
            throw new IllegalArgumentException(
                "--target-endpoint must be set for s3a:// table targets (source and target are often " +
                    "different S3 services, so it is not inferred from --source-endpoint)");
        }
        return targetEndpoint;
    }

    record Credentials(String accessKey, String secretKey, String sessionToken) {}

    static Function<String, String> repositoryEnvironment(String prefix, Function<String, String> env) {
        if (prefix == null) return env;
        if (!prefix.matches("[A-Za-z_][A-Za-z0-9_]*")) throw new IllegalArgumentException("Invalid repository environment prefix");
        return name -> env.apply(prefix + "_" + name);
    }

    /** Picocli converter for --download-concurrency; out-of-range values are usage errors. */
    static final class DownloadConcurrency implements CommandLine.ITypeConverter<Integer> {
        @Override
        public Integer convert(String value) {
            int concurrency;
            try {
                concurrency = Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                throw new CommandLine.TypeConversionException("must be a whole number between 1 and 32");
            }
            if (concurrency < 1 || concurrency > ShardMaterializer.MAX_CONCURRENCY) {
                throw new CommandLine.TypeConversionException("must be between 1 and 32");
            }
            return concurrency;
        }
    }

    private static String safeFailure(Throwable failure) {
        TargetStorageException storageFailure = targetStorageFailure(failure);
        if (storageFailure != null) return storageFailure.getClass().getSimpleName() + ": " + storageFailure.getMessage();
        Throwable root = de.sloow.gefrierschrank.core.FailureDiagnostics.rootCause(failure);
        if (root instanceof de.sloow.gefrierschrank.core.FailureDiagnostics.SafeMessage
            || root instanceof RepositorySources.UnresolvedSnapshotException
            || root instanceof de.sloow.gefrierschrank.delta.DeltaTableWriter.ArchiveTargetConflictException
            || root instanceof de.sloow.gefrierschrank.delta.TableWriteLock.TableLockedException)
            return root.getClass().getSimpleName() + ": " + root.getMessage();
        var credentialFailure = CredentialSources.configurationFailure(failure);
        if (credentialFailure != null) return CredentialSources.describe(credentialFailure);
        return de.sloow.gefrierschrank.core.FailureDiagnostics.describe(failure);
    }
}
