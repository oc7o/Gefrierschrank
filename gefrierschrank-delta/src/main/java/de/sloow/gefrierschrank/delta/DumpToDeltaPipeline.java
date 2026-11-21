package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.IndexMapping;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Streams every document of an index into a Delta table: per shard, files are
 * materialized to a temp dir, opened through the caller's {@link ShardReaderOpener}
 * (the CLI's in-process Lucene 10 reader), and each {@code _source} payload flows
 * into the Parquet/Delta writer without buffering the table.
 *
 * <p>Shards run one after another on the calling thread, or, for archive exports with a
 * {@link ShardWorkPool} of several workers, concurrently: the calling thread then only
 * dispatches, in shard order, and remains the only thread that records outcomes, prints, and
 * calls the listener. Concurrent shards decode, stage their Parquet files and commit through
 * the one shared {@link DeltaTableWriter}, which serializes only the short commits per table.
 * Exactly-once never depends on this planning: the writer refuses a shard key that is already
 * committed at commit time.
 */
public final class DumpToDeltaPipeline {

    /** Opens a reader over a materialized shard dir; dispatches the Lucene runtime. */
    public interface ShardReaderOpener {
        ShardReader open(ShardSnapshot shard, Path shardDir) throws Exception;

        default ShardReader open(ShardSnapshot shard, Path shardDir, JsonNode rawIndexMetadata) throws Exception {
            return open(shard, shardDir);
        }

        /**
         * The commit facts of a shard directory holding at least its commit files, or null when this
         * runtime cannot read them (then there is no commit-point preflight and the plan is full).
         */
        default de.sloow.gefrierschrank.core.repo.CommitPoint readCommitPoint(ShardSnapshot shard, Path shardDir)
                throws Exception {
            return null;
        }

        /** A reader over a selectively fetched directory, which lacks the files of the pruned structures. */
        default ShardReader openPruned(ShardSnapshot shard, Path shardDir, JsonNode rawIndexMetadata) throws Exception {
            return open(shard, shardDir, rawIndexMetadata);
        }
    }

    /**
     * Enumerates the shards of an index to dump. Exists as a seam between this class and
     * {@link RepositoryInspector} purely so tests can simulate an index with more shards than
     * any single index in a fixture repository provides, without touching
     * RepositoryInspector itself.
     */
    interface ShardCatalog {
        int shardCount();
        ShardSnapshot shardSnapshot(int shardId) throws IOException;
    }

    private static final long ADMISSION_POLL_MILLIS = 200;

    private final RepositoryInspector inspector;
    private final BlobStore blobs;
    private final DeltaTableWriter writer;
    private final Configuration hadoopConf;
    private final ShardReaderOpener opener;
    private ShardWorkPool pool;
    private DocumentMemoryGate memoryGate = DocumentMemoryGate.shared();

    public DumpToDeltaPipeline(RepositoryInspector inspector, BlobStore blobs,
                               String tableUri, Configuration hadoopConf,
                               ShardReaderOpener opener) {
        this(inspector, blobs, new DeltaTableWriter(tableUri), hadoopConf, opener);
    }

    /** Commits through {@code writer}, e.g. the instance that already checked the table's snapshot. */
    public DumpToDeltaPipeline(RepositoryInspector inspector, BlobStore blobs,
                               DeltaTableWriter writer, Configuration hadoopConf,
                               ShardReaderOpener opener) {
        this.inspector = inspector;
        this.blobs = blobs;
        this.writer = writer;
        this.hadoopConf = hadoopConf;
        this.opener = opener;
    }

    /**
     * Runs the shards of archive exports ({@link #runSnapshot}) concurrently within {@code pool}.
     * Without a pool, or with one worker, shards run inline on the calling thread as before.
     */
    DumpToDeltaPipeline withWorkPool(ShardWorkPool pool) {
        this.pool = pool;
        return this;
    }

    /** Test seam: a gate other than the JVM-wide one, e.g. with a small budget. */
    DumpToDeltaPipeline withDocumentMemoryGate(DocumentMemoryGate gate) {
        this.memoryGate = gate;
        return this;
    }

    /**
     * Dumps every shard of the index into the target Delta table, skipping shards already
     * committed by a previous (crashed) run - see {@link ShardKey} for how "already committed"
     * is determined from the Delta log alone.
     *
     * <p>Returns a {@link RunReport} covering only <em>this call</em> - not rows/bytes
     * contributed by previously-committed shards that were skipped: that's what tells an
     * operator watching a long unattended export whether a given invocation actually made
     * progress.
     */
    public RunReport run(String indexName) throws Exception {
        return run(indexName, RunReportListener.NO_OP);
    }

    /** Same as {@link #run(String)}, but invokes {@code listener} after every shard's outcome is decided. */
    public RunReport run(String indexName, RunReportListener listener) throws Exception {
        var index = inspector.overview().indices().stream()
            .filter(i -> i.name().equals(indexName))
            .findFirst()
            .orElseThrow(() -> new NoSuchElementException("Index not in repository: " + indexName));

        IndexMapping mapping;
        com.fasterxml.jackson.databind.JsonNode rawIndexMetadata;
        try {
            mapping = inspector.indexMapping(indexName);
        } catch (IOException e) {
            mapping = null;  // fall back to first-document inference; don't abort a
                              // long unattended export over a metadata hiccup
        }
        try {
            // Resolved once per index, not per shard, same reasoning as `mapping` above - and
            // reused for the synthetic-source reconstruction fallback (see
            // SyntheticSourceFallbackShardReader), which needs the FULL metadata (mappings AND
            // settings), not just the field properties `mapping` narrows down to.
            rawIndexMetadata = inspector.indexMetadata(indexName);
        } catch (IOException e) {
            rawIndexMetadata = null;  // no reconstruction fallback available for this run; a
                                       // shard that turns out to need it still fails exactly as
                                       // it always has (see SyntheticSourceFallbackShardReader.wrap)
        }

        int shardCount = index.shardGenerations().size();
        ShardCatalog catalog = new ShardCatalog() {
            @Override public int shardCount() {
                return shardCount;
            }

            @Override public ShardSnapshot shardSnapshot(int shardId) throws IOException {
                return inspector.shardSnapshot(indexName, shardId);
            }
        };
        return run(indexName, mapping, catalog, listener, rawIndexMetadata);
    }

    /**
     * Package-private seam for tests: identical to {@link #run(String)} but with an injectable
     * shard catalog, so a multi-shard, partial-completion scenario can be tested without needing
     * a fixture repository that actually has that many shards.
     */
    RunReport run(String indexName, IndexMapping mapping, ShardCatalog catalog) throws Exception {
        return run(indexName, mapping, catalog, RunReportListener.NO_OP, null);
    }

    RunReport run(String indexName, IndexMapping mapping, ShardCatalog catalog, RunReportListener listener) throws Exception {
        return run(indexName, mapping, catalog, listener, null);
    }

    /**
     * @param rawIndexMetadata full per-index metadata (mappings + settings) for the
     *                         synthetic-source reconstruction fallback, or {@code null} to
     *                         disable it entirely (a shard needing it then fails exactly as it
     *                         did before that fallback existed - see
     *                         {@link SyntheticSourceFallbackShardReader#wrap}). Test-only seams
     *                         above default this to {@code null} deliberately: they exercise
     *                         resumability/quarantine behavior, which is orthogonal to
     *                         reconstruction and shouldn't need a real ES mapping to pass.
     */
    RunReport run(String indexName, IndexMapping mapping, ShardCatalog catalog, RunReportListener listener,
                  com.fasterxml.jackson.databind.JsonNode rawIndexMetadata) throws Exception {
        return run(indexName, mapping, catalog, listener, rawIndexMetadata, null, null);
    }

    /** Called after exact snapshot and mounted-reference resolution. */
    public RunReport runSnapshot(ArchiveContext context, JsonNode metadata, int shardCount,
                                 RunReportListener listener) throws Exception {
        return runSnapshot(context, metadata, shardCount, listener, null);
    }

    /**
     * @param committedShardKeys the table's committed keys as the caller just read them through
     *                           this pipeline's writer, so the run does not scan the Delta log
     *                           again; {@code null} to read them here
     */
    RunReport runSnapshot(ArchiveContext context, JsonNode metadata, int shardCount, RunReportListener listener,
                          Set<String> committedShardKeys) throws Exception {
        ShardCatalog catalog = new ShardCatalog() {
            public int shardCount() { return shardCount; }
            public ShardSnapshot shardSnapshot(int shardId) throws IOException {
                return inspector.shardSnapshot(context.sourceIndex(), shardId, context.sourceSnapshotUuid());
            }
        };
        return run(context.indexName(), ArchiveContext.mapping(), catalog, listener, metadata, context,
            committedShardKeys);
    }

    /** Package-private seam for tests: an archive export over an injectable shard catalog. */
    RunReport runSnapshot(ArchiveContext context, JsonNode metadata, ShardCatalog catalog,
                          RunReportListener listener) throws Exception {
        return run(context.indexName(), ArchiveContext.mapping(), catalog, listener, metadata, context, null);
    }

    private RunReport run(String indexName, IndexMapping mapping, ShardCatalog catalog, RunReportListener listener,
                  JsonNode rawIndexMetadata, ArchiveContext archive, Set<String> knownCommittedShardKeys)
            throws Exception {
        // Resolved before any shard, so an invalid setting fails the index once.
        int shardThreads = archive == null ? 1 : ShardRanges.requested(hadoopConf, workers());
        var inputs = new RunInputs(indexName, mapping, rawIndexMetadata, archive, new ShardMaterializer(blobs), shardThreads);
        // Before any shard: an index whose excluded vectors cannot come back fails here, and a table
        // that already holds rows without them (1.0.0) is not completed with rows that have them.
        SourceVectorsPlan vectors = SourceVectorsPlan.forIndex(indexName, rawIndexMetadata);
        vectors.requireExportable();
        if (archive != null && vectors.rehydrates()) {
            writer.requireTaggedFiles(hadoopConf, SourceVectorsPlan.FILE_TAG, SourceVectorsPlan.FILE_TAG_VALUE);
            writer.withFileTags(java.util.Map.of(SourceVectorsPlan.FILE_TAG, SourceVectorsPlan.FILE_TAG_VALUE));
        }
        // Single scan of the Delta log up front, not per-shard: for an index with many shards,
        // re-scanning the whole log for every shard would be wasteful. The log is the only
        // source of truth for "what's committed" - no separate checkpoint file is kept, so
        // there's no crash window between a commit landing and bookkeeping about it landing.
        // A set the caller read moments earlier through the same writer is that same scan; it only
        // plans skips, since the writer refuses an already committed key at commit time anyway.
        Set<String> committedShardKeys = knownCommittedShardKeys != null
            ? knownCommittedShardKeys : writer.committedShardKeys(hadoopConf);
        var progress = new Progress(indexName, Instant.now(), catalog.shardCount(), listener);
        // Only archive exports run shards concurrently: first-document inference fixes the table
        // schema from whichever shard commits first, so that legacy path stays serial.
        var dispatch = archive != null && pool != null && pool.workers() > 1 ? new Dispatch(pool, progress, inputs) : null;

        try {
            for (int shardId = 0; shardId < progress.totalShards; shardId++) {
                if (dispatch != null && dispatch.indexFailure != null) break;
                Instant shardStarted = Instant.now();
                ShardSnapshot shard;
                try {
                    shard = catalog.shardSnapshot(shardId);
                } catch (Exception e) {
                    // Resolving one shard's metadata can fail independently (a missing or corrupt
                    // snap-*.dat). Quarantine it like any other per-shard failure rather than letting
                    // it propagate: otherwise a single bad shard discards the whole index's report,
                    // including the outcomes of every shard already processed in this run.
                    Throwable rootCause = rootCauseOf(e);
                    Instant shardFinished = Instant.now();
                    System.out.println("Quarantined shard " + shardId + " of " + indexName
                        + ": cannot resolve shard metadata (" + rootCause.getClass().getSimpleName() + ")");
                    progress.add(new RunReport.ShardReport(shardId, null,
                        RunReport.Outcome.QUARANTINED, 0, 0,
                        shardFinished.toString(), Duration.between(shardStarted, shardFinished).toMillis(),
                        rootCause.getClass().getSimpleName(), describedErrorMessage(rootCause), 0L, NO_FETCH,
                        null, null, null), 0, 0);
                    continue;
                }
                String shardKey = archive == null ? ShardKey.of(indexName, shardId, shard.snapshotName()) : archive.key(shardId);

                // Checked before materializing: on a large repository we must not re-download/re-materialize
                // an already-committed shard's segment files only to discover the result is discarded.
                if (committedShardKeys.contains(shardKey)) {
                    System.out.println("Skipping shard " + shardId + " of " + indexName
                        + ": already committed (" + shardKey + ")");
                    Instant shardChecked = Instant.now();
                    progress.add(new RunReport.ShardReport(shardId, shard.snapshotName(),
                        RunReport.Outcome.SKIPPED_ALREADY_COMMITTED, 0, 0,
                        shardChecked.toString(), Duration.between(shardStarted, shardChecked).toMillis(),
                        null, null, 0L, NO_FETCH, null, null, null), 0, 0);
                    continue;
                }

                var task = new ShardTask(shardId, shard, shardKey, Duration.between(shardStarted, Instant.now()));
                if (dispatch == null) progress.record(runShard(inputs, task));
                else dispatch.submit(task);
            }
        } catch (Exception dispatcherFailure) {
            // Shards already started keep writing to this table: let them finish and record before
            // the index fails, as for an index-fatal shard failure. A task's Error is not drained.
            if (dispatch != null) dispatch.drainAfter(dispatcherFailure);
            throw dispatcherFailure;
        }
        if (dispatch != null) dispatch.finish();
        // End of run: every shard outcome is recorded and no shard task still writes to this table.
        writer.reconcilePendingCleanup(hadoopConf);
        return progress.buildReport();
    }

    /**
     * The per-shard body, run inline or on a pool thread: temp dir, checks, materialize with
     * checksum verification, open, write and commit, close. Per-shard failures become a
     * QUARANTINED outcome; an {@link Error}, a failure to create the temp dir, and a failure
     * caused by {@link DeltaTableWriter.ArchiveTargetConflictException} (the table belongs to
     * another snapshot) or {@link TargetStorageException} (the target cannot store data), after
     * which every further shard would fail the same way, propagate and end the index. Touches no
     * run-wide state: stdout lines and warnings travel in the outcome.
     */
    private ShardOutcome runShard(RunInputs inputs, ShardTask task) throws Exception {
        Instant taskStarted = Instant.now();
        ShardSnapshot shard = task.shard();
        // A missing commit is not evidence of an empty index. Mounted references must
        // already have been resolved before reaching this loop.
        boolean hasLuceneCommit = shard.files().stream().anyMatch(f -> f.physicalName().startsWith("segments_"));
        boolean hasVersion = shard.files().stream().anyMatch(f -> f.writtenBy() != null);

        List<String> warnings = new ArrayList<>();
        var opening = new ShardOpening.Tracker();
        Path shardDir = de.sloow.gefrierschrank.core.TempRoot.newShardDirectory();
        try {
            try {
                if (!hasLuceneCommit) throw new MissingLuceneCommitException();
                if (!hasVersion) throw new IOException("Lucene files exist but their runtime version is missing");
                ShardOpening.Opened opened = ShardOpening.open(inputs.materializer(), opener, shard, shardDir,
                    inputs.indexName(), inputs.rawIndexMetadata(), opening);
                String probeNote = probeNote(task, inputs.indexName(), opened);
                if (probeNote != null) warnings.add(probeNote);
                int warningsBefore = warnings.size();
                var counts = new ShardCounts();
                DeltaTableWriter.WriteResult result;
                ShardReader reader = exportReader(inputs, opened.rawReader(), shardDir);
                try {
                    result = write(inputs, task, reader, shardDir, warnings, counts);
                } catch (Exception | Error failure) {
                    try { reader.close(); } catch (Exception close) { failure.addSuppressed(close); }
                    // A selectively fetched shard is completed in place and read once more with the
                    // normal open; only a second failure quarantines it.
                    if (!retriesInRun(opened, failure, task)) throw failure;
                    warnings.subList(warningsBefore, warnings.size()).clear();
                    counts = new ShardCounts();
                    try {
                        opened.fetch().ensureComplete();
                        reader = exportReader(inputs, opener.open(shard, shardDir, inputs.rawIndexMetadata()), shardDir);
                    } catch (Exception | Error reopen) {
                        reopen.addSuppressed(failure);
                        throw reopen;
                    }
                    try {
                        result = write(inputs, task, reader, shardDir, warnings, counts);
                    } catch (Exception | Error second) {
                        try { reader.close(); } catch (Exception close) { second.addSuppressed(close); }
                        throw second;
                    }
                }
                counts.ignoredFieldDocuments = SyntheticSourceFallbackShardReader.ignoredFieldDocuments(reader);
                if (counts.ignoredFieldDocuments != null && !result.alreadyCommitted())
                    warnings.add(SyntheticSourceFallbackShardReader.ignoredFieldNote(inputs.indexName(), counts.ignoredFieldDocuments));
                counts.unrecoverableUnmappedValues = SyntheticSourceFallbackShardReader.unrecoverableUnmappedValues(reader);
                try { reader.close(); } catch (Exception close) {
                    warnings.add("warning: shard committed, but closing its reader failed ("
                        + close.getClass().getSimpleName() + ")");
                }
                Instant shardFinished = Instant.now();
                if (result.alreadyCommitted()) {
                    return new ShardOutcome(new RunReport.ShardReport(task.shardId(), shard.snapshotName(),
                        RunReport.Outcome.SKIPPED_ALREADY_COMMITTED, 0, shard.totalSize(),
                        shardFinished.toString(), task.elapsedMillis(taskStarted, shardFinished),
                        null, null, opening.bytesFetched(), opening.fetchMode(), null, null, null), 0, shard.totalSize(),
                        "Skipping shard " + task.shardId() + " of " + inputs.indexName()
                            + ": committed by another writer (" + task.shardKey() + ")", warnings);
                }
                long rows = result.rows();
                return new ShardOutcome(new RunReport.ShardReport(task.shardId(), shard.snapshotName(),
                    RunReport.Outcome.COMMITTED, rows, shard.totalSize(),
                    shardFinished.toString(), task.elapsedMillis(taskStarted, shardFinished),
                    null, null, opening.bytesFetched(), opening.fetchMode(),
                    counts.convertedSources > 0 ? counts.convertedSources : null, counts.ignoredFieldDocuments,
                    counts.unrecoverableUnmappedValues), rows, shard.totalSize(), null, warnings);
            } catch (Exception e) {
                // Per-shard isolation: a single bad shard (corrupt segment, unsupported codec,
                // transient S3 error) must not abort a long run over every other shard.
                // Deliberately catches Exception, not Throwable - an OutOfMemoryError or similar
                // VM-level failure should still kill the process rather than risk continuing in a
                // corrupted JVM state. Nothing is added to committedShardKeys here (nothing was
                // committed), so this shard is indistinguishable from a never-attempted one to the
                // resumability check above - a later run naturally retries it, same as after a crash.
                Throwable rootCause = rootCauseOf(e);
                var storage = de.sloow.gefrierschrank.core.FailureDiagnostics.find(e, TargetStorageException.class);
                if (storage != null) throw storage;
                if (rootCause instanceof DeltaTableWriter.ArchiveTargetConflictException conflict) throw conflict;
                Instant shardFinished = Instant.now();
                return new ShardOutcome(new RunReport.ShardReport(task.shardId(), shard.snapshotName(),
                    RunReport.Outcome.QUARANTINED, 0, shard.totalSize(),
                    shardFinished.toString(), task.elapsedMillis(taskStarted, shardFinished),
                    rootCause.getClass().getSimpleName(), describedErrorMessage(rootCause),
                    opening.bytesFetched(), opening.fetchMode(), null, null, null), 0, 0,
                    "Quarantined shard " + task.shardId() + " of " + inputs.indexName()
                        + " (" + task.shardKey() + "): " + rootCause.getClass().getSimpleName(), warnings);
            }
        } finally {
            try { deleteRecursively(shardDir); } catch (Exception cleanup) {
                warnings.add("warning: temporary shard cleanup failed ("
                    + cleanup.getClass().getSimpleName() + ")");
            }
        }
    }

    /**
     * The content-free note of a synthetic-source candidate of {@code --selective-fetch}: why it was
     * fetched in full, or which structures its probe kept; null for every other shard.
     */
    private static String probeNote(ShardTask task, String indexName, ShardOpening.Opened opened) {
        String shard = "note: shard " + task.shardId() + " of " + indexName + ": ";
        if (opened.fullReason() != null) return shard + "fetched in full (" + opened.fullReason() + ")";
        ShardOpening.Probing probes = opened.probes();
        if (probes == null) return null;
        String kept = probes.kept().isEmpty() ? "no index structure"
            : probes.kept().stream().sorted().map(s -> s.name().toLowerCase(java.util.Locale.ROOT)).collect(java.util.stream.Collectors.joining(", "));
        return shard + "selective fetch kept " + kept + " (" + probes.calls() + " probe call(s), " + probes.millis() + " ms)";
    }

    /** The report's {@code fetch} of a shard that never reached a download in this run. */
    private static final String NO_FETCH = "none";

    /**
     * The export's reader stack over a runtime reader. When the wrap throws, it has already closed
     * {@code raw}, so the caller never closes it again.
     */
    private ShardReader exportReader(RunInputs inputs, ShardReader raw, Path shardDir) throws IOException {
        ShardReader reader = SyntheticSourceFallbackShardReader.wrap(raw, shardDir, inputs.indexName(), inputs.rawIndexMetadata());
        // The _ignored note goes into the shard's warnings after a committed write, never from an attempt that fails.
        SyntheticSourceFallbackShardReader.deferIgnoredFieldNote(reader);
        return reader;
    }

    private DeltaTableWriter.WriteResult write(RunInputs inputs, ShardTask task, ShardReader reader, Path shardDir,
                                               List<String> warnings, ShardCounts counts) throws Exception {
        return inputs.archive() == null
            ? new DeltaTableWriter.WriteResult(writer.write(hadoopConf,
                new ShardSourceIterator(reader, null, task.shardId()), inputs.mapping(), task.shardKey()), false)
            : writeArchive(inputs, task, reader, shardDir, warnings, counts);
    }

    /**
     * Whether a failed write is read again in this run: only a selectively fetched shard that was
     * not already completed (a pruned open that fell back to the normal open read the complete
     * directory), never for a failure that would end the index, a document too large for the heap
     * or an Error, and never once its key landed in the table (that failure is reported as without
     * selective fetch).
     */
    private boolean retriesInRun(ShardOpening.Opened opened, Throwable failure, ShardTask task) {
        if (!opened.selective() || opened.fetch().completed() || failure instanceof Error) return false;
        if (de.sloow.gefrierschrank.core.FailureDiagnostics.find(failure, TargetStorageException.class) != null
            || de.sloow.gefrierschrank.core.FailureDiagnostics.find(failure, DeltaTableWriter.ArchiveTargetConflictException.class) != null
            || de.sloow.gefrierschrank.core.FailureDiagnostics.find(failure, DocumentMemoryGate.DocumentTooLargeException.class) != null)
            return false;
        try {
            return !writer.committedShardKeys(hadoopConf).contains(task.shardKey());
        } catch (RuntimeException unreadable) {
            failure.addSuppressed(unreadable);
            return false;
        }
    }

    /** Per-attempt counts for the shard report; a retried shard reports its retry's counts only. */
    private static final class ShardCounts {
        long convertedSources;
        Long ignoredFieldDocuments;
        Boolean unrecoverableUnmappedValues;
    }

    /**
     * The archive row path: each document's stored bytes become its row, and only documents the
     * {@link SourceValidator} does not accept go through the per-document String path. A
     * content-free count of those goes to stderr with the shard's outcome.
     *
     * <p>With more than one requested shard thread, the shard is read as contiguous doc-id ranges
     * (see {@link ShardRanges}), each through its own fork of {@code reader}, staged in parallel
     * and committed once. Each range closes its fork on its own thread as soon as it is staged, so
     * no range's last decompressed block outlives the range; forks no range took over are closed
     * here. Every fork is closed before this method returns, so before the caller closes
     * {@code reader}. A reader that cannot fork is read as one range. If a large document does not
     * fit the heap beside the shard's other ranges, the shard is read again as one range, which has
     * the budget of a single range to itself. A large document that waits for heap waits in a
     * temporary file in {@code shardDir}.
     */
    private DeltaTableWriter.WriteResult writeArchive(RunInputs inputs, ShardTask task, ShardReader reader, Path shardDir,
                                                      List<String> warnings, ShardCounts counts) throws IOException {
        List<ShardRanges.ArchiveRowIterator> ranges = new ArrayList<>();
        List<ShardReader> unownedForks = new ArrayList<>();
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        // A synthetic-source range's reader holds a reconstruction batch ahead of it; the heap bound
        // and the memory gate count it like the rest of the range's own heap.
        long readAhead = ShardRanges.readAheadBytes(inputs.rawIndexMetadata(),
            SourceVectorsPlan.forIndex(inputs.indexName(), inputs.rawIndexMetadata()));
        try {
            // A reader that cannot fork is read as one range without the planning pass.
            ShardReader firstFork = inputs.shardThreads() > 1 ? reader.fork() : null;
            if (firstFork != null) unownedForks.add(firstFork);
            ShardRanges.Plan plan = firstFork != null
                ? ShardRanges.plan(reader, inputs.shardThreads(), Runtime.getRuntime().maxMemory(), workers(), readAhead)
                : null;
            if (plan != null && plan.k() > 1) {
                for (int range = 1; range < plan.k(); range++) {
                    ShardReader fork = reader.fork();
                    if (fork == null) break;
                    unownedForks.add(fork);
                }
            }
            if (plan == null || plan.k() == 1 || unownedForks.size() < plan.k()) {
                ranges.add(new ShardRanges.ArchiveRowIterator(reader, 0, -1, null, memoryGate, shardDir, false, readAhead));
            } else {
                for (int range = 0; range < plan.k(); range++) {
                    ranges.add(new ShardRanges.ArchiveRowIterator(unownedForks.get(range), plan.bounds()[range],
                        plan.bounds()[range + 1], cancelled, memoryGate, shardDir, true, readAhead));
                }
                unownedForks.clear();
            }
            try {
                return writer.writeArchive(hadoopConf, ranges, () -> cancelled.set(true), inputs.archive(),
                    task.shardId(), task.shardKey());
            } catch (IOException | RuntimeException failure) {
                if (ranges.size() == 1 || !(rootCauseOf(failure) instanceof DocumentMemoryGate.DocumentTooLargeException))
                    throw failure;
                // Every range has ended and its files are gone. A fresh fork reads from the first
                // document again, on this thread, as the shard's only range.
                ShardReader single = reader.fork();
                if (single == null) throw failure;
                warnings.add("warning: shard " + task.shardId() + " of " + inputs.indexName() + ": a large document"
                    + " did not fit the heap beside " + (ranges.size() - 1) + " other range(s); read again as one range");
                closeRanges(ranges, task, warnings);
                ranges.clear();
                ranges.add(new ShardRanges.ArchiveRowIterator(single, 0, -1, null, memoryGate, shardDir, true, readAhead));
                return writer.writeArchive(hadoopConf, ranges, () -> {}, inputs.archive(), task.shardId(), task.shardKey());
            }
        } finally {
            closeRanges(ranges, task, warnings);
            for (ShardReader fork : unownedForks) {
                try { fork.close(); } catch (Exception close) {
                    warnings.add(rangeReaderCloseWarning(task, close));
                }
            }
            long perDocument = ranges.stream().mapToLong(ShardRanges.ArchiveRowIterator::perDocumentValidations).sum();
            if (perDocument > 0) {
                warnings.add("warning: shard " + task.shardId() + " of " + inputs.indexName() + ": " + perDocument
                    + " documents are not strict UTF-8 JSON and were validated one by one, as before"
                    + " (malformed UTF-8 is exported with U+FFFD replacement characters, and a document that is"
                    + " not a JSON object fails the shard)");
            }
            long[] converted = ShardRanges.convertedSources(ranges);
            if (converted[0] + converted[1] + converted[2] > 0) {
                warnings.add("note: shard " + task.shardId() + " of " + inputs.indexName() + ": " + converted[0] + " SMILE, "
                    + converted[1] + " CBOR, " + converted[2] + " YAML documents converted to JSON (not byte-exact;"
                    + " _source_kind stored-smile/stored-cbor/stored-yaml)");
            }
            counts.convertedSources = converted[0] + converted[1] + converted[2];
        }
    }

    /** Closes every range (a range that owns its fork closes it, once) and reports forks that failed to close. */
    private static void closeRanges(List<ShardRanges.ArchiveRowIterator> ranges, ShardTask task, List<String> warnings) {
        for (var range : ranges) {
            range.close();
            if (range.readerCloseFailure() != null) warnings.add(rangeReaderCloseWarning(task, range.readerCloseFailure()));
        }
    }

    private static String rangeReaderCloseWarning(ShardTask task, Exception failure) {
        return "warning: closing a range reader of shard " + task.shardId() + " failed ("
            + failure.getClass().getSimpleName() + ")";
    }

    /**
     * Temp disk a shard occupies while it is processed: its materialized files.
     */
    static long tempEstimateBytes(ShardSnapshot shard) {
        return tempEstimateBytes(shard, null, null);
    }

    /**
     * With selective fetch, an eligible index's shard is estimated by name: every file but those
     * of the pruned structures. A shard that then takes the full plan or completes its download
     * exceeds the estimate by at most its pruned bytes. A synthetic-source candidate (synthetic
     * signal, {@code _source} not retained) is never eligible and keeps the full size: its plan is
     * known only after the probe, and the lease is taken before.
     */
    static long tempEstimateBytes(ShardSnapshot shard, String indexName, JsonNode metadata) {
        if (de.sloow.gefrierschrank.core.repo.SelectiveFetch.enabled() && SelectiveEligibility.of(indexName, metadata))
            return de.sloow.gefrierschrank.core.repo.SelectiveFetch.estimateBytes(shard);
        return shard.totalSize();
    }

    /** {@code shardThreads}: requested ranges per shard, always 1 on the JSON row path. */
    private record RunInputs(String indexName, IndexMapping mapping, JsonNode rawIndexMetadata,
                             ArchiveContext archive, ShardMaterializer materializer, int shardThreads) {}

    /** Concurrent shards of this run. */
    private int workers() {
        return pool == null ? 1 : pool.workers();
    }

    /** {@code planning}: time spent resolving the shard's metadata before its task started. */
    private record ShardTask(int shardId, ShardSnapshot shard, String shardKey, Duration planning) {
        long elapsedMillis(Instant taskStarted, Instant finished) {
            return planning.plus(Duration.between(taskStarted, finished)).toMillis();
        }
    }

    /** One shard's result as handed to the dispatcher; {@code stdoutLine} may be null. */
    private record ShardOutcome(RunReport.ShardReport report, long rows, long bytes, String stdoutLine,
                                List<String> warnings) {}

    /**
     * Report state of one run. Only the dispatcher thread (the caller of run) touches it, prints,
     * or calls the listener, so the listener never needs to be thread-safe.
     */
    private static final class Progress {
        private final String indexName;
        private final Instant startedAt;
        private final int totalShards;
        private final RunReportListener listener;
        private final List<RunReport.ShardReport> shardReports = new ArrayList<>();
        private long totalRows;
        private long totalBytes;

        Progress(String indexName, Instant startedAt, int totalShards, RunReportListener listener) {
            this.indexName = indexName;
            this.startedAt = startedAt;
            this.totalShards = totalShards;
            this.listener = listener;
        }

        void record(ShardOutcome outcome) {
            if (outcome.stdoutLine() != null) System.out.println(outcome.stdoutLine());
            outcome.warnings().forEach(System.err::println);
            add(outcome.report(), outcome.rows(), outcome.bytes());
        }

        void add(RunReport.ShardReport report, long rows, long bytes) {
            shardReports.add(report);
            totalRows += rows;
            totalBytes += bytes;
            notifyListener();
        }

        // A listener is observability, so a misbehaving one must never abort an extraction.
        // JsonReportWriter already swallows its own I/O errors; this is defense in depth for any
        // other implementation.
        void notifyListener() {
            DumpToDeltaPipeline.notifyListener(listener, buildReport());
        }

        /**
         * Shards sorted by id, so the final report (and the last one a listener persists) equals a
         * serial run's. Concurrent shards finish out of order, so an intermediate report is a
         * subset of the final one, not a prefix.
         */
        RunReport buildReport() {
            var shards = new ArrayList<>(shardReports);
            shards.sort(java.util.Comparator.comparingInt(RunReport.ShardReport::shardId));
            return DumpToDeltaPipeline.buildReport(indexName, startedAt, totalShards, shards, totalRows, totalBytes);
        }
    }

    /**
     * Runs shard tasks on {@link ShardWorkPool} threads. Admission happens here, on the
     * dispatcher, in shard order; while waiting for a permit or budget it records finished tasks.
     * A task's Error is rethrown at once without interrupting in-flight shards (their uncommitted
     * files are orphans, as after a crash). An index-fatal exception, from a task or from the
     * dispatcher itself, stops admission, lets in-flight shards finish and record, then is rethrown.
     */
    private final class Dispatch {
        private final ShardWorkPool workers;
        private final Progress progress;
        private final RunInputs inputs;
        private final BlockingQueue<ShardWorkPool.Result<ShardOutcome>> finished = new LinkedBlockingQueue<>();
        private int inFlight;
        Exception indexFailure;

        Dispatch(ShardWorkPool workers, Progress progress, RunInputs inputs) {
            this.workers = workers;
            this.progress = progress;
            this.inputs = inputs;
        }

        void submit(ShardTask task) throws InterruptedException {
            var lease = workers.lease(tempEstimateBytes(task.shard(), inputs.indexName(), inputs.rawIndexMetadata()));
            boolean started = false;
            try {
                boolean admitted;
                do {
                    admitted = lease.tryAcquire(ADMISSION_POLL_MILLIS, TimeUnit.MILLISECONDS);
                    collect(false);
                } while (!admitted && indexFailure == null);
                if (indexFailure != null) return;
                workers.start(lease, () -> runShard(inputs, task), finished::add);
                started = true;
                inFlight++;
            } finally {
                if (!started) lease.release();
            }
        }

        void finish() throws Exception {
            while (inFlight > 0) collect(true);
            if (indexFailure != null) {
                progress.notifyListener();
                throw indexFailure;
            }
        }

        /**
         * The dispatcher itself failed, e.g. was interrupted while waiting to admit a shard: records
         * the shards still in flight, then leaves {@code failure} to the caller. Stops waiting if
         * interrupted again.
         */
        void drainAfter(Exception failure) {
            try {
                while (inFlight > 0) collect(true);
            } catch (InterruptedException interrupted) {
                failure.addSuppressed(interrupted);
                Thread.currentThread().interrupt();
                return;
            }
            if (indexFailure != null) failure.addSuppressed(indexFailure);
            progress.notifyListener();
        }

        /** Records finished tasks; with {@code block}, first waits for one if any is in flight. */
        private void collect(boolean block) throws InterruptedException {
            var next = block && inFlight > 0 ? awaitFinished() : finished.poll();
            while (next != null) {
                inFlight--;
                Throwable failure = next.failure();
                if (failure == null) {
                    progress.record(next.value());
                } else if (failure instanceof Error error) {
                    throw error;
                } else {
                    Exception exception = failure instanceof Exception e ? e : new ExecutionException(failure);
                    if (indexFailure == null) indexFailure = exception;
                    else indexFailure.addSuppressed(exception);
                }
                next = finished.poll();
            }
        }

        /**
         * The next finished task. Never waits unboundedly: a task whose result could not be queued
         * (an OutOfMemoryError allocating it) has stopped the pool instead, and its Error is rethrown.
         */
        private ShardWorkPool.Result<ShardOutcome> awaitFinished() throws InterruptedException {
            while (true) {
                var next = finished.poll(ADMISSION_POLL_MILLIS, TimeUnit.MILLISECONDS);
                if (next != null) return next;
                workers.rethrowFatal();
            }
        }
    }

    private static void notifyListener(RunReportListener listener, RunReport reportSoFar) {
        try {
            listener.onShardProcessed(reportSoFar);
        } catch (Exception e) {
            System.err.println("warning: run report listener failed ("
                + e.getClass().getSimpleName() + ") - the dump continues");
        }
    }

    private static RunReport buildReport(String indexName, Instant startedAt, int totalShards,
                                          List<RunReport.ShardReport> shardReports,
                                          long rowsWrittenThisRun, long bytesProcessedThisRun) {
        int committed = 0, skipped = 0, quarantined = 0;
        for (RunReport.ShardReport s : shardReports) {
            switch (s.outcome()) {
                case COMMITTED -> committed++;
                // Both skip reasons roll into one count; the per-shard outcome keeps them
                // distinguishable in the report without widening RunReport's shape.
                case SKIPPED_ALREADY_COMMITTED, SKIPPED_NO_DATA -> skipped++;
                case QUARANTINED -> quarantined++;
            }
        }
        Instant asOf = Instant.now();
        return new RunReport(indexName, startedAt.toString(), asOf.toString(),
            Duration.between(startedAt, asOf).toMillis(), totalShards, committed, skipped, quarantined,
            rowsWrittenThisRun, bytesProcessedThisRun, List.copyOf(shardReports));
    }

    private static Throwable rootCauseOf(Throwable failure) {
        return de.sloow.gefrierschrank.core.FailureDiagnostics.rootCause(failure);
    }

    private static String describedErrorMessage(Throwable failure) {
        return de.sloow.gefrierschrank.core.FailureDiagnostics.describe(failure);
    }

    /** Pull adapter: turns the (document, index-ordered) reader into a String iterator. */
    private static final class ShardSourceIterator implements java.util.Iterator<String> {
        private final ShardReader reader;
        private final ArchiveContext archive;
        private final int shardId;
        private int next = 0;

        ShardSourceIterator(ShardReader reader, ArchiveContext archive, int shardId) {
            this.reader = reader;
            this.archive = archive;
            this.shardId = shardId;
        }

        @Override
        public boolean hasNext() {
            try {
                while (next < reader.docCount() && !reader.isLive(next)) next++;
                return next < reader.docCount();
            } catch (IOException e) {
                throw new IllegalStateException("counting shard documents", e);
            }
        }

        @Override
        public String next() {
            if (!hasNext()) throw new NoSuchElementException();
            var holder = new String[1];
            try {
                reader.readSource(next, (bytes, length) ->
                    holder[0] = new String(bytes, 0, length, StandardCharsets.UTF_8));
                if (holder[0] == null || holder[0].isBlank()) throw new IOException("Live document has no source payload");
                if (archive != null) holder[0] = archive.document(holder[0], shardId, reader.documentMetadata(next));
            } catch (IOException e) {
                throw new IllegalStateException("reading _source of doc " + next, e);
            }
            next++;
            return holder[0];
        }
    }

    public static final class MissingLuceneCommitException extends IOException {
        public MissingLuceneCommitException() { super("No Lucene commit; backing data is missing or unresolved"); }
    }

    private static void deleteRecursively(Path dir) throws IOException {
        try (var walk = Files.walk(dir)) {
            walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
