package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves the two correctness guarantees a resumed run must uphold: no shard's rows are ever
 * committed twice, and no shard's rows are ever silently skipped. The real fixture repo only has
 * single-shard indices, so shards here are simulated via a fake {@link DumpToDeltaPipeline.ShardCatalog}
 * and {@link DumpToDeltaPipeline.ShardReaderOpener} - materializing real files from the fixture
 * repo (proving that part of the pipeline for real) while faking the document content each
 * "virtual shard" produces (so shards are trivially distinguishable in the final table).
 */
class DumpToDeltaPipelineTest {

    private static final Path FIXTURE_REPO =
        Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");
    private static final String INDEX_NAME = "test_data_7_17_9";
    private static final int ROWS_PER_SHARD = 2;

    @Test
    void resumeAfterCrashProducesExactlyTheRowsOfAnUninterruptedRun(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot template = inspector.shardSnapshot(INDEX_NAME, 0);

        int totalShards = 5;
        int crashAtShardId = 3; // shards 0,1,2 commit for real; the pipeline crashes attempting shard 3
        var catalog = virtualCatalog(template, totalShards);

        var firstRunOpener = new FakeShardReaderOpener(ROWS_PER_SHARD, crashAtShardId);
        var pipeline1 = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), firstRunOpener);
        assertThrows(SimulatedCrash.class, () -> pipeline1.run(INDEX_NAME, null, catalog));
        assertEquals(List.of(0, 1, 2), firstRunOpener.openedShardIds,
            "only the shards committed before the crash were ever opened");

        // Fresh pipeline instance pointed at the same table: simulates a process restart. Delta's
        // transaction log - not any in-memory or on-disk checkpoint - is what makes this correct.
        var secondRunOpener = new FakeShardReaderOpener(ROWS_PER_SHARD, null);
        var pipeline2 = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), secondRunOpener);
        RunReport report = pipeline2.run(INDEX_NAME, null, catalog);

        assertEquals(List.of(3, 4), secondRunOpener.openedShardIds,
            "resume must reprocess exactly the shards that never committed - not the already-committed ones, not skip extra ones");
        assertEquals((long) (totalShards - crashAtShardId) * ROWS_PER_SHARD, report.rowsWrittenThisRun(),
            "run() reports rows written by this call only, not rows already committed by the previous run");
        assertEquals(0, report.quarantinedCount(), "a clean resume quarantines nothing");

        Map<Long, Long> rowsPerShardInTable = readRowsGroupedByShard(tableUri);
        assertEquals(totalShards, rowsPerShardInTable.size(), "every shard is present in the final table exactly once");
        for (long shardId = 0; shardId < totalShards; shardId++) {
            assertEquals((long) ROWS_PER_SHARD, rowsPerShardInTable.get(shardId),
                "shard " + shardId + " must contribute exactly its own rows - no duplicates, no gaps");
        }
    }

    @Test
    void firstEverRunAgainstANonExistentTableWorks(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("brand-new-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot template = inspector.shardSnapshot(INDEX_NAME, 0);
        var catalog = virtualCatalog(template, 2);
        var opener = new FakeShardReaderOpener(ROWS_PER_SHARD, null);
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        RunReport report = pipeline.run(INDEX_NAME, null, catalog);

        assertEquals(2L * ROWS_PER_SHARD, report.rowsWrittenThisRun());
        assertEquals(List.of(0, 1), opener.openedShardIds);
    }

    @Test
    void aShardThatThrowsIsQuarantinedNotFatal(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot template = inspector.shardSnapshot(INDEX_NAME, 0);

        int totalShards = 5;
        int badShardId = 2;
        var catalog = virtualCatalog(template, totalShards);
        var opener = new FakeShardReaderOpener(ROWS_PER_SHARD, null, Set.of(badShardId));
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        RunReport report = pipeline.run(INDEX_NAME, null, catalog);

        assertEquals(List.of(0, 1, 3, 4), opener.openedShardIds,
            "the bad shard is skipped over, but every other shard still gets processed in the same call");
        assertEquals(totalShards, report.totalShards());
        assertEquals(4, report.committedCount());
        assertEquals(0, report.skippedCount());
        assertEquals(1, report.quarantinedCount());
        assertEquals((long) 4 * ROWS_PER_SHARD, report.rowsWrittenThisRun());

        RunReport.ShardReport badShard = report.shards().stream()
            .filter(s -> s.shardId() == badShardId).findFirst().orElseThrow();
        assertEquals(RunReport.Outcome.QUARANTINED, badShard.outcome());
        assertEquals(FakeShardReaderOpener.SimulatedShardFailure.class.getSimpleName(), badShard.errorClass());
        assertNotNull(badShard.errorMessage());
        assertEquals(0, badShard.rowsWritten());
    }

    @Test
    void aQuarantinedShardIsReattemptedOnTheNextRunNotSkipped(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot template = inspector.shardSnapshot(INDEX_NAME, 0);

        int totalShards = 3;
        int badShardId = 1;
        var catalog = virtualCatalog(template, totalShards);

        var firstRunOpener = new FakeShardReaderOpener(ROWS_PER_SHARD, null, Set.of(badShardId));
        var pipeline1 = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), firstRunOpener);
        RunReport firstReport = pipeline1.run(INDEX_NAME, null, catalog);
        assertEquals(1, firstReport.quarantinedCount());

        // Fresh pipeline, same table, opener that no longer fails: proves the quarantined shard's
        // key was never recorded as committed, so it's reattempted exactly like a never-attempted shard.
        var secondRunOpener = new FakeShardReaderOpener(ROWS_PER_SHARD, null);
        var pipeline2 = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), secondRunOpener);
        RunReport secondReport = pipeline2.run(INDEX_NAME, null, catalog);

        assertEquals(List.of(badShardId), secondRunOpener.openedShardIds,
            "only the previously-quarantined shard is reattempted - the others were already committed");
        assertEquals(1, secondReport.committedCount());
        assertEquals(2, secondReport.skippedCount());
        assertEquals(0, secondReport.quarantinedCount());

        Map<Long, Long> rowsPerShardInTable = readRowsGroupedByShard(tableUri);
        assertEquals(totalShards, rowsPerShardInTable.size(), "the previously-quarantined shard ends up committed too - no data loss");
    }

    @Test
    void multipleIndependentShardFailuresAreAllReportedAlongsideSuccessfulShards(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot template = inspector.shardSnapshot(INDEX_NAME, 0);

        int totalShards = 5;
        var catalog = virtualCatalog(template, totalShards);
        var opener = new FakeShardReaderOpener(ROWS_PER_SHARD, null, Set.of(1, 3));
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        RunReport report = pipeline.run(INDEX_NAME, null, catalog);

        assertEquals(List.of(0, 2, 4), opener.openedShardIds);
        assertEquals(3, report.committedCount());
        assertEquals(2, report.quarantinedCount());
        assertEquals(0, report.skippedCount());

        Set<Integer> quarantinedIds = report.shards().stream()
            .filter(s -> s.outcome() == RunReport.Outcome.QUARANTINED)
            .map(RunReport.ShardReport::shardId)
            .collect(java.util.stream.Collectors.toSet());
        assertEquals(Set.of(1, 3), quarantinedIds);

        Set<Integer> committedIds = report.shards().stream()
            .filter(s -> s.outcome() == RunReport.Outcome.COMMITTED)
            .map(RunReport.ShardReport::shardId)
            .collect(java.util.stream.Collectors.toSet());
        assertEquals(Set.of(0, 2, 4), committedIds);
    }

    @Test
    void listenerIsInvokedAfterEveryShardWithProgressReflectingProcessedShardsSoFar(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot template = inspector.shardSnapshot(INDEX_NAME, 0);

        int totalShards = 3;
        var catalog = virtualCatalog(template, totalShards);
        var opener = new FakeShardReaderOpener(ROWS_PER_SHARD, null);
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        List<Integer> processedCountsSeen = new ArrayList<>();
        RunReport finalReport = pipeline.run(INDEX_NAME, null, catalog,
            reportSoFar -> processedCountsSeen.add(reportSoFar.shards().size()));

        assertEquals(List.of(1, 2, 3), processedCountsSeen,
            "the listener fires once per shard, each time with the report grown by exactly one shard");
        assertEquals(3, finalReport.shards().size());
    }

    /**
     * A listener that throws must not abort the index: observability must never destroy the
     * extraction it observes.
     */
    @Test
    void aThrowingListenerDoesNotAbortTheRun(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot template = inspector.shardSnapshot(INDEX_NAME, 0);

        int totalShards = 3;
        var catalog = virtualCatalog(template, totalShards);
        var opener = new FakeShardReaderOpener(ROWS_PER_SHARD, null);
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        RunReport report = pipeline.run(INDEX_NAME, null, catalog, reportSoFar -> {
            throw new RuntimeException("listener exploded");
        });

        assertEquals(totalShards, report.committedCount(), "every shard still commits despite the listener throwing");
        assertEquals((long) totalShards * ROWS_PER_SHARD, report.rowsWrittenThisRun());
    }

    /**
     * A mounted searchable snapshot ("partial-" index) backs up as a metadata shell with no
     * Lucene files, so no file carries written_by.
     */
    @Test
    void aShardWithNoLuceneCommitFailsInsteadOfClaimingAnEmptyExport(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot real = inspector.shardSnapshot(INDEX_NAME, 0);

        // A mounted shell has no Lucene commit; removing version metadata from real
        // segment files would instead be corruption and must not be treated as no data.
        List<de.sloow.gefrierschrank.core.repo.RepoFileInfo> stripped = List.of();
        var catalog = new DumpToDeltaPipeline.ShardCatalog() {
            @Override public int shardCount() {
                return 1;
            }

            @Override public ShardSnapshot shardSnapshot(int shardId) {
                return new ShardSnapshot(real.repoUuid(), real.shardPrefix(), "partial-mount",
                    real.indexVersion(), real.totalSize(), stripped);
            }
        };
        var opener = new FakeShardReaderOpener(ROWS_PER_SHARD, null);
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        RunReport report = pipeline.run(INDEX_NAME, null, catalog);

        assertEquals(1, report.quarantinedCount(), "unresolved data cannot count as success");
        assertEquals(0, report.skippedCount());
        assertEquals(RunReport.Outcome.QUARANTINED, report.shards().get(0).outcome());
        assertTrue(opener.openedShardIds.isEmpty(), "skipped before materializing - no wasted download");
    }

    /**
     * One shard's metadata failing to resolve must not discard the whole index's
     * report, including shards already successfully processed in the same run.
     */
    @Test
    void aShardWhoseMetadataCannotBeResolvedIsQuarantinedNotFatal(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot template = inspector.shardSnapshot(INDEX_NAME, 0);

        var catalog = new DumpToDeltaPipeline.ShardCatalog() {
            @Override public int shardCount() {
                return 3;
            }

            @Override public ShardSnapshot shardSnapshot(int shardId) throws IOException {
                if (shardId == 1) throw new IOException("corrupt snap-*.dat");
                return new ShardSnapshot(template.repoUuid(), template.shardPrefix(),
                    "virtual-shard-" + shardId, template.indexVersion(), template.totalSize(), template.files());
            }
        };
        var opener = new FakeShardReaderOpener(ROWS_PER_SHARD, null);
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        RunReport report = pipeline.run(INDEX_NAME, null, catalog);

        assertEquals(2, report.committedCount(), "the other shards still commit");
        assertEquals(1, report.quarantinedCount());
        assertEquals(3, report.shards().size(), "the report still accounts for every shard");
        assertEquals(List.of(0, 2), opener.openedShardIds);
    }

    /**
     * The report must not show "IllegalStateException: reading _source of doc 0" for a
     * quarantined shard - the generic wrapper ShardSourceIterator uses to satisfy Iterator's
     * checked-exception-free contract, not the real underlying failure. It must show the actual
     * cause's class and where it was thrown, not that wrapper.
     */
    @Test
    void aQuarantinedShardReportsTheRealCauseNotTheIteratorWrapper(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot template = inspector.shardSnapshot(INDEX_NAME, 0);

        var catalog = virtualCatalog(template, 1);
        DumpToDeltaPipeline.ShardReaderOpener opener =
            (shard, shardDir) -> new FailingReadSourceShardReader();
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        RunReport report = pipeline.run(INDEX_NAME, null, catalog);

        assertEquals(1, report.quarantinedCount());
        RunReport.ShardReport badShard = report.shards().get(0);
        assertEquals("IllegalArgumentException", badShard.errorClass(),
            "must report the real cause's class, not IllegalStateException (the Iterator wrapper)");
        assertTrue(badShard.errorMessage().contains("FailingReadSourceShardReader"));
        assertFalse(badShard.errorMessage().contains("corrupt segment: bad codec header"),
            "raw exception messages must not leak input values into reports");
    }

    /**
     * An exception with no message must still report its throwing site in a quarantined shard's
     * report, so the report alone locates the failure; "NullPointerException: null" would not.
     */
    @Test
    void aQuarantinedShardWithAMessagelessErrorStillReportsWhereItCameFrom(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot template = inspector.shardSnapshot(INDEX_NAME, 0);

        var catalog = virtualCatalog(template, 1);
        DumpToDeltaPipeline.ShardReaderOpener opener =
            (shard, shardDir) -> { java.util.Objects.requireNonNull(null); return null; };
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        RunReport report = pipeline.run(INDEX_NAME, null, catalog);

        assertEquals(1, report.quarantinedCount());
        RunReport.ShardReport badShard = report.shards().get(0);
        assertEquals("NullPointerException", badShard.errorClass());
        assertNotNull(badShard.errorMessage(), "a message-less exception must still describe its origin");
        assertTrue(badShard.errorMessage().contains("DumpToDeltaPipelineTest"),
            "origin must name the throwing site: " + badShard.errorMessage());
    }

    @Test
    void readerCloseFailureDoesNotQuarantineACommittedShard(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        var catalog = virtualCatalog(inspector.shardSnapshot(INDEX_NAME, 0), 1);
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, directory) -> new ShardReader() {
            public String runtimeName() { return "test"; }
            public int docCount() { return 2; }
            public boolean isLive(int id) { return id == 1; }
            public void readSource(int id, SourceConsumer consumer) {
                assertEquals(1, id, "the deleted slot must never reach the writer");
                byte[] source = "{\"id\":1}".getBytes(StandardCharsets.UTF_8);
                consumer.accept(source, source.length);
            }
            public void close() throws IOException { throw new IOException("close failure"); }
        };
        var pipeline = new DumpToDeltaPipeline(inspector, store, tmp.resolve("table").toString(), new Configuration(), opener);
        var first = pipeline.run(INDEX_NAME, null, catalog);
        assertEquals(1, first.committedCount());
        assertEquals(0, first.quarantinedCount());
        assertEquals(1, first.rowsWrittenThisRun());
        assertEquals(1, pipeline.run(INDEX_NAME, null, catalog).skippedCount());
    }

    // --- concurrent shards (archive exports with a ShardWorkPool) ---

    private static final int WORKERS = 4;
    private static final ArchiveContext ARCHIVE =
        new ArchiveContext(INDEX_NAME, "repo-index-id", "snap-uuid", "snap", INDEX_NAME, "snap-uuid");

    @Test
    @Tag("concurrency")
    @Timeout(120)
    void parallelRunCommitsEveryShardExactlyOnce(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var opener = new ConcurrentOpener(new CyclicBarrier(WORKERS), Set.of(), null);

        RunReport report = parallelPipeline(tableUri, opener, new ShardWorkPool(WORKERS, 0))
            .runSnapshot(ARCHIVE, null, virtualCatalog(template(), 12), RunReportListener.NO_OP);

        assertEquals(12, report.committedCount());
        assertEquals(12L * ROWS_PER_SHARD, report.rowsWrittenThisRun());
        assertEquals(Set.copyOf(range(0, 12)), Set.copyOf(opener.opened), "each shard opened once");
        assertEquals(12, opener.opened.size());
        assertExactlyOnce(tableUri, range(0, 12));
        assertTrue(opener.maxConcurrentOpens.get() >= WORKERS, "shards genuinely overlapped");
        assertTempDirsRemoved(opener);
    }

    /**
     * Shard writes into one table overlap, not just download and open: every shard of a group
     * stops in the middle of its document stream until all of them are streaming. Were writes
     * serialized per table, the second shard could never start streaming while the first waits,
     * so the barrier would time out and quarantine the shards.
     */
    @Test
    @Tag("concurrency")
    @Timeout(120)
    void shardsOfOneTableStreamIntoTheWriterConcurrentlyAndEachCommitsOnce(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var allStreaming = new CyclicBarrier(WORKERS);
        DumpToDeltaPipeline.ShardReaderOpener opener =
            (shard, dir) -> new MidStreamBarrierReader(new FakeShardReader(shardIdOf(shard), ROWS_PER_SHARD), allStreaming);

        RunReport report = parallelPipeline(tableUri, opener, new ShardWorkPool(WORKERS, 0))
            .runSnapshot(ARCHIVE, null, virtualCatalog(template(), 2 * WORKERS), RunReportListener.NO_OP);

        assertEquals(Set.of(), shardIds(report, RunReport.Outcome.QUARANTINED), "quarantined shards");
        assertEquals(2 * WORKERS, report.committedCount());
        assertExactlyOnce(tableUri, range(0, 2 * WORKERS));
    }

    @Test
    @Tag("concurrency")
    @Timeout(120)
    void parallelQuarantineIsIsolatedAndRetriedNextRun(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var failing = new ConcurrentOpener(new CyclicBarrier(WORKERS), Set.of(2, 7), null);
        var catalog = virtualCatalog(template(), 12);

        RunReport first = parallelPipeline(tableUri, failing, new ShardWorkPool(WORKERS, 0))
            .runSnapshot(ARCHIVE, null, catalog, RunReportListener.NO_OP);

        assertEquals(10, first.committedCount());
        assertEquals(Set.of(2, 7), shardIds(first, RunReport.Outcome.QUARANTINED));
        assertEquals(FakeShardReaderOpener.SimulatedShardFailure.class.getSimpleName(),
            first.shards().stream().filter(s -> s.shardId() == 2).findFirst().orElseThrow().errorClass());

        var healthy = new ConcurrentOpener(null, Set.of(), null);
        RunReport second = parallelPipeline(tableUri, healthy, new ShardWorkPool(WORKERS, 0))
            .runSnapshot(ARCHIVE, null, catalog, RunReportListener.NO_OP);

        assertEquals(Set.of(2, 7), Set.copyOf(healthy.opened), "only the quarantined shards are reattempted");
        assertEquals(2, second.committedCount());
        assertEquals(10, second.skippedCount());
        assertExactlyOnce(tableUri, range(0, 12));
        assertTempDirsRemoved(failing);
        assertTempDirsRemoved(healthy);
    }

    @Test
    @Tag("concurrency")
    @Timeout(120)
    void parallelCrashThenResumeHasNoDuplicatesOrGaps(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var catalog = virtualCatalog(template(), 12);
        var crashing = new ConcurrentOpener(new CyclicBarrier(WORKERS), Set.of(), 5);
        var crashedPool = new ShardWorkPool(WORKERS, 0);

        assertThrows(SimulatedCrash.class, () -> parallelPipeline(tableUri, crashing, crashedPool)
            .runSnapshot(ARCHIVE, null, catalog, RunReportListener.NO_OP));
        // Shards in flight at the crash are not interrupted; wait for them like a restart would
        // only happen after the old process is gone.
        assertTrue(crashedPool.awaitIdle(60, TimeUnit.SECONDS));
        assertFalse(crashing.opened.stream().anyMatch(id -> id > 7), "admission stopped at the crash");
        Set<Integer> committedBefore = committedShards(tableUri);
        assertFalse(committedBefore.contains(5));

        var resumed = new ConcurrentOpener(null, Set.of(), null);
        RunReport report = parallelPipeline(tableUri, resumed, new ShardWorkPool(WORKERS, 0))
            .runSnapshot(ARCHIVE, null, catalog, RunReportListener.NO_OP);

        var expectedResume = new java.util.HashSet<>(range(0, 12));
        expectedResume.removeAll(committedBefore);
        assertEquals(expectedResume, Set.copyOf(resumed.opened));
        assertEquals(0, report.quarantinedCount());
        assertExactlyOnce(tableUri, range(0, 12));
    }

    @Test
    @Tag("concurrency")
    @Timeout(120)
    void listenerIsCalledFromOneThreadWithMonotonicProgress(@TempDir Path tmp) throws Exception {
        var opener = new ConcurrentOpener(new CyclicBarrier(WORKERS), Set.of(3), null);
        var threads = java.util.Collections.synchronizedSet(new java.util.HashSet<Thread>());
        var sizes = java.util.Collections.synchronizedList(new ArrayList<Integer>());
        var last = new java.util.concurrent.atomic.AtomicReference<RunReport>();

        RunReport report = parallelPipeline(tmp.resolve("t").toString(), opener, new ShardWorkPool(WORKERS, 0))
            .runSnapshot(ARCHIVE, null, virtualCatalog(template(), 12), reportSoFar -> {
                threads.add(Thread.currentThread());
                sizes.add(reportSoFar.shards().size());
                last.set(reportSoFar);
            });

        assertEquals(Set.of(Thread.currentThread()), threads, "only the dispatcher calls the listener");
        assertEquals(range(1, 13), sizes, "one call per shard outcome, each with one more shard");
        assertEquals(12, report.shards().size());
        assertEquals(report.shards(), last.get().shards(), "the last persisted report is the final, sorted one");
    }

    @Test
    @Tag("concurrency")
    @Timeout(120)
    void finalReportIsSortedByShardId(@TempDir Path tmp) throws Exception {
        var opener = new ConcurrentOpener(new CyclicBarrier(WORKERS), Set.of(1), null);
        // 13 shards, one without metadata: the other 12 fill the barrier in groups of 4.
        var catalog = new DumpToDeltaPipeline.ShardCatalog() {
            final ShardSnapshot template = templateUnchecked();
            public int shardCount() { return 13; }
            public ShardSnapshot shardSnapshot(int shardId) throws IOException {
                if (shardId == 9) throw new IOException("corrupt snap-*.dat");
                return virtualShard(template, shardId);
            }
        };

        RunReport report = parallelPipeline(tmp.resolve("t").toString(), opener, new ShardWorkPool(WORKERS, 0))
            .runSnapshot(ARCHIVE, null, catalog, RunReportListener.NO_OP);

        assertEquals(range(0, 13), report.shards().stream().map(RunReport.ShardReport::shardId).toList());
        assertEquals(Set.of(1, 9), shardIds(report, RunReport.Outcome.QUARANTINED));
        assertEquals(11, report.committedCount());
    }

    @Test
    void aConflictingShardFailsTheIndexAlsoWhenShardsRunInline(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var opened = new ArrayList<Integer>();
        var alreadyOpen = new CountDownLatch(0);
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> {
            int shardId = shardIdOf(shard);
            opened.add(shardId);
            return shardId == 1 ? new ConflictingShardReader(alreadyOpen, new CountDownLatch(1))
                : new FakeShardReader(shardId, ROWS_PER_SHARD);
        };

        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class,
            () -> parallelPipeline(tableUri, opener, ShardWorkPool.create(1, 0))
                .runSnapshot(ARCHIVE, null, virtualCatalog(template(), 3), RunReportListener.NO_OP));

        assertEquals(List.of(0, 1), opened, "the index stops at the conflicting shard");
        assertEquals(Set.of(0), committedShards(tableUri));
    }

    /**
     * A shard failing because the table belongs to another snapshot means every other shard
     * would fail the same way: the index fails instead of quarantining, after shards already in
     * flight finish and are recorded.
     */
    @Test
    @Tag("concurrency")
    @Timeout(120)
    void aConflictingShardFailsTheIndexAfterInFlightShardsComplete(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var othersOpened = new CountDownLatch(3);
        var conflictThrown = new CountDownLatch(1);
        var opened = java.util.Collections.synchronizedList(new ArrayList<Integer>());
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> {
            int shardId = shardIdOf(shard);
            opened.add(shardId);
            if (shardId == 1) return new ConflictingShardReader(othersOpened, conflictThrown);
            if (shardId < 4) {
                othersOpened.countDown();
                assertTrue(conflictThrown.await(60, TimeUnit.SECONDS));
            }
            return new FakeShardReader(shardId, ROWS_PER_SHARD);
        };
        var last = new java.util.concurrent.atomic.AtomicReference<RunReport>();

        var failure = assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class,
            () -> parallelPipeline(tableUri, opener, new ShardWorkPool(WORKERS, 0))
                .runSnapshot(ARCHIVE, null, virtualCatalog(template(), 8), last::set));

        assertNotNull(failure);
        var inFlight = new java.util.HashSet<>(opened);
        inFlight.remove(1);
        assertTrue(inFlight.containsAll(Set.of(0, 2, 3)));
        assertEquals(inFlight, committedShards(tableUri), "shards in flight finished before the index failed");
        assertEquals(inFlight, shardIds(last.get(), RunReport.Outcome.COMMITTED), "and were reported");
        assertFalse(last.get().shards().stream().anyMatch(s -> s.shardId() == 1), "not quarantined");
        assertTrue(opened.size() < 8, "admission stopped");
    }

    /**
     * The dispatcher itself failing, here interrupted while waiting to admit the next shard, ends
     * the index like a conflict does: the shards in flight finish and are recorded first, so no
     * shard task still writes to this table once the failure reaches the caller.
     */
    @Test
    @Tag("concurrency")
    @Timeout(120)
    void aDispatcherFailureDrainsInFlightShardsBeforeItPropagates(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var bothOpen = new CountDownLatch(2);
        var release = new CountDownLatch(1);
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> {
            bothOpen.countDown();
            assertTrue(release.await(60, TimeUnit.SECONDS));
            return new FakeShardReader(shardIdOf(shard), ROWS_PER_SHARD);
        };
        var last = new java.util.concurrent.atomic.AtomicReference<RunReport>();
        var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var catalog = virtualCatalog(template(), 4);
        var pool = new ShardWorkPool(2, 0);
        var dispatcher = new Thread(() -> {
            try {
                parallelPipeline(tableUri, opener, pool).runSnapshot(ARCHIVE, null, catalog, last::set);
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        dispatcher.start();
        assertTrue(bothOpen.await(60, TimeUnit.SECONDS));
        // Both workers are busy, so the dispatcher is waiting to admit shard 2 or about to.
        dispatcher.interrupt();
        release.countDown();
        dispatcher.join(60_000);

        assertFalse(dispatcher.isAlive());
        assertInstanceOf(InterruptedException.class, failure.get());
        assertEquals(Set.of(0, 1), committedShards(tableUri));
        assertNotNull(last.get(), "in-flight shards were recorded before the failure propagated");
        assertEquals(Set.of(0, 1), shardIds(last.get(), RunReport.Outcome.COMMITTED));
        assertTrue(pool.awaitIdle(60, TimeUnit.SECONDS));
        assertEquals(2, pool.availablePermits(), "no permit leaked");
    }

    @Test
    void theLegacyInferencePathStaysSerialEvenWithAPool(@TempDir Path tmp) throws Exception {
        var opener = new FakeShardReaderOpener(ROWS_PER_SHARD, null);
        var threads = java.util.Collections.synchronizedSet(new java.util.HashSet<Thread>());
        DumpToDeltaPipeline.ShardReaderOpener recording = (shard, dir) -> {
            threads.add(Thread.currentThread());
            return opener.open(shard, dir);
        };
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var pipeline = new DumpToDeltaPipeline(new RepositoryInspector(store), store,
            tmp.resolve("t").toString(), new Configuration(), recording).withWorkPool(new ShardWorkPool(WORKERS, 0));

        pipeline.run(INDEX_NAME, null, virtualCatalog(template(), 5));

        assertEquals(List.of(0, 1, 2, 3, 4), opener.openedShardIds);
        assertEquals(Set.of(Thread.currentThread()), threads);
    }

    @Test
    void serialArchiveRunUsesTheCallingThreadInShardOrder(@TempDir Path tmp) throws Exception {
        var opener = new ConcurrentOpener(null, Set.of(), null);
        RunReport report = parallelPipeline(tmp.resolve("t").toString(), opener, ShardWorkPool.create(1, 0))
            .runSnapshot(ARCHIVE, null, virtualCatalog(template(), 5), RunReportListener.NO_OP);

        assertEquals(range(0, 5), opener.opened);
        assertEquals(Set.of(Thread.currentThread()), opener.threads);
        assertEquals(5, report.committedCount());
    }

    /** Malformed UTF-8 still exports with replacement characters, and stderr gets a content-free count. */
    @Test
    void documentsOffTheFastPathExportAsBeforeAndAreCounted(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        byte[] malformed = {'{', '"', 's', 'e', 'c', 'r', 'e', 't', '"', ':', '"', (byte) 0xC3, '(', '"', '}'};
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> new ShardReader() {
            @Override public String runtimeName() { return "fake"; }
            @Override public int docCount() { return 3; }
            @Override public void close() {}

            @Override public void readSource(int docId, SourceConsumer consumer) {
                byte[] bytes = docId == 1 ? malformed.clone() : ("{\"row\":" + docId + "}").getBytes(StandardCharsets.UTF_8);
                consumer.accept(bytes, bytes.length);
            }
        };
        var err = new java.io.ByteArrayOutputStream();
        var stderr = System.err;
        RunReport report;
        System.setErr(new java.io.PrintStream(err, true, StandardCharsets.UTF_8));
        try {
            report = parallelPipeline(tableUri, opener, ShardWorkPool.create(1, 0))
                .runSnapshot(ARCHIVE, null, virtualCatalog(template(), 1), RunReportListener.NO_OP);
        } finally {
            System.setErr(stderr);
        }
        assertEquals(1, report.committedCount());
        String warnings = err.toString(StandardCharsets.UTF_8);
        assertTrue(warnings.contains("warning: shard 0 of " + INDEX_NAME + ": 1 documents are not strict UTF-8 JSON"), warnings);
        assertFalse(warnings.contains("secret"), "no document content on stderr");
        var sources = new java.util.ArrayList<String>();
        try (var rows = io.delta.standalone.DeltaLog.forTable(new Configuration(), tableUri).snapshot().open()) {
            while (rows.hasNext()) sources.add(rows.next().getString("_source"));
        }
        assertEquals(List.of("{\"row\":0}", "{\"secret\":\"�(\"}", "{\"row\":2}"), sources);
    }

    /**
     * A shard whose key another writer commits while this run stages it is reported as skipped
     * with its bytes, not as a committed shard of 0 rows; an empty shard is still committed.
     */
    @Test
    void aShardCommittedByAnotherWriterMeanwhileIsReportedAsSkipped(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> {
            int shardId = shardIdOf(shard);
            if (shardId == 2) return new FakeShardReader(shardId, 0);
            if (shardId != 1) return new FakeShardReader(shardId, ROWS_PER_SHARD);
            var fake = new FakeShardReader(shardId, ROWS_PER_SHARD);
            return new ShardReader() {
                @Override public String runtimeName() { return fake.runtimeName(); }
                @Override public int docCount() { return fake.docCount(); }
                @Override public void close() {}

                @Override public void readSource(int docId, SourceConsumer consumer) {
                    if (docId == 0) {
                        try {
                            String row = ARCHIVE.document("{\"other\":true}", 1,
                                new de.sloow.gefrierschrank.core.lucene.DocumentMetadata("x", null, "stored", 0));
                            new DeltaTableWriter(tableUri).write(new Configuration(), List.of(row).iterator(),
                                ArchiveContext.mapping(), ARCHIVE.key(1));
                        } catch (IOException e) {
                            throw new java.io.UncheckedIOException(e);
                        }
                    }
                    fake.readSource(docId, consumer);
                }
            };
        };
        var out = new java.io.ByteArrayOutputStream();
        var stdout = System.out;
        RunReport report;
        System.setOut(new java.io.PrintStream(out, true, StandardCharsets.UTF_8));
        try {
            report = parallelPipeline(tableUri, opener, ShardWorkPool.create(1, 0))
                .runSnapshot(ARCHIVE, null, virtualCatalog(template(), 3), RunReportListener.NO_OP);
        } finally {
            System.setOut(stdout);
        }

        var skipped = report.shards().get(1);
        assertEquals(RunReport.Outcome.SKIPPED_ALREADY_COMMITTED, skipped.outcome());
        assertEquals(0, skipped.rowsWritten());
        assertEquals(template().totalSize(), skipped.bytesProcessed());
        assertNull(skipped.errorClass());
        assertEquals(RunReport.Outcome.COMMITTED, report.shards().get(2).outcome(), "an empty shard commits");
        assertEquals(0, report.shards().get(2).rowsWritten());
        assertEquals(2, report.committedCount());
        assertEquals(1, report.skippedCount());
        assertEquals(0, report.quarantinedCount());
        assertEquals(3L * template().totalSize(), report.bytesProcessedThisRun());
        assertTrue(out.toString(StandardCharsets.UTF_8).contains("Skipping shard 1 of " + INDEX_NAME
            + ": committed by another writer (" + ARCHIVE.key(1) + ")"), out.toString(StandardCharsets.UTF_8));
        assertEquals(Set.of(0, 1, 2), committedShards(tableUri));
        assertEquals(ParquetFooters.addFilePaths(new Configuration(), tableUri),
            ParquetFooters.parquetFilesOnDisk(Path.of(tableUri)), "the losing shard's staged files are gone");
    }

    private static DumpToDeltaPipeline parallelPipeline(String tableUri, DumpToDeltaPipeline.ShardReaderOpener opener,
                                                        ShardWorkPool pool) {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        return new DumpToDeltaPipeline(new RepositoryInspector(store), store, new DeltaTableWriter(tableUri),
            new Configuration(), opener).withWorkPool(pool);
    }

    private static ShardSnapshot template() throws IOException {
        return new RepositoryInspector(new FileSystemBlobStore(FIXTURE_REPO)).shardSnapshot(INDEX_NAME, 0);
    }

    private static ShardSnapshot templateUnchecked() {
        try {
            return template();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static ShardSnapshot virtualShard(ShardSnapshot template, int shardId) {
        return new ShardSnapshot(template.repoUuid(), template.shardPrefix(),
            "virtual-shard-" + shardId, template.indexVersion(), template.totalSize(), template.files());
    }

    private static int shardIdOf(ShardSnapshot shard) {
        return Integer.parseInt(shard.snapshotName().substring("virtual-shard-".length()));
    }

    private static List<Integer> range(int from, int to) {
        return java.util.stream.IntStream.range(from, to).boxed().toList();
    }

    private static Set<Integer> shardIds(RunReport report, RunReport.Outcome outcome) {
        return report.shards().stream().filter(s -> s.outcome() == outcome)
            .map(RunReport.ShardReport::shardId).collect(java.util.stream.Collectors.toSet());
    }

    /** Every expected shard is committed once with exactly its rows, and no stray Parquet file exists. */
    private static void assertExactlyOnce(String tableUri, List<Integer> expectedShards) throws IOException {
        var conf = new Configuration();
        var snapshot = io.delta.standalone.DeltaLog.forTable(conf, tableUri).snapshot();
        Map<Long, Long> rowsPerShard = new HashMap<>();
        var rowKeys = new java.util.HashSet<String>();
        try (var it = snapshot.open()) {
            while (it.hasNext()) {
                var row = it.next();
                rowsPerShard.merge(row.getLong("_shard"), 1L, Long::sum);
                assertTrue(rowKeys.add(row.getLong("_shard") + "/" + row.getLong("_doc")), "duplicate row");
            }
        }
        assertEquals(expectedShards.size(), rowsPerShard.size());
        for (int shardId : expectedShards)
            assertEquals((long) ROWS_PER_SHARD, rowsPerShard.get((long) shardId), "rows of shard " + shardId);
        assertEquals(expectedShards.size(), committedShards(tableUri).size());
        long parquetFiles;
        try (var files = java.nio.file.Files.list(Path.of(tableUri))) {
            parquetFiles = files.filter(f -> f.getFileName().toString().endsWith(".parquet")).count();
        }
        assertEquals(snapshot.getAllFiles().size(), parquetFiles, "Parquet files on disk equal committed AddFiles");
    }

    private static Set<Integer> committedShards(String tableUri) {
        var log = io.delta.standalone.DeltaLog.forTable(new Configuration(), tableUri);
        if (!log.tableExists()) return Set.of();
        var shards = new java.util.HashSet<Integer>();
        for (var file : log.snapshot().getAllFiles())
            shards.add(ArchiveContext.parseKey(file.getTags().get(ShardKey.TAG)).orElseThrow().shard());
        return shards;
    }

    private static void assertTempDirsRemoved(ConcurrentOpener opener) {
        assertFalse(opener.dirs.isEmpty());
        for (Path dir : opener.dirs) assertFalse(java.nio.file.Files.exists(dir), dir + " must be deleted");
    }

    /**
     * Records opens under a lock, then waits on {@code barrier} outside it, so a whole group of
     * shards is provably open at the same time. Failing and crashing shards pass the barrier too.
     */
    static final class ConcurrentOpener implements DumpToDeltaPipeline.ShardReaderOpener {
        final List<Integer> opened = new ArrayList<>();
        final List<Path> dirs = new ArrayList<>();
        final Set<Thread> threads = new java.util.HashSet<>();
        final java.util.concurrent.atomic.AtomicInteger openNow = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger maxConcurrentOpens = new java.util.concurrent.atomic.AtomicInteger();
        private final CyclicBarrier barrier;
        private final Set<Integer> failing;
        private final Integer crashAt;

        ConcurrentOpener(CyclicBarrier barrier, Set<Integer> failing, Integer crashAt) {
            this.barrier = barrier;
            this.failing = failing;
            this.crashAt = crashAt;
        }

        @Override
        public ShardReader open(ShardSnapshot shard, Path shardDir) throws Exception {
            int shardId = shardIdOf(shard);
            synchronized (this) {
                opened.add(shardId);
                dirs.add(shardDir);
                threads.add(Thread.currentThread());
            }
            maxConcurrentOpens.accumulateAndGet(openNow.incrementAndGet(), Math::max);
            try {
                if (barrier != null) barrier.await(60, TimeUnit.SECONDS);
            } finally {
                openNow.decrementAndGet();
            }
            if (crashAt != null && crashAt == shardId) throw new SimulatedCrash();
            if (failing.contains(shardId))
                throw new FakeShardReaderOpener.SimulatedShardFailure("shard " + shardId + " has a corrupt segment");
            return new FakeShardReader(shardId, ROWS_PER_SHARD);
        }
    }

    /** Fails its first document with a conflict once the other shards of its group are open. */
    static final class ConflictingShardReader implements ShardReader {
        private final CountDownLatch othersOpened;
        private final CountDownLatch conflictThrown;

        ConflictingShardReader(CountDownLatch othersOpened, CountDownLatch conflictThrown) {
            this.othersOpened = othersOpened;
            this.conflictThrown = conflictThrown;
        }

        @Override public String runtimeName() { return "fake"; }
        @Override public int docCount() { return 1; }
        @Override public void close() {}

        @Override
        public void readSource(int docId, SourceConsumer consumer) throws IOException {
            try {
                if (!othersOpened.await(60, TimeUnit.SECONDS)) throw new IOException("group never formed");
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            conflictThrown.countDown();
            throw new IOException("commit refused", new DeltaTableWriter.ArchiveTargetConflictException());
        }
    }

    private DumpToDeltaPipeline.ShardCatalog virtualCatalog(ShardSnapshot template, int shardCount) {
        return new DumpToDeltaPipeline.ShardCatalog() {
            @Override
            public int shardCount() {
                return shardCount;
            }

            @Override
            public ShardSnapshot shardSnapshot(int shardId) {
                // Same real files as the fixture shard (so materialization exercises real I/O);
                // only snapshotName varies, which is how the fake opener tells shards apart.
                return new ShardSnapshot(template.repoUuid(), template.shardPrefix(),
                    "virtual-shard-" + shardId, template.indexVersion(), template.totalSize(), template.files());
            }
        };
    }

    private Map<Long, Long> readRowsGroupedByShard(String tableUri) throws IOException {
        var conf = new Configuration();
        var log = io.delta.standalone.DeltaLog.forTable(conf, tableUri);
        Map<Long, Long> counts = new HashMap<>();
        try (var it = log.snapshot().open()) {
            while (it.hasNext()) {
                var row = it.next();
                counts.merge(row.getLong("shardId"), 1L, Long::sum);
            }
        }
        return counts;
    }

    /**
     * Simulates a whole-process crash (e.g. {@code kill -9}), not an isolated shard error -
     * extends {@link Error}, not {@link RuntimeException}, specifically so it is NOT caught by
     * the pipeline's per-shard {@code catch (Exception e)} quarantine handling and instead
     * propagates all the way out of {@code run()}, same as a real VM-level failure would.
     */
    static final class SimulatedCrash extends Error {}

    /**
     * Records which virtual shard (identified by ShardSnapshot.snapshotName(), see
     * {@link #virtualCatalog}) was actually opened, and can simulate either a mid-dump crash
     * (process death - propagates out of run(), see {@link SimulatedCrash}) or an isolated
     * per-shard failure (should be quarantined - run() continues to the next shard) by throwing
     * instead of returning a reader once a chosen shard is reached.
     */
    static final class FakeShardReaderOpener implements DumpToDeltaPipeline.ShardReaderOpener {
        final List<Integer> openedShardIds = new ArrayList<>();
        private final int rowsPerShard;
        private final Integer crashAtShardId;
        private final Set<Integer> quarantineAtShardIds;

        FakeShardReaderOpener(int rowsPerShard, Integer crashAtShardId) {
            this(rowsPerShard, crashAtShardId, Set.of());
        }

        FakeShardReaderOpener(int rowsPerShard, Integer crashAtShardId, Set<Integer> quarantineAtShardIds) {
            this.rowsPerShard = rowsPerShard;
            this.crashAtShardId = crashAtShardId;
            this.quarantineAtShardIds = quarantineAtShardIds;
        }

        @Override
        public ShardReader open(ShardSnapshot shard, Path shardDir) {
            int shardId = Integer.parseInt(shard.snapshotName().substring("virtual-shard-".length()));
            if (crashAtShardId != null && shardId == crashAtShardId) {
                throw new SimulatedCrash();
            }
            if (quarantineAtShardIds.contains(shardId)) {
                throw new SimulatedShardFailure("shard " + shardId + " has a corrupt segment");
            }
            openedShardIds.add(shardId);
            return new FakeShardReader(shardId, rowsPerShard);
        }

        static final class SimulatedShardFailure extends RuntimeException {
            SimulatedShardFailure(String message) {
                super(message);
            }
        }
    }

    /** Waits on {@code barrier} before handing out its second document, i.e. with its shard's write under way. */
    static final class MidStreamBarrierReader implements ShardReader {
        private final ShardReader delegate;
        private final CyclicBarrier barrier;

        MidStreamBarrierReader(ShardReader delegate, CyclicBarrier barrier) {
            this.delegate = delegate;
            this.barrier = barrier;
        }

        @Override public String runtimeName() { return delegate.runtimeName(); }
        @Override public int docCount() throws IOException { return delegate.docCount(); }
        @Override public void close() throws IOException { delegate.close(); }

        @Override
        public void readSource(int docId, SourceConsumer consumer) throws IOException {
            if (docId == 1) {
                try {
                    barrier.await(30, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IOException("the other shards of the group never streamed at the same time", e);
                }
            }
            delegate.readSource(docId, consumer);
        }
    }

    /** Emits deterministic, shard-identifiable documents instead of reading real Lucene data. */
    static final class FakeShardReader implements ShardReader {
        private final int shardId;
        private final int rowCount;

        FakeShardReader(int shardId, int rowCount) {
            this.shardId = shardId;
            this.rowCount = rowCount;
        }

        @Override
        public String runtimeName() {
            return "fake";
        }

        @Override
        public int docCount() {
            return rowCount;
        }

        @Override
        public void readSource(int docId, SourceConsumer consumer) {
            byte[] bytes = ("{\"shardId\": " + shardId + ", \"row\": " + docId + "}")
                .getBytes(StandardCharsets.UTF_8);
            consumer.accept(bytes, bytes.length);
        }

        @Override
        public void close() {
        }
    }

    /**
     * Simulates a shard whose read fails: docCount() succeeds, but readSource() throws an
     * IOException whose cause is the real underlying failure (a codec error, in this case) -
     * exactly what ShardReader implementations do, and exactly what ShardSourceIterator wraps
     * in an IllegalStateException to satisfy Iterator's checked-exception-free contract.
     */
    static final class FailingReadSourceShardReader implements ShardReader {
        @Override
        public String runtimeName() {
            return "fake";
        }

        @Override
        public int docCount() {
            return 1;
        }

        @Override
        public void readSource(int docId, SourceConsumer consumer) throws IOException {
            throw new IOException("reading segment",
                new IllegalArgumentException("corrupt segment: bad codec header"));
        }

        @Override
        public void close() {
        }
    }
}
