package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Failure semantics of a shard read in ranges: a failing range fails the whole shard
 * with no commit and no file of any range left, an Error stays an Error, a committed key starts no
 * range, the snapshot guard still refuses a racing snapshot, the writer never returns while a
 * range still reads, and every range's fork is closed, once, on the range's thread as soon as the
 * range is staged, before the parent.
 */
@Timeout(120)
class ShardRangeFailureTest {

    private static final Path FIXTURE_REPO = Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");
    private static final String INDEX_NAME = "test_data_7_17_9";
    private static final ArchiveContext ARCHIVE =
        new ArchiveContext(INDEX_NAME, "repo-index-id", "snap-uuid", "snap", INDEX_NAME, "snap-uuid");

    private String previousMinimum;

    @BeforeEach
    void smallRanges() {
        previousMinimum = System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "1");
    }

    @AfterEach
    void restore() {
        if (previousMinimum == null) System.clearProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY);
        else System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, previousMinimum);
    }

    static final class InjectedRangeFailure extends IOException {
        InjectedRangeFailure() {
            super("injected");
        }
    }

    static final class SimulatedCrash extends Error {}

    /** Documents 0..docs-1, all live; forks share the event log and the per-document hook. */
    static class ForkingReader implements ShardReader {
        final int docs;
        final List<String> events;
        final String name;
        final IntConsumer beforeRead;
        final java.util.concurrent.atomic.AtomicInteger forks;
        final Map<String, List<String>> threads;
        final Map<String, String> closedOn;

        ForkingReader(int docs, List<String> events, IntConsumer beforeRead) {
            this(docs, events, "parent", beforeRead, new java.util.concurrent.atomic.AtomicInteger(), new ConcurrentHashMap<>(),
                new ConcurrentHashMap<>());
        }

        private ForkingReader(int docs, List<String> events, String name, IntConsumer beforeRead,
                              java.util.concurrent.atomic.AtomicInteger forks, Map<String, List<String>> threads,
                              Map<String, String> closedOn) {
            this.docs = docs;
            this.events = events;
            this.name = name;
            this.beforeRead = beforeRead;
            this.forks = forks;
            this.threads = threads;
            this.closedOn = closedOn;
        }

        @Override public String runtimeName() { return "forking"; }
        @Override public int docCount() { return docs; }

        @Override public DocumentMetadata documentMetadata(int docId) {
            return new DocumentMetadata("id-" + docId, null, "stored", docId);
        }

        @Override public void readSource(int docId, SourceConsumer consumer) throws IOException {
            var thread = Thread.currentThread();
            threads.computeIfAbsent(name, n -> Collections.synchronizedList(new ArrayList<>()))
                .add(thread.getName() + "|" + thread.isDaemon() + "|" + thread.getContextClassLoader());
            beforeRead.accept(docId);
            byte[] bytes = ("{\"doc\":" + docId + ",\"pad\":\"" + "p".repeat(100) + "\"}").getBytes(StandardCharsets.UTF_8);
            consumer.accept(bytes, bytes.length);
        }

        @Override public ShardReader fork() {
            events.add("fork " + forks.incrementAndGet());
            return new ForkingReader(docs, events, "fork-" + forks.get(), beforeRead, forks, threads, closedOn);
        }

        @Override public void close() {
            events.add("close " + name);
            closedOn.put(name, Thread.currentThread().getName());
        }
    }

    private static Configuration conf(int threads) {
        var conf = new Configuration();
        conf.set(ShardRanges.SHARD_THREADS_KEY, Integer.toString(threads));
        conf.set(DeltaTableWriter.TARGET_FILE_BYTES_KEY, "1");
        return conf;
    }

    private static DumpToDeltaPipeline pipeline(String table, Configuration conf, DumpToDeltaPipeline.ShardReaderOpener opener) {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        return new DumpToDeltaPipeline(new RepositoryInspector(store), store, new DeltaTableWriter(table), conf, opener);
    }

    private static DumpToDeltaPipeline.ShardCatalog catalog(int shards) throws IOException {
        ShardSnapshot template = new RepositoryInspector(new FileSystemBlobStore(FIXTURE_REPO)).shardSnapshot(INDEX_NAME, 0);
        return new DumpToDeltaPipeline.ShardCatalog() {
            @Override public int shardCount() { return shards; }
            @Override public ShardSnapshot shardSnapshot(int shardId) {
                return new ShardSnapshot(template.repoUuid(), template.shardPrefix(), "virtual-shard-" + shardId,
                    template.indexVersion(), template.totalSize(), template.files());
            }
        };
    }

    private static void assertNoParquetFiles(Path table) throws IOException {
        assertEquals(Set.of(), ParquetFooters.parquetFilesOnDisk(table), "no file of any range may remain");
    }

    private static void assertForksClosedBeforeParent(List<String> events, int forks) {
        var closes = events.stream().filter(e -> e.startsWith("close")).toList();
        assertEquals(forks + 1, closes.size(), "every reader closed once: " + events);
        assertEquals("close parent", closes.get(closes.size() - 1), "the parent last: " + events);
    }

    /** Range 0 finishes; then range 3 fails: the finished range's files go too. */
    @Test
    void anExceptionInOneRangeQuarantinesTheShardWithoutAnyFile(@TempDir Path tmp) throws Exception {
        var events = Collections.synchronizedList(new ArrayList<String>());
        var firstRangeRead = new CountDownLatch(10);
        IntConsumer hook = doc -> {
            if (doc < 10) firstRangeRead.countDown();
            if (doc == 30) {
                try {
                    assertTrue(firstRangeRead.await(60, TimeUnit.SECONDS));
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
                throw new java.io.UncheckedIOException(new InjectedRangeFailure());
            }
        };
        Path table = tmp.resolve("table");
        var report = pipeline(table.toString(), conf(4), (s, d) -> new ForkingReader(40, events, hook))
            .runSnapshot(ARCHIVE, null, catalog(2), RunReportListener.NO_OP);

        assertEquals(RunReport.Outcome.QUARANTINED, report.shards().get(0).outcome());
        assertEquals("InjectedRangeFailure", report.shards().get(0).errorClass());
        assertFalse(report.shards().get(0).errorMessage().contains("injected"), "no raw message in the report");
        assertEquals(RunReport.Outcome.QUARANTINED, report.shards().get(1).outcome(), "each shard fails on its own");
        assertFalse(Files.exists(table.resolve("_delta_log")), "nothing committed");
        assertNoParquetFiles(table);
        var closes = events.stream().filter(e -> e.startsWith("close")).toList();
        assertEquals(10, closes.size(), "every reader closed once: " + closes);
        var forks = Set.of("close fork-1", "close fork-2", "close fork-3", "close fork-4");
        assertEquals(forks, Set.copyOf(closes.subList(0, 4)), "each shard's forks before its parent: " + closes);
        assertEquals("close parent", closes.get(4));
        assertEquals(forks, Set.copyOf(closes.subList(5, 9)), "each shard's forks before its parent: " + closes);
        assertEquals("close parent", closes.get(9));
    }

    @Test
    void anErrorInOneRangeStaysAnErrorAndCommitsNothing(@TempDir Path tmp) {
        var events = Collections.synchronizedList(new ArrayList<String>());
        IntConsumer hook = doc -> {
            if (doc == 17) throw new SimulatedCrash();
        };
        Path table = tmp.resolve("table");
        assertThrows(SimulatedCrash.class, () -> pipeline(table.toString(), conf(3), (s, d) -> new ForkingReader(30, events, hook))
            .runSnapshot(ARCHIVE, null, catalog(3), RunReportListener.NO_OP));
        assertFalse(Files.exists(table.resolve("_delta_log")), "nothing committed");
        assertDoesNotThrow(() -> assertNoParquetFiles(table));
        assertForksClosedBeforeParent(events, 3);
    }

    /**
     * Two ranges fail, first one with the target's FSError, then one with a fatal Error: the fatal
     * Error propagates, and is not translated into a TargetStorageException.
     */
    @Test
    void aFatalErrorWinsOverAnotherRangesStorageFailure(@TempDir Path tmp) {
        var events = Collections.synchronizedList(new ArrayList<String>());
        var bothReading = new CountDownLatch(2);
        IntConsumer hook = doc -> {
            if (doc != 0 && doc != 20) return;
            bothReading.countDown();
            try {
                assertTrue(bothReading.await(60, TimeUnit.SECONDS));
                // Past its cancellation check already, the second range fails after the first.
                if (doc == 20) Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            if (doc == 0) throw FaultInjectingLogStore.fsError("No space left on device");
            throw new SimulatedCrash();
        };
        Path table = tmp.resolve("table");
        var thrown = assertThrows(SimulatedCrash.class, () -> pipeline(table.toString(), conf(2),
            (s, d) -> new ForkingReader(40, events, hook)).runSnapshot(ARCHIVE, null, catalog(1), RunReportListener.NO_OP));
        assertTrue(java.util.Arrays.stream(thrown.getSuppressed()).anyMatch(org.apache.hadoop.fs.FSError.class::isInstance),
            "the storage failure is kept as suppressed");
        assertFalse(Files.exists(table.resolve("_delta_log")), "nothing committed");
        assertDoesNotThrow(() -> assertNoParquetFiles(table));
        assertForksClosedBeforeParent(events, 2);
    }

    /** Ten documents from {@code base}; closing it throws {@code closeFailure}, if any. */
    private static final class ClosingRange implements Iterator<ArchiveRow>, AutoCloseable {
        private final int end;
        private final Error closeFailure;
        private int next;

        ClosingRange(int base, Error closeFailure) {
            this.next = base;
            this.end = base + 10;
            this.closeFailure = closeFailure;
        }

        @Override public boolean hasNext() { return next < end; }

        @Override public ArchiveRow next() {
            byte[] source = ("{\"doc\":" + next + "}").getBytes(StandardCharsets.UTF_8);
            return new ArchiveRow(source, source.length, new DocumentMetadata(null, null, "stored", next++));
        }

        @Override public void close() {
            if (closeFailure != null) throw closeFailure;
        }
    }

    /**
     * A range that staged its rows and then fails to close with an Error, as with an
     * OutOfMemoryError while it gives back its document memory: the Error fails the shard, which
     * commits nothing, instead of ending only the range's thread while the shard commits.
     */
    @Test
    void anErrorClosingARangeStaysAnErrorAndCommitsNothing(@TempDir Path tmp) {
        var crash = new SimulatedCrash();
        List<Iterator<ArchiveRow>> ranges = List.of(new ClosingRange(0, null), new ClosingRange(10, crash));
        Path table = tmp.resolve("table");
        var thrown = assertThrows(SimulatedCrash.class, () -> new DeltaTableWriter(table.toString())
            .writeArchive(new Configuration(), ranges, () -> {}, ARCHIVE, 0, ARCHIVE.key(0)));
        assertSame(crash, thrown);
        assertFalse(Files.exists(table.resolve("_delta_log")), "nothing committed");
        assertDoesNotThrow(() -> assertNoParquetFiles(table));
    }

    @Test
    void aCommittedKeyStartsNoRange(@TempDir Path tmp) throws Exception {
        String table = tmp.resolve("table").toString();
        var writer = new DeltaTableWriter(table);
        var conf = new Configuration();
        var committed = new ArrayList<ArchiveRow>();
        byte[] source = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        committed.add(new ArchiveRow(source, source.length, new DocumentMetadata("1", null, "stored", 0)));
        writer.writeArchive(conf, List.of(committed.iterator()), ARCHIVE, 0, ARCHIVE.key(0));

        Iterator<ArchiveRow> untouchable = new Iterator<>() {
            @Override public boolean hasNext() { throw new AssertionError("a range started"); }
            @Override public ArchiveRow next() { throw new AssertionError("a range started"); }
        };
        var cancelled = new AtomicBoolean();
        assertEquals(new DeltaTableWriter.WriteResult(0, true), writer.writeArchive(conf,
            List.of(untouchable, untouchable, untouchable), () -> cancelled.set(true), ARCHIVE, 0, ARCHIVE.key(0)));
        assertFalse(cancelled.get());
    }

    @Test
    void theSnapshotGuardRefusesARacingSnapshotInRanges(@TempDir Path tmp) throws Exception {
        String index = INDEX_NAME;
        Path tableDir = tmp.resolve("table");
        String table = tableDir.toString();
        var conf = conf(3);
        var otherSnapshot = new ArchiveContext(index, "rid", "another-snapshot", "other", index, "another-snapshot");
        var raced = new boolean[1];
        var forked = new java.util.concurrent.atomic.AtomicInteger();
        // Another snapshot commits while the ranges stage: the refusal comes at the shard's commit.
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> new RacingReader(
            new ShardRangeExportTest.CountingReader(new Lucene10ShardReader(dir), forked, new java.util.concurrent.atomic.AtomicInteger(-1)),
            () -> {
                synchronized (raced) {
                    if (!raced[0]) ArchiveGuardTest.writeShard(new DeltaTableWriter(table), conf, otherSnapshot, 0);
                    raced[0] = true;
                }
            });
        var sources = new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(FIXTURE_REPO)));
        var exporter = new FullIndexExporter(sources, conf, opener);

        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class, () -> exporter.run(index, table, RunReportListener.NO_OP));
        assertTrue(raced[0]);
        assertTrue(forked.get() >= 1, "the shard was read in ranges");
        assertEquals(Set.of(otherSnapshot.key(0)), new DeltaTableWriter(table).committedShardKeys(new Configuration()));
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(tableDir),
            "the refused ranges left no Parquet files behind");
    }

    /** Runs {@code race} when a range reaches document 400, i.e. with every range's staging under way. */
    static final class RacingReader implements ShardReader {
        private final ShardReader delegate;
        private final ThrowingRunnable race;

        interface ThrowingRunnable {
            void run() throws IOException;
        }

        RacingReader(ShardReader delegate, ThrowingRunnable race) {
            this.delegate = delegate;
            this.race = race;
        }

        @Override public String runtimeName() { return delegate.runtimeName(); }
        @Override public int docCount() throws IOException { return delegate.docCount(); }
        @Override public boolean isLive(int docId) throws IOException { return delegate.isLive(docId); }
        @Override public DocumentMetadata documentMetadata(int docId) throws IOException { return delegate.documentMetadata(docId); }

        @Override public void readSource(int docId, SourceConsumer consumer) throws IOException {
            if (docId == 400) race.run();
            delegate.readSource(docId, consumer);
        }

        @Override public ShardReader fork() throws IOException {
            ShardReader fork = delegate.fork();
            return fork == null ? null : new RacingReader(fork, race);
        }

        @Override public void close() throws IOException { delegate.close(); }
    }

    @Test
    void rangesRunOnDaemonThreadsWithTheCallersContextLoaderAndForksCloseFirst(@TempDir Path tmp) throws Exception {
        var events = Collections.synchronizedList(new ArrayList<String>());
        var reader = new ForkingReader(40, events, doc -> {});
        var loader = new java.net.URLClassLoader(new java.net.URL[0], getClass().getClassLoader());
        var caller = Thread.currentThread();
        ClassLoader previous = caller.getContextClassLoader();
        caller.setContextClassLoader(loader);
        RunReport report;
        try {
            report = pipeline(tmp.resolve("table").toString(), conf(4), (s, d) -> reader)
                .runSnapshot(ARCHIVE, null, catalog(1), RunReportListener.NO_OP);
        } finally {
            caller.setContextClassLoader(previous);
        }
        assertEquals(1, report.committedCount());
        assertEquals(40, report.rowsWrittenThisRun());
        assertEquals(Set.of("fork-1", "fork-2", "fork-3", "fork-4"), reader.threads.keySet(), "every range reads through a fork");
        var threadNames = new java.util.HashSet<String>();
        for (var reads : reader.threads.values()) {
            for (String read : reads) {
                String[] parts = read.split("\\|");
                threadNames.add(parts[0]);
                assertTrue(parts[0].startsWith("gefrierschrank-range-0-"), read);
                assertEquals("true", parts[1], "daemon: " + read);
                assertEquals(loader.toString(), parts[2], "context class loader: " + read);
            }
        }
        assertEquals(4, threadNames.size());
        assertForksClosedBeforeParent(events, 4);
        for (String fork : List.of("fork-1", "fork-2", "fork-3", "fork-4")) {
            assertEquals(reader.threads.get(fork).get(0).split("\\|")[0], reader.closedOn.get(fork),
                "a fork is closed by the thread of its range, as soon as the range is staged");
        }
    }

    /** An interrupted caller still waits for every range before the commit. */
    @Test
    void theWriterWaitsForEveryRangeEvenWhenInterrupted(@TempDir Path tmp) throws Exception {
        var release = new CountDownLatch(1);
        var finished = new java.util.concurrent.atomic.AtomicInteger();
        List<Iterator<ArchiveRow>> ranges = new ArrayList<>();
        for (int range = 0; range < 3; range++) {
            int base = range * 10;
            ranges.add(new Iterator<>() {
                int next = base;

                @Override public boolean hasNext() {
                    if (next == base + 5) {
                        try {
                            release.await();
                        } catch (InterruptedException e) {
                            throw new IllegalStateException(e);
                        }
                    }
                    boolean more = next < base + 10;
                    if (!more) finished.incrementAndGet();
                    return more;
                }

                @Override public ArchiveRow next() {
                    byte[] source = ("{\"doc\":" + next + "}").getBytes(StandardCharsets.UTF_8);
                    return new ArchiveRow(source, source.length, new DocumentMetadata(null, null, "stored", next++));
                }
            });
        }
        var rangesDoneAtReturn = new java.util.concurrent.atomic.AtomicInteger(-1);
        Thread caller = new Thread(() -> {
            try {
                new DeltaTableWriter(tmp.resolve("table").toString())
                    .writeArchive(new Configuration(), ranges, () -> {}, ARCHIVE, 0, ARCHIVE.key(0));
            } catch (Throwable ignored) {
                // An interrupted commit may fail; this test is about the ranges.
            }
            rangesDoneAtReturn.set(finished.get());
        });
        caller.start();
        Thread.sleep(300);
        caller.interrupt();
        Thread.sleep(300);
        assertTrue(caller.isAlive(), "the writer does not return while ranges still read");
        release.countDown();
        caller.join(60_000);
        assertEquals(3, rangesDoneAtReturn.get(), "every range ended before the writer returned");
    }

    @Test
    void joiningRestoresTheInterrupt() throws Exception {
        var done = new CountDownLatch(1);
        Thread range = new Thread(() -> {
            try {
                done.await();
            } catch (InterruptedException ignored) {
            }
        });
        range.start();
        var restored = new AtomicBoolean();
        Thread caller = new Thread(() -> {
            DeltaTableWriter.joinUninterruptibly(List.of(range));
            restored.set(Thread.currentThread().isInterrupted() && !range.isAlive());
        });
        caller.start();
        Thread.sleep(100);
        caller.interrupt();
        Thread.sleep(100);
        assertTrue(caller.isAlive());
        done.countDown();
        caller.join(10_000);
        assertTrue(restored.get());
    }

    @Test
    void aCancelledRangeThrowsInsteadOfEnding() {
        var cancelled = new AtomicBoolean();
        var rows = new ShardRanges.ArchiveRowIterator(new ForkingReader(10, new ArrayList<>(), doc -> {}), 3, 6, cancelled);
        assertTrue(rows.hasNext());
        assertEquals(3, rows.next().metadata().slot());
        cancelled.set(true);
        assertThrows(CancellationException.class, rows::hasNext);
        var exhausted = new ShardRanges.ArchiveRowIterator(new ForkingReader(10, new ArrayList<>(), doc -> {}), 8, 10, cancelled);
        assertThrows(CancellationException.class, exhausted::hasNext, "even at its end a cancelled range never ends normally");
    }

    @Test
    void cutsBalanceLiveDocumentsDeterministically() throws Exception {
        // Every third document deleted, and a long deleted run in the middle.
        ShardReader reader = new ForkingReader(10_000, new ArrayList<>(), doc -> {}) {
            @Override public boolean isLive(int docId) {
                return docId % 3 != 0 && (docId < 4000 || docId > 6000);
            }
        };
        long live = java.util.stream.IntStream.range(0, 10_000).filter(d -> d % 3 != 0 && (d < 4000 || d > 6000)).count();
        for (int k : new int[]{1, 2, 3, 7, 8, 64}) {
            var plan = ShardRanges.plan(reader, k, Long.MAX_VALUE, 1);
            assertEquals(k, plan.k());
            assertEquals(live, plan.liveDocs());
            assertEquals(0, plan.bounds()[0]);
            assertEquals(10_000, plan.bounds()[k]);
            for (int r = 0; r < k; r++) {
                int from = plan.bounds()[r], to = plan.bounds()[r + 1];
                assertTrue(from < to);
                long rangeLive = java.util.stream.IntStream.range(from, to).filter(d -> d % 3 != 0 && (d < 4000 || d > 6000)).count();
                assertEquals(live * (r + 1) / k - live * r / k, rangeLive, "live documents of range " + r + " of " + k);
                if (r > 0) assertTrue(reader.isLive(from), "a range starts at a live document");
            }
            assertArrayEquals(plan.bounds(), ShardRanges.plan(reader, k, Long.MAX_VALUE, 1).bounds(), "deterministic");
        }
        System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "2000");
        assertEquals(2, ShardRanges.plan(reader, 8, Long.MAX_VALUE, 1).k(), "ranges of at least minRangeLiveDocs");
        System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "1");
        assertEquals(1, ShardRanges.plan(new ForkingReader(0, new ArrayList<>(), doc -> {}), 8, Long.MAX_VALUE, 1).k());
    }

    @Test
    void theHeapBoundsTheRanges() {
        long mib = 1L << 20;
        assertEquals(Integer.MAX_VALUE, ShardRanges.heapBound(Long.MAX_VALUE, 4));
        assertEquals(1, ShardRanges.heapBound(512 * mib, 1));
        assertEquals((4096 * mib - 512 * mib) / ShardRanges.PER_RANGE_HEAP_BYTES, ShardRanges.heapBound(4096 * mib, 1));
        assertEquals((1024 * mib - 512 * mib) / ShardRanges.PER_RANGE_HEAP_BYTES, ShardRanges.heapBound(4096 * mib, 4));
        int bound = (int) ((1024 * mib - 512 * mib) / ShardRanges.PER_RANGE_HEAP_BYTES);
        assertEquals(Math.min(8, bound), ShardRanges.effective(8, 1_000_000, 4096 * mib, 4));
        assertEquals(1, ShardRanges.effective(8, 1_000_000, 256 * mib, 1));
        assertEquals(3, ShardRanges.effective(3, 1_000_000, Long.MAX_VALUE, 1));
    }

    /**
     * A synthetic-source range's reader reconstructs a batch ahead of it (twice the worker's byte
     * limit while converting), which the heap bound counts on top of the range's own heap, while a
     * stored index or an export without metadata reads nothing ahead.
     */
    @Test
    void theHeapBoundCountsASyntheticRangesReconstructionBatch() throws Exception {
        long mib = 1L << 20;
        var json = new com.fasterxml.jackson.databind.ObjectMapper();
        var synthetic = json.readTree("{\"settings\":{\"index\":{\"mapping\":{\"source\":{\"mode\":\"synthetic\"}}}}}");
        var logsdb = json.readTree("{\"settings\":{\"index.mode\":\"logsdb\"}}");
        var stored = json.readTree("{\"settings\":{\"index.number_of_shards\":\"1\"}}");
        assertEquals(2 * de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker.DEFAULT_BATCH_BYTES,
            ShardRanges.syntheticReadAheadBytes());
        assertEquals(16 * mib, ShardRanges.readAheadBytes(synthetic));
        assertEquals(16 * mib, ShardRanges.readAheadBytes(logsdb));
        assertEquals(0, ShardRanges.readAheadBytes(stored));
        assertEquals(0, ShardRanges.readAheadBytes(null));
        String previous = System.setProperty(
            de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker.BATCH_BYTES_PROPERTY, Long.toString(mib));
        try {
            assertEquals(2 * mib, ShardRanges.readAheadBytes(synthetic), "the batch limit the worker really uses");
        } finally {
            if (previous == null) System.clearProperty(de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker.BATCH_BYTES_PROPERTY);
            else System.setProperty(de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker.BATCH_BYTES_PROPERTY, previous);
        }
        long syntheticRange = ShardRanges.PER_RANGE_HEAP_BYTES + 16 * mib;
        assertEquals((4096 * mib - 512 * mib) / syntheticRange, ShardRanges.heapBound(4096 * mib, 1, 16 * mib));
        assertEquals(ShardRanges.heapBound(4096 * mib, 1), ShardRanges.heapBound(4096 * mib, 1, 0));
        // 1 GiB per worker: 12 stored ranges, 9 synthetic ones.
        assertEquals(12, ShardRanges.effective(16, 1_000_000, 4096 * mib, 4));
        assertEquals(9, ShardRanges.effective(16, 1_000_000, 4096 * mib, 4, 16 * mib));
        var settings = new ExportSettings(SnapshotSelection.DEFAULT, 4, 0);
        String previousThreads = System.setProperty(ShardRanges.SHARD_THREADS_KEY, "10");
        try {
            var warnings = settings.resourceWarnings(4096 * mib, 8, 128);
            assertTrue(warnings.stream().anyMatch(w -> w.equals("--parallelism 4 x --shard-threads 10 x 56 MiB per synthetic-source"
                + " range (its reconstruction batch included) exceeds the maximum heap of 4096 MiB; a synthetic-source shard is"
                + " read in at most 9 range(s), raise -Xmx for more")), warnings.toString());
            System.setProperty(ShardRanges.SHARD_THREADS_KEY, "16");
            warnings = settings.resourceWarnings(4096 * mib, 8, 128);
            assertTrue(warnings.stream().anyMatch(w -> w.contains("each shard is read in at most 12 range(s) (9 for"
                + " synthetic-source indices, whose ranges also hold a reconstruction batch), raise -Xmx")), warnings.toString());
        } finally {
            if (previousThreads == null) System.clearProperty(ShardRanges.SHARD_THREADS_KEY);
            else System.setProperty(ShardRanges.SHARD_THREADS_KEY, previousThreads);
        }
    }

    @Test
    void resourceWarningsCountEveryRangeAndItsUploadBuffers() {
        long mib = 1L << 20;
        var settings = new ExportSettings(SnapshotSelection.DEFAULT, 2, 0);
        String previous = System.clearProperty(ShardRanges.SHARD_THREADS_KEY);
        try {
            System.setProperty(ShardRanges.SHARD_THREADS_KEY, "1");
            assertEquals(List.of(), ExportSettings.DEFAULT.resourceWarnings(mib, 8, 1), "1 x 1 warns about nothing");
            System.setProperty(ShardRanges.SHARD_THREADS_KEY, "8");
            var warnings = settings.resourceWarnings(1400 * mib, 8, 128);
            assertEquals(4, warnings.size(), warnings.toString());
            assertTrue(warnings.get(0).startsWith("--parallelism 2 x 1 GiB exceeds"), warnings.get(0));
            assertTrue(warnings.get(1).contains("--parallelism 2 x --shard-threads 8 x " + ShardRanges.PER_RANGE_HEAP_BYTES / mib
                + " MiB per range exceeds the maximum heap of 1400 MiB; each shard is read in at most "
                + ShardRanges.heapBound(1400 * mib, 2) + " range(s)"), warnings.get(1));
            assertTrue(warnings.get(2).contains("writes up to 16 Parquet files at once"), warnings.get(2));
            assertEquals("--parallelism 2 x --shard-threads 8 ranges can each read a document before the document memory gate"
                + " knows its size: 16 x (250 + 40) MiB for documents of up to 100 MiB, plus 2 x 512 MiB reserve, exceeds the"
                + " maximum heap of 1400 MiB; raise -Xmx or lower --shard-threads if the index has such documents", warnings.get(3));
            assertEquals(1, settings.resourceWarnings(Long.MAX_VALUE, 8, 128).size(), "only the upload buffers without a heap limit");
            var local = settings.resourceWarnings(1400 * mib, 8, 128, false);
            assertEquals(3, local.size(), local.toString());
            assertTrue(local.stream().noneMatch(w -> w.contains("fs.s3a.buffer.dir")), "no upload buffers for a local target");
            assertEquals(List.of(), settings.resourceWarnings(Long.MAX_VALUE, 8, 128, false));
            assertEquals(3, ExportSettings.DEFAULT.resourceWarnings(600 * mib, 8, 1).size(), "ranges at --parallelism 1 too");
            assertEquals(1, ExportSettings.DEFAULT.resourceWarnings(4096 * mib, 8, 1).size(),
                "8 x (250 + 40) MiB + 512 MiB fit 4 GiB: only the upload buffers");
            System.setProperty(ShardRanges.SHARD_THREADS_KEY, "4");
            var oneGib = ExportSettings.DEFAULT.resourceWarnings(1024 * mib, 8, 1, false);
            assertEquals(1, oneGib.size(), "4 x 250 MiB fit 1 GiB, but not with the ranges' own heap and the reserve: " + oneGib);
            assertTrue(oneGib.get(0).contains("4 x (250 + 40) MiB for documents of up to 100 MiB, plus 1 x 512 MiB reserve"), oneGib.get(0));
            System.setProperty(ShardRanges.SHARD_THREADS_KEY, "nonsense");
            assertEquals(List.of(), ExportSettings.DEFAULT.resourceWarnings(mib, 8, 1), "the export reports an invalid setting");
        } finally {
            if (previous == null) System.clearProperty(ShardRanges.SHARD_THREADS_KEY);
            else System.setProperty(ShardRanges.SHARD_THREADS_KEY, previous);
        }
    }

    @Test
    void settingsAreAutoOrOneToSixtyFour() {
        assertEquals(1, ShardRanges.resolve("1", 1));
        assertEquals(64, ShardRanges.resolve("64", 1));
        int cores = Runtime.getRuntime().availableProcessors();
        assertEquals(Math.max(1, Math.min(8, cores)), ShardRanges.resolve("auto", 1));
        assertEquals(Math.max(1, Math.min(8, cores / 4)), ShardRanges.resolve("AUTO", 4));
        assertEquals(1, ShardRanges.resolve("auto", 1000));
        for (String invalid : new String[]{"0", "65", "-1", "two", "", "1.5"})
            assertThrows(IllegalArgumentException.class, () -> ShardRanges.resolve(invalid, 1), invalid);
        var conf = new Configuration();
        // The default, not a -Dgefrierschrank.shard.threads that a forced-K sweep passes to the whole module.
        String ambient = System.clearProperty(ShardRanges.SHARD_THREADS_KEY);
        try {
            assertEquals(ShardRanges.resolve("auto", 1), ShardRanges.requested(conf, 1), "the default is auto");
            System.setProperty(ShardRanges.SHARD_THREADS_KEY, "3");
            conf.set(ShardRanges.SHARD_THREADS_KEY, "5");
            assertEquals(5, ShardRanges.requested(conf, 1), "the Hadoop key before the JVM property");
        } finally {
            if (ambient == null) System.clearProperty(ShardRanges.SHARD_THREADS_KEY);
            else System.setProperty(ShardRanges.SHARD_THREADS_KEY, ambient);
        }
    }
}
