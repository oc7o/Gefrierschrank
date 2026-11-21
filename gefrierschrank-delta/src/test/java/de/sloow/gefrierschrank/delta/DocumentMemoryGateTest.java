package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The document memory gate: a document that can never fit the heap budget quarantines its
 * shard instead of ending the run with an OutOfMemoryError, concurrent shards and ranges never
 * reserve more than the budget, every path gives its reservation back, and nothing deadlocks.
 */
@Timeout(180)
class DocumentMemoryGateTest {

    private static final Path FIXTURE_REPO = Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");
    private static final String INDEX_NAME = "test_data_7_17_9";
    private static final ArchiveContext ARCHIVE =
        new ArchiveContext(INDEX_NAME, "repo-index-id", "snap-uuid", "snap", INDEX_NAME, "snap-uuid");
    private static final long MIB = 1L << 20;

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

    /**
     * A JSON object of exactly {@code bytes} bytes whose content names its shard and doc, built in
     * one array: the reader's own allocation happens before the gate can see the document.
     */
    static byte[] source(int shard, int doc, int bytes) {
        byte[] head = ("{\"shard\":" + shard + ",\"doc\":" + doc + ",\"blob\":\"").getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[bytes];
        System.arraycopy(head, 0, out, 0, head.length);
        for (int i = head.length; i < bytes - 2; i++) out[i] = (byte) ('a' + (i * 7 + doc) % 26);
        out[bytes - 2] = '"';
        out[bytes - 1] = '}';
        return out;
    }

    /** Documents of the sizes {@code sizes} gives per shard and doc; forks share everything. */
    record SizedReader(int shard, int docs, IntFunction<Integer> size) implements ShardReader {
        @Override public String runtimeName() { return "sized"; }
        @Override public int docCount() { return docs; }
        @Override public ShardReader fork() { return new SizedReader(shard, docs, size); }
        @Override public void close() {}

        @Override public DocumentMetadata documentMetadata(int docId) {
            return new DocumentMetadata("id-" + docId, null, "stored", docId);
        }

        @Override public void readSource(int docId, SourceConsumer consumer) {
            byte[] bytes = source(shard, docId, size.apply(docId));
            consumer.accept(bytes, bytes.length);
        }
    }

    private static DumpToDeltaPipeline pipeline(String table, Configuration conf, DumpToDeltaPipeline.ShardReaderOpener opener,
                                                DocumentMemoryGate gate) {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        return new DumpToDeltaPipeline(new RepositoryInspector(store), store, new DeltaTableWriter(table), conf, opener)
            .withDocumentMemoryGate(gate);
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

    private static int shardIdOf(ShardSnapshot shard) {
        return Integer.parseInt(shard.snapshotName().substring("virtual-shard-".length()));
    }

    private static void assertReleased(DocumentMemoryGate gate) {
        assertEquals(0, gate.reservedKiB(), "every reservation released");
        assertEquals(0, gate.registeredRanges(), "every range closed");
    }

    @Test
    void aDocumentAboveTheBudgetQuarantinesItsShardAndARerunRetriesIt(@TempDir Path tmp) throws Exception {
        String table = tmp.resolve("table").toString();
        // Budget: 64 MiB - 2 x 8 MiB = 48 MiB with one writing range; 12 MiB x 5 needs 60 MiB.
        var small = new DocumentMemoryGate(64 * MIB, 8 * MIB, 5.0);
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> {
            int id = shardIdOf(shard);
            return new SizedReader(id, 4, doc -> id == 1 && doc == 2 ? (int) (12 * MIB) : 200);
        };
        RunReport first = pipeline(table, new Configuration(), opener, small)
            .runSnapshot(ARCHIVE, null, catalog(3), RunReportListener.NO_OP);
        var tooLarge = first.shards().get(1);
        assertEquals(RunReport.Outcome.QUARANTINED, tooLarge.outcome());
        assertEquals("DocumentTooLargeException", tooLarge.errorClass());
        assertFalse(tooLarge.errorMessage().contains("blob") || tooLarge.errorMessage().contains("aaaa"),
            "no document content in the report: " + tooLarge.errorMessage());
        assertEquals(RunReport.Outcome.COMMITTED, first.shards().get(2).outcome(), "the next shard still commits");
        assertEquals(2, first.committedCount());
        assertReleased(small);
        assertEquals(ParquetFooters.addFilePaths(new Configuration(), table), ParquetFooters.parquetFilesOnDisk(Path.of(table)));

        var larger = new DocumentMemoryGate(256 * MIB, 8 * MIB, 5.0);
        RunReport second = pipeline(table, new Configuration(), opener, larger)
            .runSnapshot(ARCHIVE, null, catalog(3), RunReportListener.NO_OP);
        assertEquals(1, second.committedCount(), "the quarantined shard is retried, not skipped");
        assertEquals(2, second.skippedCount());
        assertReleased(larger);
    }

    @Test
    void theMessageHoldsSizesOnly() {
        var failure = new DocumentMemoryGate.DocumentTooLargeException(12 * MIB, 60 * MIB, 48 * MIB);
        assertEquals("a document of 12582912 bytes needs about 62914560 bytes of heap, more than the 50331648 bytes "
            + "available for documents; raise -Xmx or lower --parallelism or --shard-threads", failure.getMessage());
        assertInstanceOf(IOException.class, failure);
    }

    /**
     * Three shards at once, each in three ranges, with documents of 1-6 MiB against a budget that
     * holds only a few of them beside every range's reading room: no admission takes the charges
     * above the budget, documents that wait do so in temporary files and are exported byte-exact,
     * and every range gives its memory back.
     */
    @Test
    void concurrentShardsAndRangesStayWithinTheBudgetAndReleaseEverything(@TempDir Path tmp) throws Exception {
        String table = tmp.resolve("table").toString();
        var gate = new DocumentMemoryGate(160 * MIB, 4 * MIB, 5.0);
        IntFunction<Integer> sizes = doc -> doc % 3 == 0 ? (int) ((1 + doc % 6) * MIB) : 300;
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> new SizedReader(shardIdOf(shard), 24, sizes);
        var conf = new Configuration();
        conf.set(ShardRanges.SHARD_THREADS_KEY, "3");
        conf.set(DeltaTableWriter.PARQUET_CODEC_KEY, "uncompressed");
        RunReport report = pipeline(table, conf, opener, gate).withWorkPool(new ShardWorkPool(3, 0))
            .runSnapshot(ARCHIVE, null, catalog(6), RunReportListener.NO_OP);

        assertEquals(6, report.committedCount(), String.valueOf(report.shards()));
        assertEquals(6 * 8, gate.admittedDocuments(), "every document of 1 MiB or more went through the gate");
        assertTrue(gate.maxOverBudgetKiB() <= 0, "an admission exceeded the budget by " + gate.maxOverBudgetKiB() + " KiB");
        assertTrue(gate.documentsWaited() > 0, "documents waited, in temporary files");
        assertReleased(gate);
        Map<String, String> sources = new TreeMap<>();
        try (var rows = io.delta.standalone.DeltaLog.forTable(conf, table).update().open()) {
            while (rows.hasNext()) {
                var row = rows.next();
                assertNull(sources.put(row.getLong("_shard") + "/" + row.getLong("_doc"), row.getString("_source")));
            }
        }
        assertEquals(6 * 24, sources.size());
        for (var entry : sources.entrySet()) {
            String[] key = entry.getKey().split("/");
            int shard = Integer.parseInt(key[0]), doc = Integer.parseInt(key[1]);
            assertEquals(new String(source(shard, doc, sizes.apply(doc)), StandardCharsets.UTF_8), entry.getValue(),
                "byte-exact read-back of " + entry.getKey());
        }
    }

    /**
     * Eight ranges, registered before any document, admit random large documents from eight threads
     * at once, keeping nothing of a waiting document: no admission takes the charges (reading room
     * included) above the budget, nobody deadlocks although seven ranges' reading room alone exceeds
     * the budget, no document fails, and all is released.
     */
    @Test
    void reservationsNeverExceedTheBudgetUnderContention() throws Exception {
        var gate = new DocumentMemoryGate(100 * MIB, 4 * MIB, 3.0);
        int threads = 8;
        var registeredAll = new java.util.concurrent.CyclicBarrier(threads);
        var failures = java.util.Collections.synchronizedList(new ArrayList<Throwable>());
        var workers = new ArrayList<Thread>();
        for (int t = 0; t < threads; t++) {
            long seed = t;
            Thread worker = new Thread(() -> {
                var random = new java.util.Random(seed);
                try (var range = gate.register()) {
                    registeredAll.await(30, TimeUnit.SECONDS);
                    for (int doc = 0; doc < 200; doc++) {
                        range.admit(random.nextBoolean() ? 1000 : (1 + random.nextInt(6)) * MIB);
                        if (random.nextInt(4) == 0) Thread.sleep(1);
                    }
                } catch (Throwable t2) {
                    failures.add(t2);
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) worker.join(120_000);
        assertEquals(List.of(), failures);
        assertTrue(gate.admittedDocuments() > 500);
        assertTrue(gate.maxOverBudgetKiB() <= 0, "the reserved total exceeded the budget by " + gate.maxOverBudgetKiB() + " KiB");
        assertReleased(gate);
    }

    /** A document admitted and then rejected by validation still gives its reservation back. */
    @Test
    void failingPathsReleaseTheirReservation(@TempDir Path tmp) throws Exception {
        var gate = new DocumentMemoryGate(128 * MIB, 4 * MIB, 5.0);
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> new ShardReader() {
            @Override public String runtimeName() { return "invalid"; }
            @Override public int docCount() { return 3; }
            @Override public ShardReader fork() { return this; }
            @Override public void close() {}

            @Override public void readSource(int docId, SourceConsumer consumer) {
                byte[] bytes = source(0, docId, (int) (2 * MIB));
                if (docId == 1) bytes[bytes.length - 1] = ',';
                consumer.accept(bytes, bytes.length);
            }
        };
        for (String threads : new String[]{"1", "3"}) {
            var conf = new Configuration();
            conf.set(ShardRanges.SHARD_THREADS_KEY, threads);
            RunReport report = pipeline(tmp.resolve("t" + threads).toString(), conf, opener, gate)
                .runSnapshot(ARCHIVE, null, catalog(2), RunReportListener.NO_OP);
            assertEquals(2, report.quarantinedCount());
            assertReleased(gate);
        }
    }

    @Test
    void anInterruptWhileWaitingIsAnIOException() throws Exception {
        var gate = new DocumentMemoryGate(64 * MIB, 8 * MIB, 1.0, 0);
        var holder = gate.register();
        var second = gate.register();
        holder.admit(40 * MIB);
        var failure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
        Thread waiter = new Thread(() -> {
            try (var range = second) {
                range.admit(30 * MIB);
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        waiter.start();
        await(() -> gate.documentsWaited() == 1, "the second document waits for the first");
        waiter.interrupt();
        waiter.join(10_000);
        assertInstanceOf(InterruptedIOException.class, failure.get());
        holder.close();
        assertReleased(gate);
    }

    @Test
    void aWaitingDocumentProceedsOnceTheHolderMovesOn() throws Exception {
        var gate = new DocumentMemoryGate(64 * MIB, 8 * MIB, 1.0, 0);
        var first = gate.register();
        first.admit(40 * MIB);
        var admitted = new CountDownLatch(1);
        Thread second = new Thread(() -> {
            try (var range = gate.register()) {
                range.admit(30 * MIB);
                admitted.countDown();
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
        second.start();
        assertFalse(admitted.await(300, TimeUnit.MILLISECONDS));
        first.admit(100);
        assertTrue(admitted.await(10, TimeUnit.SECONDS), "releasing on the next document lets the waiter in");
        second.join(10_000);
        first.close();
        first.close();
        assertReleased(gate);
    }

    /**
     * What a range's reader holds ahead of it (a synthetic-source reconstruction batch) comes off
     * the budget while the range is registered, so a document that fits beside a stored range can
     * be too large beside a synthetic one.
     */
    @Test
    void aRangesReadAheadComesOffTheBudgetWhileItIsRegistered() throws IOException {
        var gate = new DocumentMemoryGate(1024 * MIB, 40 * MIB, 6.0);
        long alone = gate.budgetKiB();
        try (var stored = gate.register()) {
            assertEquals(alone - 40 * 1024, gate.budgetKiB());
            try (var synthetic = gate.register(null, 16 * MIB)) {
                assertEquals(alone - 80 * 1024 - 16 * 1024, gate.budgetKiB());
                assertEquals(16 * 1024, gate.largestRangeReadAheadKiB());
            }
            assertEquals(alone - 40 * 1024, gate.budgetKiB(), "closing the range gives its read-ahead back");
        }
        assertEquals(alone, gate.budgetKiB());

        // 128 MiB - 2 x 8 MiB leaves 112 MiB beside a stored range, 96 MiB beside a synthetic one;
        // a 17 MiB document needs 6 x 17 = 102 MiB.
        var small = new DocumentMemoryGate(128 * MIB, 8 * MIB, 6.0);
        try (var stored = small.register()) {
            stored.admit(17 * MIB);
        }
        try (var synthetic = small.register(null, 16 * MIB)) {
            assertThrows(DocumentMemoryGate.DocumentTooLargeException.class, () -> synthetic.admit(17 * MIB));
        }
        assertReleased(small);
    }

    /** The pipeline registers a synthetic-source index's ranges with the reconstruction batch they read ahead. */
    @Test
    void aSyntheticSourceIndexRegistersItsRangesWithTheReconstructionBatch(@TempDir Path tmp) throws Exception {
        var logsdb = new com.fasterxml.jackson.databind.ObjectMapper().readTree("{\"settings\":{\"index.mode\":\"logsdb\"}}");
        for (boolean synthetic : new boolean[]{true, false}) {
            var gate = new DocumentMemoryGate(1024 * MIB, 8 * MIB, 6.0);
            RunReport report = pipeline(tmp.resolve("table-" + synthetic).toString(), new Configuration(),
                (shard, dir) -> new SizedReader(0, 4, doc -> 200), gate)
                .runSnapshot(ARCHIVE, synthetic ? logsdb : null, catalog(1), RunReportListener.NO_OP);
            assertEquals(1, report.committedCount(), report.toString());
            assertEquals(synthetic ? ShardRanges.syntheticReadAheadBytes() >> 10 : 0, gate.largestRangeReadAheadKiB());
            assertReleased(gate);
        }
    }

    @Test
    void smallDocumentsAndUnboundedHeapsBypassTheGate() throws IOException {
        var gate = new DocumentMemoryGate(64 * MIB, 8 * MIB, 5.0);
        try (var range = gate.register()) {
            range.admit(DocumentMemoryGate.BYPASS_BYTES - 1);
            assertEquals(0, gate.reservedKiB());
            range.admit(DocumentMemoryGate.BYPASS_BYTES);
            assertEquals(5 * 1024, gate.reservedKiB());
        }
        var unbounded = new DocumentMemoryGate(Long.MAX_VALUE, 8 * MIB, 5.0);
        try (var range = unbounded.register()) {
            range.admit(1L << 40);
            assertEquals(0, unbounded.reservedKiB());
        }
        assertThrows(IllegalArgumentException.class, () -> new DocumentMemoryGate(64 * MIB, 8 * MIB, 0));
    }

    private static void await(java.util.function.BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) fail("timed out waiting until " + what);
            Thread.sleep(5);
        }
    }

    /** Runs {@code admit} on its own thread; the future completes with null or the failure. */
    private static java.util.concurrent.CompletableFuture<Throwable> admitting(DocumentMemoryGate.Range range, long bytes) {
        var outcome = new java.util.concurrent.CompletableFuture<Throwable>();
        Thread thread = new Thread(() -> {
            try {
                range.admit(bytes);
                outcome.complete(null);
            } catch (Throwable t) {
                outcome.complete(t);
            }
        });
        thread.setDaemon(true);
        thread.start();
        return outcome;
    }

    /** A waiting range keeps nothing of its document (the pipeline has moved it to disk), so nothing is charged for it. */
    @Test
    void aWaitingDocumentIsNotCharged() throws Exception {
        var gate = new DocumentMemoryGate(64 * MIB, 8 * MIB, 1.0, 0);
        var holder = gate.register();
        var waiter = gate.register();
        holder.admit(30 * MIB);
        var admitted = admitting(waiter, 20 * MIB);
        await(() -> gate.documentsWaited() == 1, "the document waits");
        assertEquals(30 * 1024, gate.reservedKiB());
        assertEquals(30 * 1024, gate.chargedKiB());
        assertFalse(admitted.isDone());
        holder.admit(100);
        assertNull(admitted.get(10, TimeUnit.SECONDS));
        assertEquals(20 * 1024, gate.reservedKiB());
        holder.close();
        waiter.close();
        assertReleased(gate);
    }

    /**
     * A range whose own open file holds the heap its next document needs has the writer close that
     * file instead of waiting for itself, which would never end.
     */
    @Test
    void aRangeClosesItsOpenFileInsteadOfWaitingForIt() throws Exception {
        var gate = new DocumentMemoryGate(64 * MIB, 8 * MIB, 2.0, 0);
        var closes = new java.util.concurrent.atomic.AtomicInteger();
        try (var range = gate.register()) {
            range.admit(20 * MIB, closes::incrementAndGet, null);
            range.openFileRetains(30 * MIB);
            assertEquals(40 * 1024, gate.reservedKiB(), "the file's share moved over from the document");
            range.admit(20 * MIB, closes::incrementAndGet, null);
            assertEquals(1, closes.get());
            assertEquals(1, gate.filesClosedForHeap());
            assertEquals(40 * 1024, gate.reservedKiB(), "only the new document is reserved");
            range.openFileRetains(8 * MIB);
            range.admit(20 * MIB, closes::incrementAndGet, null);
            assertEquals(1, closes.get(), "a file whose heap fits beside the next document stays open");
        }
        assertReleased(gate);
    }

    /** A range that keeps writing is asked to close its open file when another range waits for heap. */
    @Test
    void aWaitingRangeAsksWritingRangesToCloseTheirFiles() throws Exception {
        var gate = new DocumentMemoryGate(64 * MIB, 8 * MIB, 1.0, 0);
        var writing = gate.register();
        var waiter = gate.register();
        writing.admit(20 * MIB);
        writing.openFileRetains(20 * MIB);
        writing.admit(100);
        assertFalse(writing.closeFileRequested());
        var admitted = admitting(waiter, 30 * MIB);
        await(writing::closeFileRequested, "the writing range is asked to close its file");
        assertFalse(admitted.isDone());
        writing.openFileClosed();
        assertNull(admitted.get(10, TimeUnit.SECONDS));
        assertFalse(writing.closeFileRequested());
        writing.close();
        waiter.close();
        assertReleased(gate);
    }

    /** Waiting documents go first come, first served: a later one that would fit waits behind the first. */
    @Test
    void waitingDocumentsAreAdmittedFirstComeFirstServed() throws Exception {
        // Budget 80 MiB with three ranges; the multiplier doubles each document.
        var gate = new DocumentMemoryGate(120 * MIB, 10 * MIB, 2.0, 0);
        var holder = gate.register();
        var first = gate.register();
        var second = gate.register();
        holder.admit(30 * MIB);
        var firstAdmitted = admitting(first, 20 * MIB);
        await(() -> gate.documentsWaited() == 1, "the first document waits");
        var secondAdmitted = admitting(second, 5 * MIB);
        await(() -> gate.documentsWaited() == 2, "the second document waits behind it");
        Thread.sleep(200);
        assertFalse(secondAdmitted.isDone(), "60 + 10 MiB would fit, but the first came first");
        holder.admit(100);
        assertNull(firstAdmitted.get(10, TimeUnit.SECONDS));
        assertNull(secondAdmitted.get(10, TimeUnit.SECONDS));
        for (var range : List.of(holder, first, second)) range.close();
        assertReleased(gate);
    }

    /**
     * Every range that is not waiting may be reading a document as large as the largest seen, before
     * the gate can see it: that room is charged, so three 16 MiB documents are not admitted while the
     * other ranges may still be reading theirs. Waiting ranges hold nothing, so once every other
     * range waits the first waiter goes ahead, and no document fails.
     */
    @Test
    void readingRangesAreChargedRoomForOneMoreDocumentAsLarge() throws Exception {
        // Budget 80 MiB with three ranges. A 16 MiB document needs 32; the reading room is 40.
        var gate = new DocumentMemoryGate(120 * MIB, 10 * MIB, 2.0, 2.5);
        var a = gate.register();
        var b = gate.register();
        var c = gate.register();
        var aAdmitted = admitting(a, 16 * MIB);
        await(() -> gate.documentsWaited() == 1, "a waits while b and c may be reading");
        assertEquals(80 * 1024, gate.chargedKiB(), "b and c are charged their reading room, a nothing");
        var bAdmitted = admitting(b, 16 * MIB);
        assertNull(aAdmitted.get(10, TimeUnit.SECONDS), "a goes ahead once b waits: 40 (c reading) + 40 of 80");
        await(() -> gate.documentsWaited() == 2, "b waits");
        Thread.sleep(200);
        assertFalse(bAdmitted.isDone(), "a and c are charged 40 each");
        var cAdmitted = admitting(c, 16 * MIB);
        assertNull(bAdmitted.get(10, TimeUnit.SECONDS), "once c waits too, b fits beside a: 40 + 40 of 80");
        await(() -> gate.documentsWaited() == 3, "c waits: 40 + 40 + 40 exceed 80");
        a.admit(100);
        Thread.sleep(200);
        assertFalse(cAdmitted.isDone(), "a is reading again and charged its room");
        a.close();
        assertNull(cAdmitted.get(10, TimeUnit.SECONDS));
        assertTrue(gate.maxOverBudgetKiB() <= 0);
        b.close();
        c.close();
        assertReleased(gate);
    }

    /**
     * A range registers, and so starts reading, only once its reading room fits beside the others'
     * charges; it gives up waiting when its shard is cancelled or it is interrupted.
     */
    @Test
    void aRangeRegistersOnlyOnceItsReadingRoomFits() throws Exception {
        // Budget 180 MiB with one range, 170 with two. A 40 MiB document needs 80, its reading room is 100.
        var gate = new DocumentMemoryGate(200 * MIB, 10 * MIB, 2.0, 2.5);
        var holder = gate.register();
        holder.admit(40 * MIB);
        var registered = new java.util.concurrent.CompletableFuture<DocumentMemoryGate.Range>();
        Thread registering = new Thread(() -> {
            try {
                registered.complete(gate.register());
            } catch (Throwable t) {
                registered.completeExceptionally(t);
            }
        });
        registering.start();
        await(() -> gate.registrationsWaited() == 1, "the second range waits to register: 100 + 100 exceed 170");
        holder.admit(100);
        Thread.sleep(200);
        assertFalse(registered.isDone(), "the holder, reading again, is still charged its room");
        holder.close();
        try (var range = registered.get(10, TimeUnit.SECONDS)) {
            assertEquals(1, gate.registeredRanges());
            var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
            range.admit(40 * MIB);
            var waitingCancelled = new java.util.concurrent.CompletableFuture<Throwable>();
            Thread cancelledThread = new Thread(() -> {
                try (var never = gate.register(cancelled::get)) {
                    waitingCancelled.complete(null);
                } catch (Throwable t) {
                    waitingCancelled.complete(t);
                }
            });
            cancelledThread.start();
            await(() -> gate.registrationsWaited() == 2, "a third range waits to register");
            cancelled.set(true);
            assertInstanceOf(java.util.concurrent.CancellationException.class, waitingCancelled.get(10, TimeUnit.SECONDS));
            var interruptedFailure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
            Thread interrupted = new Thread(() -> {
                try (var never = gate.register()) {
                    interruptedFailure.set(new AssertionError("registered"));
                } catch (Throwable t) {
                    interruptedFailure.set(t);
                }
            });
            interrupted.start();
            await(() -> gate.registrationsWaited() == 3, "a fourth range waits to register");
            interrupted.interrupt();
            interrupted.join(10_000);
            assertInstanceOf(InterruptedIOException.class, interruptedFailure.get());
        }
        assertReleased(gate);
    }

    /** New ranges wait while a document waits, so a waiting document is not overtaken by new readers. */
    @Test
    void noRangeRegistersWhileADocumentWaits() throws Exception {
        var gate = new DocumentMemoryGate(64 * MIB, 8 * MIB, 1.0, 0);
        var holder = gate.register();
        var waiter = gate.register();
        holder.admit(30 * MIB);
        var admitted = admitting(waiter, 20 * MIB);
        await(() -> gate.documentsWaited() == 1, "the document waits");
        var registered = new java.util.concurrent.CompletableFuture<DocumentMemoryGate.Range>();
        Thread registering = new Thread(() -> {
            try {
                registered.complete(gate.register());
            } catch (Throwable t) {
                registered.completeExceptionally(t);
            }
        });
        registering.start();
        await(() -> gate.registrationsWaited() == 1, "a new range waits behind the waiting document");
        holder.close();
        assertNull(admitted.get(10, TimeUnit.SECONDS));
        registered.get(10, TimeUnit.SECONDS).close();
        waiter.close();
        assertReleased(gate);
    }

    private static long waitingFiles(Path dir) throws IOException {
        try (var files = Files.list(dir)) {
            return files.filter(file -> file.getFileName().toString().startsWith(ShardRanges.WAITING_PREFIX)).count();
        }
    }

    /**
     * The pipeline's side of waiting: a document that does not fit waits in a temporary file, so
     * nothing of it is in the heap or charged meanwhile; it is read back byte-exact once admitted,
     * and the file is gone afterwards, also when the shard is cancelled while it waits.
     */
    @Test
    void aDocumentThatHasToWaitWaitsInATemporaryFile(@TempDir Path waitingDir) throws Exception {
        var gate = new DocumentMemoryGate(64 * MIB, 8 * MIB, 1.0, 0);
        var holder = gate.register();
        holder.admit(30 * MIB);
        var reader = new SizedReader(0, 2, doc -> (int) (20 * MIB));
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        var rows = new ShardRanges.ArchiveRowIterator(reader, 0, 1, cancelled, gate, waitingDir);
        var first = new java.util.concurrent.CompletableFuture<ArchiveRow>();
        Thread ranging = new Thread(() -> {
            try {
                first.complete(rows.hasNext() ? rows.next() : null);
            } catch (Throwable t) {
                first.completeExceptionally(t);
            }
        });
        ranging.start();
        await(() -> gate.documentsWaited() == 1, "the document waits");
        assertEquals(1, waitingFiles(waitingDir));
        assertEquals(30 * 1024, gate.reservedKiB(), "nothing is reserved for the waiting document");
        holder.admit(100);
        ArchiveRow row = first.get(10, TimeUnit.SECONDS);
        assertArrayEquals(source(0, 0, (int) (20 * MIB)), java.util.Arrays.copyOf(row.source(), row.length()));
        assertEquals(0, row.metadata().slot());
        assertEquals(0, waitingFiles(waitingDir), "the temporary file is gone once the document is read back");
        rows.close();

        holder.admit(30 * MIB);
        var next = new ShardRanges.ArchiveRowIterator(reader, 1, 2, cancelled, gate, waitingDir);
        var second = new java.util.concurrent.CompletableFuture<Throwable>();
        Thread cancelledRange = new Thread(() -> {
            try {
                if (next.hasNext()) next.next();
                second.complete(null);
            } catch (Throwable t) {
                second.complete(t);
            }
        });
        cancelledRange.start();
        await(() -> gate.documentsWaited() == 2, "the next document waits");
        assertEquals(1, waitingFiles(waitingDir));
        cancelled.set(true);
        assertInstanceOf(java.util.concurrent.CancellationException.class, second.get(10, TimeUnit.SECONDS));
        assertEquals(0, waitingFiles(waitingDir), "a cancelled wait deletes its file");
        next.close();
        holder.close();
        assertReleased(gate);
    }

    @Test
    void aCancelledShardStopsWaiting() throws Exception {
        var gate = new DocumentMemoryGate(64 * MIB, 8 * MIB, 1.0, 0);
        var holder = gate.register();
        var waiter = gate.register();
        holder.admit(40 * MIB);
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        var outcome = new java.util.concurrent.CompletableFuture<Throwable>();
        Thread thread = new Thread(() -> {
            try {
                waiter.admit(30 * MIB, null, cancelled::get);
                outcome.complete(null);
            } catch (Throwable t) {
                outcome.complete(t);
            }
        });
        thread.start();
        await(() -> gate.documentsWaited() == 1, "the document waits");
        cancelled.set(true);
        assertInstanceOf(java.util.concurrent.CancellationException.class, outcome.get(10, TimeUnit.SECONDS));
        assertEquals(40 * 1024, gate.reservedKiB());
        holder.close();
        waiter.close();
        assertReleased(gate);
    }

    @Test
    void theReserveMultiplierIsAFiniteNumberAboveZero() {
        assertEquals(2.5, DocumentMemoryGate.reserveMultiplier(" 2.5 "));
        for (String invalid : new String[]{"abc", "", "0", "-1", "NaN", "Infinity"}) {
            var failure = assertThrows(IllegalArgumentException.class, () -> DocumentMemoryGate.reserveMultiplier(invalid), invalid);
            assertEquals("gefrierschrank.document.reserveMultiplier must be a number above 0", failure.getMessage());
        }
        assertThrows(IllegalArgumentException.class, () -> new DocumentMemoryGate(64 * MIB, 8 * MIB, Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> new DocumentMemoryGate(64 * MIB, 8 * MIB, 1.0, -1));
    }

    /**
     * The writer's side: after every row of 1 MiB or more it reports what its open file keeps (row
     * groups flushed so far, each holding copies of its largest value), it reports every closed
     * file, it closes its file when asked to after a row, and the producer can have it closed
     * between two rows.
     */
    @Test
    void theWriterReportsItsOpenFilesHeapAndClosesThemOnRequest(@TempDir Path tmp) throws Exception {
        class Accounting implements java.util.Iterator<ArchiveRow>, DeltaTableWriter.OpenFileHeap {
            final List<Long> retained = new ArrayList<>();
            int closed;
            boolean closeAfterNext;
            DeltaTableWriter.OpenFileCloser closer;
            int doc;
            final int[] sizes = {(int) (12 * MIB), (int) (12 * MIB), 300, (int) (12 * MIB), (int) (12 * MIB), 300};

            @Override public boolean hasNext() { return doc < sizes.length; }

            @Override public ArchiveRow next() {
                if (doc == 3) {
                    try {
                        closer.close();
                    } catch (IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }
                closeAfterNext = doc == 4;
                byte[] bytes = source(0, doc, sizes[doc]);
                var row = new ArchiveRow(bytes, bytes.length, new DocumentMetadata("id-" + doc, null, "stored", doc));
                doc++;
                return row;
            }

            @Override public void closeOpenFileWith(DeltaTableWriter.OpenFileCloser closer) { this.closer = closer; }
            @Override public void openFileRetains(long bytes) { retained.add(bytes); }
            @Override public void openFileClosed() { closed++; }
            @Override public boolean openFileCloseRequested() { return closeAfterNext; }
        }
        var rows = new Accounting();
        String table = tmp.resolve("table").toString();
        var conf = new Configuration();
        conf.set(DeltaTableWriter.PARQUET_CODEC_KEY, "uncompressed");
        var result = new DeltaTableWriter(table).writeArchive(conf, List.of(rows), ARCHIVE, 0, ARCHIVE.key(0));
        assertEquals(6, result.rows());
        // A 12 MiB row is a row group of its own (parquet-mr flushes at 32 MiB minus twice the average
        // row); each keeps two copies of it until its file is closed.
        long twelve = 12 * MIB;
        assertEquals(List.of(2 * twelve, 4 * twelve, 2 * twelve, 4 * twelve), rows.retained.stream()
            .map(bytes -> bytes / MIB * MIB).toList(), "reported per large row: " + rows.retained);
        assertEquals(3, rows.closed, "closed for the producer, on request after doc 4, and at the end");
        assertEquals(3, ParquetFooters.addFilePaths(conf, table).size());
        Map<Long, String> sources = new TreeMap<>();
        try (var read = io.delta.standalone.DeltaLog.forTable(conf, table).update().open()) {
            while (read.hasNext()) {
                var row = read.next();
                sources.put(row.getLong("_doc"), row.getString("_source"));
            }
        }
        for (int doc = 0; doc < rows.sizes.length; doc++) {
            assertEquals(new String(source(0, doc, rows.sizes[doc]), StandardCharsets.UTF_8), sources.get((long) doc));
        }
    }

    /**
     * A document that does not fit beside the shard's other ranges, but fits a single range's
     * budget, is not quarantined: the shard is read again as one range.
     */
    @Test
    void aShardWhoseDocumentFitsOnlyOneRangeIsReadAgainAsOneRange(@TempDir Path tmp) throws Exception {
        String table = tmp.resolve("table").toString();
        // Budget 64 - 4 x 8 = 32 MiB with three ranges, 48 MiB with one; 8 MiB x 5 needs 40.
        var gate = new DocumentMemoryGate(64 * MIB, 8 * MIB, 5.0);
        IntFunction<Integer> sizes = doc -> doc == 4 ? (int) (8 * MIB) : 300;
        var forks = new java.util.concurrent.atomic.AtomicInteger();
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> new ShardReader() {
            final SizedReader sized = new SizedReader(shardIdOf(shard), 6, sizes);
            @Override public String runtimeName() { return sized.runtimeName(); }
            @Override public int docCount() { return sized.docCount(); }
            @Override public void close() {}
            @Override public DocumentMetadata documentMetadata(int docId) { return sized.documentMetadata(docId); }
            @Override public void readSource(int docId, SourceConsumer consumer) { sized.readSource(docId, consumer); }

            @Override public ShardReader fork() {
                forks.incrementAndGet();
                return sized.fork();
            }
        };
        var conf = new Configuration();
        conf.set(ShardRanges.SHARD_THREADS_KEY, "3");
        var err = new java.io.ByteArrayOutputStream();
        var stderr = System.err;
        RunReport report;
        System.setErr(new java.io.PrintStream(err, true, StandardCharsets.UTF_8));
        try {
            report = pipeline(table, conf, opener, gate).runSnapshot(ARCHIVE, null, catalog(1), RunReportListener.NO_OP);
        } finally {
            System.setErr(stderr);
        }
        assertEquals(RunReport.Outcome.COMMITTED, report.shards().get(0).outcome(), String.valueOf(report.shards()));
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("warning: shard 0 of " + INDEX_NAME
            + ": a large document did not fit the heap beside 2 other range(s); read again as one range"), err.toString());
        assertEquals(4, forks.get(), "three range forks, then one for the single range");
        assertEquals(1, ParquetFooters.addFilePaths(conf, table).size(), "one range, one file");
        assertReleased(gate);
        Map<Long, String> sources = new TreeMap<>();
        try (var rows = io.delta.standalone.DeltaLog.forTable(conf, table).update().open()) {
            while (rows.hasNext()) {
                var row = rows.next();
                assertNull(sources.put(row.getLong("_doc"), row.getString("_source")));
            }
        }
        assertEquals(6, sources.size());
        assertEquals(new String(source(0, 4, (int) (8 * MIB)), StandardCharsets.UTF_8), sources.get(4L));
    }

    /**
     * -Xmx512m, four shards at once, each with a 48 MiB document: every shard commits in one run,
     * where every shard writing its document at the same time would not fit. The documents that do
     * not fit wait in temporary files and are admitted one after another. The heavy tests take a
     * few minutes together; they run with {@code -Dgefrierschrank.heavyTests=true}.
     */
    @Test
    @Tag("heavy")
    @EnabledIfSystemProperty(named = "gefrierschrank.heavyTests", matches = "true")
    void fourLargeDocumentsAtOnceFitASmallHeap(@TempDir Path tmp) throws Exception {
        String output = runChild(tmp, "512m", List.of(), 4, 4, 3, 1, 48, "pattern");
        assertTrue(output.contains("committed 4 of 4"), output);
    }

    /**
     * One shard of four poorly compressible 100 MiB documents read as four ranges, the first
     * document of every range large (an earlier version: OutOfMemoryError at 832-960 MiB, where
     * one range commits): the first document to reach the gate waits while the other ranges may be
     * reading one as large, waiting documents are on disk, so the documents are written one after
     * another and the shard commits in its four ranges.
     */
    @Test
    @Tag("heavy")
    @EnabledIfSystemProperty(named = "gefrierschrank.heavyTests", matches = "true")
    void fourRangesReadingALargeDocumentEachFitWhereOneRangeFits(@TempDir Path tmp) throws Exception {
        for (String heap : new String[]{"832m", "896m", "960m"}) {
            String output = runChild(tmp.resolve(heap), heap, List.of("-Dgefrierschrank.shard.threads=4",
                "-D" + ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY + "=1"), 1, 1, 40, 4, 100, "printable");
            assertTrue(output.contains("committed 1 of 1"), heap + ": " + output);
        }
    }

    /**
     * Eight ranges (auto on an eight-core machine; fewer at 768 MiB, where the heap bound allows 6),
     * one poorly compressible 64 MiB document first in each (an earlier version: OutOfMemoryError
     * at 768-1152 MiB).
     */
    @Test
    @Tag("heavy")
    @EnabledIfSystemProperty(named = "gefrierschrank.heavyTests", matches = "true")
    void eightRangesReadingALargeDocumentEachFit(@TempDir Path tmp) throws Exception {
        for (String heap : new String[]{"768m", "896m", "1024m", "1152m"}) {
            String output = runChild(tmp.resolve(heap), heap, List.of("-Dgefrierschrank.shard.threads=8",
                "-D" + ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY + "=1"), 1, 1, 40, 8, 64, "printable");
            assertTrue(output.contains("committed 1 of 1"), heap + ": " + output);
        }
    }

    /**
     * The same through a real Lucene 10 segment (LZ4 stored fields), eight ranges, eight 64 MiB
     * documents. A range's last small documents share a stored-fields block with the next range's
     * large document, which the range's reader decompresses and, before every range closed its
     * fork as soon as it was staged, kept until the whole shard was done: OutOfMemoryError at 896
     * and 1024 MiB, where the fake reader above commits.
     */
    @Test
    @Tag("heavy")
    @EnabledIfSystemProperty(named = "gefrierschrank.heavyTests", matches = "true")
    void eightRangesOverALuceneSegmentWithLargeDocumentsFit(@TempDir Path tmp) throws Exception {
        Path index = tmp.resolve("index");
        int docs = 40, big = 8, bigBytes = (int) (64 * MIB);
        var config = new org.apache.lucene.index.IndexWriterConfig()
            .setCodec(new org.apache.lucene.codecs.lucene103.Lucene103Codec(
                org.apache.lucene.codecs.lucene103.Lucene103Codec.Mode.BEST_SPEED))
            .setRAMBufferSizeMB(1024);
        try (var directory = new org.apache.lucene.store.NIOFSDirectory(index);
             var writer = new org.apache.lucene.index.IndexWriter(directory, config)) {
            for (int doc = 0; doc < docs; doc++) {
                var document = new org.apache.lucene.document.Document();
                document.add(new org.apache.lucene.document.StoredField("_id", "id-" + doc));
                byte[] source = heavySource(0, doc, doc % (docs / big) == 0 ? bigBytes : 500, "printable");
                document.add(new org.apache.lucene.document.StoredField("_source", new org.apache.lucene.util.BytesRef(source)));
                writer.addDocument(document);
            }
        }
        for (String heap : new String[]{"896m", "1024m"}) {
            String output = runChild(tmp.resolve(heap), heap, List.of("-Dgefrierschrank.shard.threads=8",
                "-D" + ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY + "=1", "-Dheavy.luceneIndex=" + index), 1, 1, docs, big, 64,
                "printable");
            assertTrue(output.contains("committed 1 of 1"), heap + ": " + output);
        }
    }

    /**
     * --parallelism 4 at the default one range per shard, four shards with two poorly compressible
     * 64 MiB documents each (an earlier version: OutOfMemoryError at 640 MiB).
     */
    @Test
    @Tag("heavy")
    @EnabledIfSystemProperty(named = "gefrierschrank.heavyTests", matches = "true")
    void fourShardsReadingLargeDocumentsAtOnceFit(@TempDir Path tmp) throws Exception {
        String output = runChild(tmp, "640m", List.of(), 4, 4, 10, 2, 64, "printable");
        assertTrue(output.contains("committed 4 of 4"), output);
    }

    /**
     * One shard of three consecutive poorly compressible 64 MiB documents, at heaps where each fits
     * on its own but the open Parquet file's copies of the earlier ones do not fit beside the next
     * (earlier versions, the first version of the gate among them: OutOfMemoryError at 600 and
     * 704 MiB). The writer closes its file before a document that would not fit beside it, so the
     * shard commits.
     */
    @Test
    @Tag("heavy")
    @EnabledIfSystemProperty(named = "gefrierschrank.heavyTests", matches = "true")
    void consecutivePoorlyCompressibleDocumentsFitWhereEachFitsAlone(@TempDir Path tmp) throws Exception {
        for (String heap : new String[]{"600m", "704m"}) {
            String output = runChild(tmp.resolve(heap), heap, List.of(), 1, 1, 3, 3, 64, "printable");
            assertTrue(output.contains("committed 1 of 1"), heap + ": " + output);
        }
    }

    /**
     * Large documents right after many small ones: parquet-mr would check page sizes only hundreds of
     * rows later and compress them as one page, outside every document's reservation (OOM at 320 MiB
     * with the gate alone, and at 192-320 MiB on an earlier version).
     */
    @Test
    @Tag("heavy")
    @EnabledIfSystemProperty(named = "gefrierschrank.heavyTests", matches = "true")
    void aBurstOfLargeDocumentsAfterSmallOnesFits(@TempDir Path tmp) throws Exception {
        String output = runChild(tmp, "320m", List.of("-Dheavy.layout=tail"), 1, 1, 1006, 6, 16, "printable");
        assertTrue(output.contains("committed 1 of 1"), output);
    }

    /**
     * One shard of four 64 MiB documents read as several ranges, at heaps where the first version of
     * the gate let ranges wait with their documents uncounted and ran out of memory (592-624 MiB):
     * the waiting documents are counted, and what does not fit beside the other ranges is read
     * again as one range.
     */
    @Test
    @Tag("heavy")
    @EnabledIfSystemProperty(named = "gefrierschrank.heavyTests", matches = "true")
    void largeDocumentsInSeveralRangesFitWhereOneRangeFits(@TempDir Path tmp) throws Exception {
        for (String heap : new String[]{"592m", "608m", "624m"}) {
            String output = runChild(tmp.resolve(heap), heap, List.of("-Dgefrierschrank.shard.threads=4",
                "-D" + ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY + "=1"), 1, 1, 4, 4, 64, "letters");
            assertTrue(output.contains("committed 1 of 1"), heap + ": " + output);
        }
    }

    /** Runs {@link HeavyChild} at {@code heap}; an OutOfMemoryError ends it with exit code 3. */
    private static String runChild(Path tmp, String heap, List<String> properties, int shards, int workers, int docs,
                                   int big, int mebibytes, String content) throws Exception {
        Files.createDirectories(tmp);
        Path table = tmp.resolve("table");
        Path log = tmp.resolve("child.log");
        // This module's test classpath binds slf4j to reload4j without a configuration, i.e. at DEBUG,
        // where parquet renders every written value as text; the CLI ships a WARN configuration.
        Path logging = Files.writeString(tmp.resolve("log4j.properties"), "log4j.rootLogger=WARN\n");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        var command = new ArrayList<>(List.of(java, "-Xmx" + heap, "-XX:+ExitOnOutOfMemoryError",
            "-Dlog4j.configuration=" + logging.toUri()));
        command.addAll(properties);
        command.addAll(List.of("-cp", System.getProperty("java.class.path"), HeavyChild.class.getName(), table.toString(),
            String.valueOf(shards), String.valueOf(workers), String.valueOf(docs), String.valueOf(big),
            String.valueOf(mebibytes), content));
        Process child = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        assertTrue(child.waitFor(10, TimeUnit.MINUTES), "child timed out");
        var output = Files.readString(log);
        assertNotEquals(3, child.exitValue(), "OutOfMemoryError at " + heap + ": " + output);
        assertEquals(0, child.exitValue(), output);
        return output;
    }

    /**
     * {@code shards} virtual shards at {@code workers} at once, each with {@code docs} documents of
     * which {@code big}, spread evenly (or the last ones with {@code -Dheavy.layout=tail}), have
     * {@code mebibytes} MiB: a repeating pattern, random letters (about 60% after ZSTD) or random
     * printable ASCII (about 82%). The others have 500 bytes.
     */
    static byte[] heavySource(int shard, int doc, int bytes, String content) {
        byte[] source = source(shard, doc, bytes);
        if (!content.equals("pattern")) {
            var random = new java.util.SplittableRandom(shard * 1_000_003L + doc);
            for (int i = source.length - 3; i > 0 && source[i] != '"'; i--) {
                int c = content.equals("letters") ? 'a' + random.nextInt(26) : 0x23 + random.nextInt(0x7E - 0x23);
                source[i] = (byte) (c == '\\' ? 'x' : c);
            }
        }
        return source;
    }

    public static final class HeavyChild {
        public static void main(String[] args) throws Exception {
            String table = args[0];
            int shards = Integer.parseInt(args[1]), workers = Integer.parseInt(args[2]), docs = Integer.parseInt(args[3]);
            int big = Integer.parseInt(args[4]), bigBytes = (int) (Integer.parseInt(args[5]) * MIB);
            String content = args[6];
            int step = docs / big;
            boolean tail = System.getProperty("heavy.layout", "spread").equals("tail");
            IntFunction<Integer> sizes = doc -> (tail ? doc >= docs - big : doc % step == 0 && doc / step < big) ? bigBytes : 500;
            String luceneIndex = System.getProperty("heavy.luceneIndex");
            var opener = luceneIndex != null
                ? (DumpToDeltaPipeline.ShardReaderOpener) (shard, dir) ->
                    new de.sloow.gefrierschrank.lucene10.Lucene10ShardReader(Path.of(luceneIndex))
                : (DumpToDeltaPipeline.ShardReaderOpener) (shard, dir) -> new ShardReader() {
                    final int id = shardIdOf(shard);
                    @Override public String runtimeName() { return "heavy"; }
                    @Override public int docCount() { return docs; }
                    @Override public ShardReader fork() { return this; }
                    @Override public void close() {}
                    @Override public DocumentMetadata documentMetadata(int docId) {
                        return new DocumentMetadata("id-" + docId, null, "stored", docId);
                    }
                    @Override public void readSource(int docId, SourceConsumer consumer) {
                        byte[] bytes = heavySource(id, docId, sizes.apply(docId), content);
                        consumer.accept(bytes, bytes.length);
                    }
                };
            var store = new FileSystemBlobStore(FIXTURE_REPO);
            var pipeline = new DumpToDeltaPipeline(new RepositoryInspector(store), store, new DeltaTableWriter(table),
                new Configuration(), opener);
            if (workers > 1) pipeline = pipeline.withWorkPool(new ShardWorkPool(workers, 0));
            var report = pipeline.runSnapshot(ARCHIVE, null, catalog(shards), RunReportListener.NO_OP);
            System.out.println("committed " + report.committedCount() + " of " + report.totalShards()
                + " " + report.shards());
            var sources = new ArrayList<Integer>();
            try (var rows = io.delta.standalone.DeltaLog.forTable(new Configuration(), table).update().open()) {
                while (rows.hasNext()) sources.add(rows.next().getString("_source").length());
            }
            if (report.committedCount() > 0 && !sources.contains(bigBytes)) throw new AssertionError("large rows missing: " + sources);
            System.exit(0);
        }
    }
}
