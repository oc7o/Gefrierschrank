package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker;
import de.sloow.gefrierschrank.syntheticsource.SyntheticSourceReconstructor;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SyntheticSourceFallbackShardReader} reconstructs documents of source-less segments in
 * batches ahead of the caller. The rows must be exactly those of per-document reconstruction, a
 * failure must stay attributed to its own document with today's quarantine, and batch state must
 * never cross readers. Uses LZ4 fixtures: on this module's class path the ambient reader cannot
 * decode ZSTD stored fields (see {@link SyntheticArchiveGoldenTest}).
 */
class SyntheticBatchFallbackTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final String DELETES = "synth-deletes-repo/test_deletes_lz4";

    /** Four segments with hard and soft deletes: every live document, and each exactly as per document. */
    @Test
    void liveDocumentsOfAMultiSegmentShardWithDeletesMatchTheGolden(@TempDir Path tmp) throws Exception {
        Path shardDir = materialize(DELETES, tmp);
        List<String> golden = golden(DELETES);
        try (ShardReader reader = wrap(new Lucene10ShardReader(shardDir), shardDir, DELETES)) {
            int live = 0;
            for (int doc = 0; doc < reader.docCount(); doc++) {
                if (!reader.isLive(doc)) continue;
                live++;
                assertEquals(golden.get(doc), read(reader, doc), "doc " + doc);
            }
            assertEquals(108, live);
        }
    }

    /**
     * A batch that fails while visiting document 40 (only the batch path fails) leaves the
     * documents it was asked for to per-document reconstruction; the rows are unchanged, and closing
     * the reader warns once, with the failure's class and no document content. Batches are cut
     * short at 2000 bytes here, so several succeed before the failing one.
     */
    @Test
    void aFailedBatchReconstructsTheRequestedDocumentsOneAtATime(@TempDir Path tmp) throws Exception {
        Path shardDir = materialize(DELETES, tmp);
        List<String> golden = golden(DELETES);
        List<Integer> live = liveDocs(shardDir);
        var failing = new FailingWorkerSetup(live.get(40), false);
        var stderr = new ByteArrayOutputStream();
        PrintStream previousErr = System.err;
        System.setProperty(IsolatedReconstructionWorker.BATCH_BYTES_PROPERTY, "2000");
        try (ShardReader reader = wrap(new Lucene10ShardReader(shardDir), shardDir, DELETES)) {
            System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
            failing.install(reader, shardDir, DELETES);
            for (int doc : live) assertEquals(golden.get(doc), read(reader, doc), "doc " + doc);
        } finally {
            System.setErr(previousErr);
            System.clearProperty(IsolatedReconstructionWorker.BATCH_BYTES_PROPERTY);
        }
        assertEquals(1, failing.worker.failedBatches);
        assertTrue(failing.worker.batches > 2, "batches before the failure: " + failing.worker.batches);
        assertTrue(failing.worker.singleReads > 0, "the failed batch's documents went one at a time");
        assertEquals("warning: 1 reconstruction batch(es) of index test_deletes_lz4 failed (first: "
            + "InjectedReconstructionFailure); their documents were reconstructed one at a time\n",
            stderr.toString(StandardCharsets.UTF_8).replace(System.lineSeparator(), "\n"));
    }

    /**
     * Where the byte limit cuts batches short, the next batch asks for about as many ids as the last
     * one returned (at most twice as many), instead of loading doc values for 1024 ids each time.
     */
    @Test
    void byteLimitedBatchesAskForAboutWhatTheLimitLetsThrough(@TempDir Path tmp) throws Exception {
        Path shardDir = materialize(DELETES, tmp);
        List<String> golden = golden(DELETES);
        var counting = new FailingWorkerSetup(-1, false);
        System.setProperty(IsolatedReconstructionWorker.BATCH_BYTES_PROPERTY, "2000");
        try (ShardReader reader = wrap(new Lucene10ShardReader(shardDir), shardDir, DELETES)) {
            counting.install(reader, shardDir, DELETES);
            for (int doc : liveDocs(shardDir)) assertEquals(golden.get(doc), read(reader, doc), "doc " + doc);
        } finally {
            System.clearProperty(IsolatedReconstructionWorker.BATCH_BYTES_PROPERTY);
        }
        List<int[]> batches = counting.worker.requestedAndReturned;
        assertTrue(batches.size() > 2, "batches: " + batches.size());
        assertTrue(batches.get(0)[1] < batches.get(0)[0], "the first batch reaches the byte limit");
        for (int i = 1; i < batches.size(); i++) {
            int[] previous = batches.get(i - 1);
            if (previous[1] < previous[0]) {
                assertTrue(batches.get(i)[0] <= Math.max(2, previous[1]), "batch " + i + " after a short one");
            } else {
                assertTrue(batches.get(i)[0] <= 2 * previous[0], "batch " + i + " after a whole one");
            }
        }
    }

    /**
     * A document that cannot be reconstructed at all fails its shard exactly as without batches: the
     * root cause is its own exception, the shard is quarantined and no row is committed.
     */
    @Test
    void aPersistentFailureQuarantinesTheShardWithItsOwnRootCause(@TempDir Path tmp) throws Exception {
        String index = "test_deletes_lz4";
        var store = new FileSystemBlobStore(FIXTURES.resolve("synth-deletes-repo"));
        var inspector = new RepositoryInspector(store);
        ShardSnapshot shard = inspector.shardSnapshot(index, 0);
        DumpToDeltaPipeline.ShardCatalog catalog = new DumpToDeltaPipeline.ShardCatalog() {
            public int shardCount() { return 1; }
            public ShardSnapshot shardSnapshot(int shardId) { return shard; }
        };
        var failing = new FailingWorkerSetup(60, true);
        assertTrue(liveDocs(materialize(DELETES, tmp.resolve("probe"))).contains(60));
        // Null metadata keeps the pipeline from wrapping again: the opener returns the wrapped reader.
        DumpToDeltaPipeline.ShardReaderOpener opener = (snapshot, dir) -> {
            ShardReader reader = wrap(new Lucene10ShardReader(dir), dir, DELETES);
            failing.install(reader, dir, DELETES);
            return reader;
        };
        String table = tmp.resolve("table").toString();
        var context = new ArchiveContext(index, "rid", "snap", "snap", index, "snap");
        // The failing worker is injected into this reader only, so the shard must stay one range even
        // where intra-shard threads are forced (the configuration wins over the JVM property).
        Configuration conf = new Configuration();
        conf.set("gefrierschrank.shard.threads", "1");
        RunReport report = new DumpToDeltaPipeline(inspector, store, table, conf, opener)
            .runSnapshot(context, null, catalog, RunReportListener.NO_OP);

        assertEquals(1, report.quarantinedCount(), report.shards().toString());
        assertEquals("InjectedReconstructionFailure", report.shards().get(0).errorClass());
        assertEquals(0, report.rowsWrittenThisRun());
        assertTrue(failing.worker.singleReads > 0, "the failing document was reconstructed on its own");
        if (Files.exists(Path.of(table, "_delta_log"))) {
            try (var rows = io.delta.standalone.DeltaLog.forTable(new Configuration(), table).update().open()) {
                assertFalse(rows.hasNext(), "a quarantined shard leaves no rows");
            }
        }
    }

    /** A fork is a new reader over the same shard directory: it starts with no batch and no reconstructor. */
    @Test
    void aForkStartsWithAnEmptyBatch(@TempDir Path tmp) throws Exception {
        Path shardDir = materialize(DELETES, tmp);
        List<String> golden = golden(DELETES);
        JsonNode metadata = metadata(DELETES);
        Constructor<SyntheticSourceFallbackShardReader> forkConstructor = SyntheticSourceFallbackShardReader.class
            .getDeclaredConstructor(ShardReader.class, Path.class, String.class, JsonNode.class);
        forkConstructor.setAccessible(true);
        try (var parent = (SyntheticSourceFallbackShardReader) wrap(new Lucene10ShardReader(shardDir), shardDir, DELETES)) {
            assertEquals(golden.get(firstLive(parent, 0)), read(parent, firstLive(parent, 0)));
            assertTrue(((int[]) field(parent, "batchIds")).length > 1, "the parent holds a batch");
            try (var fork = forkConstructor.newInstance(new Lucene10ShardReader(shardDir), shardDir, "test_deletes_lz4", metadata)) {
                assertEquals(0, ((int[]) field(fork, "batchIds")).length);
                assertEquals(0, ((byte[][]) field(fork, "batchSources")).length);
                assertEquals(0, field(fork, "batchNext"));
                assertEquals(-1, field(fork, "perDocumentThrough"));
                assertEquals(SyntheticSourceFallbackShardReader.BATCH_DOCUMENTS, field(fork, "batchDocuments"));
                assertEquals(0, field(fork, "failedBatches"));
                assertNull(field(fork, "reconstructor"));
                int from = firstLive(fork, 70);
                for (int doc = from; doc < fork.docCount(); doc++) {
                    if (fork.isLive(doc)) assertEquals(golden.get(doc), read(fork, doc), "fork doc " + doc);
                }
            }
            int next = firstLive(parent, firstLive(parent, 0) + 1);
            assertEquals(golden.get(next), read(parent, next), "the parent's batch is untouched by the fork");
        }
    }

    /** Two readers over the same shard directory, each on its own thread and range, give the serial bytes. */
    @Test
    void independentReadersOnTwoThreadsMatchOneSerialReader(@TempDir Path tmp) throws Exception {
        Path shardDir = materialize(DELETES, tmp);
        List<String> serial = new ArrayList<>();
        List<Integer> live = new ArrayList<>();
        try (ShardReader reader = wrap(new Lucene10ShardReader(shardDir), shardDir, DELETES)) {
            for (int doc = 0; doc < reader.docCount(); doc++) {
                if (!reader.isLive(doc)) continue;
                live.add(doc);
                serial.add(read(reader, doc));
            }
        }
        int half = live.size() / 2;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<List<String>>> parts = new ArrayList<>();
            for (List<Integer> range : List.of(live.subList(0, half), live.subList(half, live.size()))) {
                parts.add(pool.submit(() -> {
                    List<String> out = new ArrayList<>();
                    try (ShardReader reader = wrap(new Lucene10ShardReader(shardDir), shardDir, DELETES)) {
                        for (int round = 0; round < 3; round++) {
                            out.clear();
                            for (int doc : range) out.add(read(reader, doc));
                        }
                    }
                    return out;
                }));
            }
            List<String> concurrent = new ArrayList<>(parts.get(0).get(120, TimeUnit.SECONDS));
            concurrent.addAll(parts.get(1).get(120, TimeUnit.SECONDS));
            assertEquals(serial, concurrent);
        } finally {
            pool.shutdownNow();
        }
    }

    /** Readers that cannot tell up front keep the per-document safety net: no batches. */
    @Test
    void theExceptionSafetyNetStaysPerDocument(@TempDir Path tmp) throws Exception {
        Path shardDir = materialize("synth-repo/test_plain_synth", tmp);
        ShardReader cannotTell = new ShardReader() {
            final Lucene10ShardReader delegate = new Lucene10ShardReader(shardDir);
            public String runtimeName() { return delegate.runtimeName(); }
            public int docCount() { return delegate.docCount(); }
            public boolean isLive(int docId) { return delegate.isLive(docId); }
            public void readSource(int docId, SourceConsumer consumer) throws IOException { delegate.readSource(docId, consumer); }
            public void close() throws IOException { delegate.close(); }
        };
        var counting = new FailingWorkerSetup(-1, false);
        try (ShardReader reader = wrap(cannotTell, shardDir, "synth-repo/test_plain_synth")) {
            counting.install(reader, shardDir, "synth-repo/test_plain_synth");
            assertEquals(golden("synth-repo/test_plain_synth").get(7), read(reader, 7));
            assertEquals(golden("synth-repo/test_plain_synth").get(8), read(reader, 8));
            assertEquals(0, counting.worker.batches);
            assertEquals(2, counting.worker.singleReads);
        }
    }

    /**
     * Replaces a fallback reader's reconstructor with one whose worker wraps the real one: it counts
     * calls and fails every batch that visits {@code failingDoc}; with {@code persistent} the
     * single-document read of that doc fails as well.
     */
    private static final class FailingWorkerSetup {
        private final int failingDoc;
        private final boolean persistent;
        CountingWorker worker;

        FailingWorkerSetup(int failingDoc, boolean persistent) {
            this.failingDoc = failingDoc;
            this.persistent = persistent;
        }

        void install(ShardReader fallback, Path shardDir, String fixtureIndex) throws Exception {
            String index = fixtureIndex.substring(fixtureIndex.indexOf('/') + 1);
            SyntheticSourceReconstructor real = SyntheticSourceReconstructor.open(shardDir, index, metadata(fixtureIndex));
            var realWorker = (IsolatedReconstructionWorker) field(real, "worker");
            worker = new CountingWorker(realWorker, failingDoc, persistent);
            Constructor<?> constructor = SyntheticSourceReconstructor.class.getDeclaredConstructors()[0];
            constructor.setAccessible(true);
            Object wrapped = constructor.newInstance(worker, field(real, "loader"));
            Field reconstructor = SyntheticSourceFallbackShardReader.class.getDeclaredField("reconstructor");
            reconstructor.setAccessible(true);
            reconstructor.set(fallback, wrapped);
        }
    }

    static final class InjectedReconstructionFailure extends IOException {
        InjectedReconstructionFailure() {
            super("injected reconstruction failure");
        }
    }

    private static final class CountingWorker implements IsolatedReconstructionWorker {
        private final IsolatedReconstructionWorker delegate;
        private final int failingDoc;
        private final boolean persistent;
        int batches;
        int failedBatches;
        int singleReads;
        final List<int[]> requestedAndReturned = new ArrayList<>();

        CountingWorker(IsolatedReconstructionWorker delegate, int failingDoc, boolean persistent) {
            this.delegate = delegate;
            this.failingDoc = failingDoc;
            this.persistent = persistent;
        }

        public int docCount() { return delegate.docCount(); }

        public byte[] readSource(int docId) throws IOException {
            singleReads++;
            if (persistent && docId == failingDoc) throw new InjectedReconstructionFailure();
            return delegate.readSource(docId);
        }

        /** Fails once the batch has visited {@code failingDoc}. */
        public byte[][] readSources(int[] ids) throws IOException {
            batches++;
            byte[][] sources = delegate.readSources(ids);
            requestedAndReturned.add(new int[]{ids.length, sources.length});
            for (int i = 0; i < sources.length; i++) {
                if (ids[i] == failingDoc) {
                    failedBatches++;
                    throw new InjectedReconstructionFailure();
                }
            }
            return sources;
        }

        public void close() throws IOException { delegate.close(); }
    }

    static ShardReader wrap(ShardReader delegate, Path shardDir, String fixtureIndex) throws IOException {
        return SyntheticSourceFallbackShardReader.wrap(delegate, shardDir,
            fixtureIndex.substring(fixtureIndex.indexOf('/') + 1), metadata(fixtureIndex));
    }

    private static JsonNode metadata(String fixtureIndex) throws IOException {
        String repo = fixtureIndex.substring(0, fixtureIndex.indexOf('/'));
        return new RepositoryInspector(new FileSystemBlobStore(FIXTURES.resolve(repo)))
            .indexMetadata(fixtureIndex.substring(fixtureIndex.indexOf('/') + 1));
    }

    private static Path materialize(String fixtureIndex, Path tmp) throws IOException {
        String repo = fixtureIndex.substring(0, fixtureIndex.indexOf('/'));
        var store = new FileSystemBlobStore(FIXTURES.resolve(repo));
        Path shardDir = Files.createDirectories(tmp).resolve("shard");
        new ShardMaterializer(store).materialize(
            new RepositoryInspector(store).shardSnapshot(fixtureIndex.substring(fixtureIndex.indexOf('/') + 1), 0), shardDir);
        return shardDir;
    }

    private static List<String> golden(String fixtureIndex) throws IOException {
        List<String> docs = new ArrayList<>();
        JSON.readTree(FIXTURES.resolve("synth-reconstructed-golden.json").toFile())
            .path("reconstructed").path(fixtureIndex).forEach(doc -> docs.add(doc.asText()));
        return docs;
    }

    private static String read(ShardReader reader, int doc) throws IOException {
        String[] holder = new String[1];
        reader.readSource(doc, (bytes, length) -> holder[0] = new String(bytes, 0, length, StandardCharsets.UTF_8));
        return holder[0];
    }

    private static List<Integer> liveDocs(Path shardDir) throws IOException {
        List<Integer> live = new ArrayList<>();
        try (var reader = new Lucene10ShardReader(shardDir)) {
            for (int doc = 0; doc < reader.docCount(); doc++) if (reader.isLive(doc)) live.add(doc);
        }
        return live;
    }

    private static int firstLive(ShardReader reader, int from) throws IOException {
        int doc = from;
        while (!reader.isLive(doc)) doc++;
        return doc;
    }

    private static Object field(Object target, String name) throws ReflectiveOperationException {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
