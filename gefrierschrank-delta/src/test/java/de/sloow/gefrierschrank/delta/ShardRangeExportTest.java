package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A shard read as K contiguous doc-id ranges exports exactly the rows and columns of K = 1:
 * over LZ4, DEFLATE and ZSTD (240 KB blocks) stored fields with several segments, hard deletes,
 * soft deletes, nested documents and a malformed-UTF-8 source, no doc id is duplicated or lost at
 * a cut, rows ascend by {@code _doc} within every file, the AddFiles are in range order, and the
 * shard is still one commit with one SetTransaction.
 */
class ShardRangeExportTest {

    private static final Path FIXTURE_REPO = Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");
    private static final String INDEX_NAME = "test_data_7_17_9";
    private static final ArchiveContext ARCHIVE =
        new ArchiveContext("ranges", "repo-index-id", "snap-uuid", "snap", "ranges", "snap-uuid");
    private static final ObjectMapper JSON = new ObjectMapper();

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

    record Case(String codec, int threads) {
        @Override public String toString() { return codec + " K=" + threads; }
    }

    static Stream<Case> cases() {
        return Stream.of("lz4", "deflate", "zstd").flatMap(codec -> Stream.of(2, 3, 8).map(k -> new Case(codec, k)));
    }

    static Codec codec(String name) {
        return switch (name) {
            case "lz4" -> new Lucene103Codec(Lucene103Codec.Mode.BEST_SPEED);
            case "deflate" -> new Lucene103Codec(Lucene103Codec.Mode.BEST_COMPRESSION);
            case "zstd" -> new de.sloow.gefrierschrank.lucene10.RangeTestZstdCodec();
            default -> throw new IllegalArgumentException(name);
        };
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void rangesExportTheRowsOfOneRange(Case c, @TempDir Path tmp) throws Exception {
        Path shard = tmp.resolve("shard");
        buildShard(shard, codec(c.codec()), 7);
        var serial = export(shard, tmp.resolve("serial"), 1);
        var ranged = export(shard, tmp.resolve("ranged"), c.threads());

        assertEquals(1, serial.files().size(), "one range, one file");
        assertEquals(sorted(serial.rows()), sorted(ranged.rows()), "the same rows and columns");
        assertEquals(serial.rows().size(), ranged.rows().size(), "no duplicate row");
        var docs = new ArrayList<Long>();
        for (var file : ranged.docsPerFile()) {
            for (int i = 1; i < file.size(); i++) assertTrue(file.get(i - 1) < file.get(i), "ascending _doc within a file");
            if (!docs.isEmpty() && !file.isEmpty()) assertTrue(docs.get(docs.size() - 1) < file.get(0), "AddFiles in range order");
            docs.addAll(file);
        }
        assertEquals(serial.docsPerFile().get(0), docs, "every _doc exactly once, in the order of K = 1");
        assertEquals(c.threads(), ranged.files().size(), "one file per range");
        assertEquals(1, ranged.commits(), "one commit for the shard");
        assertEquals(List.of(ARCHIVE.key(0)), ranged.transactions(), "one SetTransaction");
        assertEquals(Set.copyOf(ranged.files()), ParquetFooters.parquetFilesOnDisk(tmp.resolve("ranged")));
        assertEquals(1, ranged.report().committedCount());
        assertEquals(serial.report().rowsWrittenThisRun(), ranged.report().rowsWrittenThisRun());
    }

    /** Real ES 7, 8 and 9 shards of the demo repository. */
    @Test
    void fixtureShardsExportTheSameRowsInRanges(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        for (String index : List.of("test_data_7_17_9", "test_data_8_17_0", "test_data_9_3_0")) {
            Path shard = tmp.resolve(index);
            new de.sloow.gefrierschrank.core.repo.ShardMaterializer(store).materialize(inspector.shardSnapshot(index, 0), shard);
            var serial = export(shard, tmp.resolve(index + "-1"), 1);
            for (int k : new int[]{3, 8}) {
                var ranged = export(shard, tmp.resolve(index + "-" + k), k);
                assertEquals(sorted(serial.rows()), sorted(ranged.rows()), index + " at K=" + k);
                assertEquals(k, ranged.files().size());
                assertEquals(1, ranged.commits());
            }
        }
    }

    /** An explicit 1 takes the serial path: no plan, no fork, no thread, one Parquet file (the default is auto). */
    @Test
    void oneThreadNeverForks(@TempDir Path tmp) throws Exception {
        Path shard = tmp.resolve("shard");
        buildShard(shard, codec("lz4"), 3);
        var forks = new java.util.concurrent.atomic.AtomicInteger();
        var liveChecksBeforeFirstRead = new java.util.concurrent.atomic.AtomicInteger(-1);
        var readingThreads = java.util.Collections.synchronizedSet(new java.util.HashSet<Thread>());
        DumpToDeltaPipeline.ShardReaderOpener opener = (s, dir) -> new CountingReader(new Lucene10ShardReader(shard), forks,
            liveChecksBeforeFirstRead) {
            @Override public void readSource(int docId, SourceConsumer consumer) throws IOException {
                readingThreads.add(Thread.currentThread());
                super.readSource(docId, consumer);
            }
        };
        // The default, not a -Dgefrierschrank.shard.threads that a forced-K sweep passes to the whole module.
        String ambient = System.clearProperty(ShardRanges.SHARD_THREADS_KEY);
        try {
            for (String threads : new String[]{"1"}) {
                var conf = new Configuration();
                conf.set(ShardRanges.SHARD_THREADS_KEY, threads);
                var report = pipeline(tmp.resolve("t" + threads).toString(), conf, opener)
                    .runSnapshot(ARCHIVE, null, catalog(), RunReportListener.NO_OP);
                assertEquals(1, report.committedCount());
                assertEquals(0, forks.get());
                assertEquals(1, ParquetFooters.addFilePaths(conf, tmp.resolve("t" + threads).toString()).size());
            }
        } finally {
            if (ambient != null) System.setProperty(ShardRanges.SHARD_THREADS_KEY, ambient);
        }
        assertTrue(liveChecksBeforeFirstRead.get() < 10, "no live-count pass before the first document");
        assertEquals(Set.of(Thread.currentThread()), readingThreads, "read on the calling thread, no range thread");
    }

    /** A reader that cannot fork is read as one range, without the live-count pass. */
    @Test
    void aReaderThatCannotForkIsReadAsOneRangeWithoutPlanning(@TempDir Path tmp) throws Exception {
        Path shard = tmp.resolve("shard");
        buildShard(shard, codec("lz4"), 3);
        var forks = new java.util.concurrent.atomic.AtomicInteger();
        var liveChecksBeforeFirstRead = new java.util.concurrent.atomic.AtomicInteger(-1);
        DumpToDeltaPipeline.ShardReaderOpener opener = (s, dir) -> new CountingReader(new Lucene10ShardReader(shard), forks,
            liveChecksBeforeFirstRead) {
            @Override public ShardReader fork() {
                forks.incrementAndGet();
                return null;
            }
        };
        var conf = new Configuration();
        conf.set(ShardRanges.SHARD_THREADS_KEY, "4");
        var report = pipeline(tmp.resolve("table").toString(), conf, opener).runSnapshot(ARCHIVE, null, catalog(), RunReportListener.NO_OP);
        assertEquals(1, report.committedCount());
        assertEquals(1, forks.get(), "asked once");
        assertTrue(liveChecksBeforeFirstRead.get() < 10, "no live-count pass before the first document");
        assertEquals(1, ParquetFooters.addFilePaths(conf, tmp.resolve("table").toString()).size());
    }

    @Test
    void anInvalidSettingFailsTheIndexOnce(@TempDir Path tmp) {
        var conf = new Configuration();
        conf.set(ShardRanges.SHARD_THREADS_KEY, "65");
        DumpToDeltaPipeline.ShardReaderOpener opener = (s, dir) -> fail("no shard may start");
        var failure = assertThrows(IllegalArgumentException.class, () -> pipeline(tmp.toString(), conf, opener)
            .runSnapshot(ARCHIVE, null, catalog(), RunReportListener.NO_OP));
        assertEquals("gefrierschrank.shard.threads must be auto or between 1 and 64", failure.getMessage());
    }

    record Export(RunReport report, List<String> rows, List<String> files, List<List<Long>> docsPerFile, int commits,
                  List<String> transactions) {}

    static Export export(Path shardDir, Path table, int threads) throws Exception {
        var conf = new Configuration();
        conf.set(ShardRanges.SHARD_THREADS_KEY, Integer.toString(threads));
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> new Lucene10ShardReader(shardDir);
        RunReport report = pipeline(table.toString(), conf, opener).runSnapshot(ARCHIVE, null, catalog(), RunReportListener.NO_OP);
        return read(report, conf, table);
    }

    /** Rows of every file, and the files in the order of their commit's add actions. */
    static Export read(RunReport report, Configuration conf, Path table) throws Exception {
        List<String> files = new ArrayList<>();
        List<String> transactions = new ArrayList<>();
        int commits = 0;
        try (var logs = Files.list(table.resolve("_delta_log"))) {
            for (Path log : logs.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().toList()) {
                commits++;
                for (String line : Files.readAllLines(log)) {
                    var action = JSON.readTree(line);
                    if (action.has("add")) files.add(action.get("add").get("path").asText());
                    if (action.has("txn")) transactions.add(action.get("txn").get("appId").asText());
                }
            }
        }
        List<String> rows = new ArrayList<>();
        List<List<Long>> docsPerFile = new ArrayList<>();
        for (String file : files) {
            var docs = new ArrayList<Long>();
            var path = new org.apache.hadoop.fs.Path(table + "/" + file);
            try (var reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(path, conf)).withConf(conf).build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    docs.add((Long) record.get("_doc"));
                    var row = new StringBuilder();
                    for (var field : record.getSchema().getFields()) {
                        Object value = record.get(field.name());
                        row.append(field.name()).append('=');
                        if (value instanceof org.apache.avro.util.Utf8 text)
                            row.append(java.util.HexFormat.of().formatHex(text.getBytes(), 0, text.getByteLength()));
                        else row.append(value);
                        row.append(';');
                    }
                    rows.add(row.toString());
                }
            }
            docsPerFile.add(docs);
        }
        return new Export(report, rows, files, docsPerFile, commits, transactions);
    }

    static List<String> sorted(List<String> rows) {
        return rows.stream().sorted().toList();
    }

    static DumpToDeltaPipeline pipeline(String table, Configuration conf, DumpToDeltaPipeline.ShardReaderOpener opener) {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        return new DumpToDeltaPipeline(new RepositoryInspector(store), store, new DeltaTableWriter(table), conf, opener);
    }

    static DumpToDeltaPipeline.ShardCatalog catalog() throws IOException {
        ShardSnapshot template = new RepositoryInspector(new FileSystemBlobStore(FIXTURE_REPO)).shardSnapshot(INDEX_NAME, 0);
        return new DumpToDeltaPipeline.ShardCatalog() {
            @Override public int shardCount() { return 1; }
            @Override public ShardSnapshot shardSnapshot(int shardId) { return template; }
        };
    }

    /**
     * Three segments of 700 blocks each: every tenth block a root with two nested children, hard
     * deletes, soft updates, ~1 KB sources and one with malformed UTF-8.
     */
    static void buildShard(Path dir, Codec codec, long seed) throws IOException {
        var config = new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE)
            .setSoftDeletesField("__soft_deletes");
        var random = new Random(seed);
        try (var directory = new NIOFSDirectory(dir); var writer = new IndexWriter(directory, config)) {
            int n = 0;
            for (int segment = 0; segment < 3; segment++) {
                for (int i = 0; i < 700; i++, n++) {
                    var block = new ArrayList<Document>();
                    if (n % 10 == 0) {
                        for (int child = 0; child < 2; child++) {
                            var nested = new Document();
                            nested.add(new StringField("_nested_path", "items", Field.Store.NO));
                            nested.add(new StoredField("_source", new BytesRef("{\"child\":" + child + "}")));
                            block.add(nested);
                        }
                    }
                    block.add(root(n, random, 0));
                    writer.addDocuments(block);
                }
                writer.commit();
            }
            for (int deleted : new int[]{5, 701, 1402, 1403, 2099}) writer.deleteDocuments(new Term("key", "doc-" + deleted));
            for (int updated : new int[]{9, 703, 1500}) {
                writer.softUpdateDocument(new Term("key", "doc-" + updated), root(updated, random, 1),
                    new NumericDocValuesField("__soft_deletes", 1));
            }
            writer.commit();
        }
    }

    private static Document root(int n, Random random, int version) {
        var doc = new Document();
        doc.add(new StringField("key", "doc-" + n, Field.Store.NO));
        doc.add(new NumericDocValuesField("_primary_term", 1));
        byte[] id = ("id-" + n).getBytes(StandardCharsets.UTF_8);
        byte[] encoded = new byte[id.length + 1];
        encoded[0] = (byte) 255;
        System.arraycopy(id, 0, encoded, 1, id.length);
        doc.add(new StoredField("_id", new BytesRef(encoded)));
        if (n % 3 == 0) doc.add(new StoredField("_routing", "tenant-" + (n % 5)));
        byte[] source;
        if (n == 1234) {
            source = new byte[]{'{', '"', 'a', '"', ':', '"', (byte) 0xC3, '(', '"', '}'};
        } else {
            var json = new StringBuilder("{\"n\":").append(n).append(",\"v\":").append(version).append(",\"message\":\"");
            for (int w = 60 + random.nextInt(60); w > 0; w--) json.append((char) ('a' + random.nextInt(26))).append("x ");
            source = json.append("é\"}").toString().getBytes(StandardCharsets.UTF_8);
        }
        doc.add(new StoredField("_source", new BytesRef(source)));
        return doc;
    }

    /** Counts forks and how many liveness checks precede the first source read. */
    static class CountingReader implements ShardReader {
        private final ShardReader delegate;
        private final java.util.concurrent.atomic.AtomicInteger forks;
        private final java.util.concurrent.atomic.AtomicInteger liveChecksBeforeFirstRead;
        private int liveChecks;

        CountingReader(ShardReader delegate, java.util.concurrent.atomic.AtomicInteger forks,
                       java.util.concurrent.atomic.AtomicInteger liveChecksBeforeFirstRead) {
            this.delegate = delegate;
            this.forks = forks;
            this.liveChecksBeforeFirstRead = liveChecksBeforeFirstRead;
        }

        @Override public String runtimeName() { return delegate.runtimeName(); }
        @Override public int docCount() throws IOException { return delegate.docCount(); }
        @Override public boolean isLive(int docId) throws IOException { liveChecks++; return delegate.isLive(docId); }
        @Override public de.sloow.gefrierschrank.core.lucene.DocumentMetadata documentMetadata(int docId) throws IOException {
            return delegate.documentMetadata(docId);
        }

        @Override public void readSource(int docId, SourceConsumer consumer) throws IOException {
            if (liveChecksBeforeFirstRead.get() < 0) liveChecksBeforeFirstRead.set(liveChecks);
            delegate.readSource(docId, consumer);
        }

        @Override public ShardReader fork() throws IOException {
            forks.incrementAndGet();
            return delegate.fork();
        }

        @Override public void close() throws IOException { delegate.close(); }
    }
}
