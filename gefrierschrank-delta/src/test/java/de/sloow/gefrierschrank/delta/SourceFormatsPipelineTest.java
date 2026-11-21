package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * dump-to-delta over a Lucene 10 shard whose stored {@code _source} mixes JSON, SMILE, CBOR and
 * YAML: converted rows, their kinds, the per-shard note with counts only, the memory gate on the
 * converted length, quarantine of undetectable and corrupt documents, resume, and ranges.
 */
@Timeout(300)
class SourceFormatsPipelineTest {

    private static final String INDEX = "sf_mixed";
    private static final ArchiveContext ARCHIVE =
        new ArchiveContext(INDEX, "repo-index-id", "snap-uuid", "snap", INDEX, "snap-uuid");
    private static final String MARKER = "SECRET-DOCUMENT-TEXT";
    private static final long MIB = 1L << 20;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonFactory SHARED_STRINGS_SMILE =
        SmileFactory.builder().enable(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES).build();

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

    /** One stored document: the bytes Lucene keeps and the row the export must produce. */
    record Stored(byte[] bytes, String kind, String row) {}

    private static String compact(int n) {
        return "{\"n\":" + n + ",\"s\":\"" + MARKER + "-" + n + "\",\"f\":1.5,\"list\":[true,null,\"x\"]}";
    }

    private static Stored stored(int n) throws IOException {
        var tree = JSON.readTree(compact(n));
        return switch (n % 4) {
            case 0 -> {
                // JSON keeps its whitespace: the row is the stored bytes.
                String text = "{ \"n\": " + n + ", \"s\": \"" + MARKER + "-" + n + "\", \"f\": 1.5, \"list\": [true, null, \"x\"] }";
                yield new Stored(text.getBytes(StandardCharsets.UTF_8), "stored", text);
            }
            case 1 -> new Stored(new ObjectMapper(SmileFactory.builder().build()).writeValueAsBytes(tree), "stored-smile", compact(n));
            case 2 -> new Stored(new ObjectMapper(CBORFactory.builder().build()).writeValueAsBytes(tree), "stored-cbor", compact(n));
            default -> new Stored(("---\nn: " + n + "\ns: " + MARKER + "-" + n + "\nf: 1.5\nlist: [true, ~, x]\n")
                .getBytes(StandardCharsets.UTF_8), "stored-yaml", compact(n));
        };
    }

    private static List<Stored> mixed(int docs) throws IOException {
        var stored = new ArrayList<Stored>();
        for (int n = 0; n < docs; n++) stored.add(stored(n));
        return stored;
    }

    /** One segment, documents in order, ids doc-0, doc-1, ... */
    private static Path shard(Path dir, List<Stored> docs) throws IOException {
        var config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setUseCompoundFile(false);
        try (var directory = new NIOFSDirectory(dir); var writer = new IndexWriter(directory, config)) {
            for (int n = 0; n < docs.size(); n++) {
                var doc = new Document();
                doc.add(new StoredField("_id", "doc-" + n));
                doc.add(new NumericDocValuesField("_primary_term", 1));
                doc.add(new StoredField("_source", docs.get(n).bytes()));
                writer.addDocument(doc);
            }
            writer.commit();
        }
        return dir;
    }

    private static byte[] smile(JsonFactory factory, Body body) throws IOException {
        var out = new ByteArrayOutputStream();
        try (var generator = factory.createGenerator(out)) {
            body.write(generator);
        }
        return out.toByteArray();
    }

    interface Body {
        void write(JsonGenerator generator) throws IOException;
    }

    /** A SMILE document of {@code copies} back-references to one 60-character value: small stored, large converted. */
    private static Stored expanding(int copies) throws IOException {
        String value = "v".repeat(59) + "!";
        byte[] bytes = smile(SHARED_STRINGS_SMILE, g -> {
            g.writeStartObject();
            g.writeArrayFieldStart("a");
            for (int i = 0; i < copies; i++) g.writeString(value);
            g.writeEndArray();
            g.writeEndObject();
        });
        var row = new StringBuilder(copies * 63 + 8).append("{\"a\":[");
        for (int i = 0; i < copies; i++) row.append(i == 0 ? "" : ",").append('"').append(value).append('"');
        return new Stored(bytes, "stored-smile", row.append("]}").toString());
    }

    record Run(RunReport report, String stderr) {}

    private static Run run(Path table, Path shardDir, int threads, DocumentMemoryGate gate) throws Exception {
        var conf = new Configuration();
        conf.set(ShardRanges.SHARD_THREADS_KEY, Integer.toString(threads));
        var pipeline = ShardRangeExportTest.pipeline(table.toString(), conf, (s, d) -> new Lucene10ShardReader(shardDir));
        if (gate != null) pipeline.withDocumentMemoryGate(gate);
        var err = new ByteArrayOutputStream();
        var stderr = System.err;
        System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
        try {
            return new Run(pipeline.runSnapshot(ARCHIVE, null, ShardRangeExportTest.catalog(), RunReportListener.NO_OP),
                err.toString(StandardCharsets.UTF_8));
        } finally {
            System.setErr(stderr);
        }
    }

    record Row(String id, String kind, String source) {}

    private static Map<Long, Row> rows(Path table) {
        Map<Long, Row> rows = new TreeMap<>();
        try (var read = io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).update().open()) {
            while (read.hasNext()) {
                var row = read.next();
                assertNull(rows.put(row.getLong("_doc"),
                    new Row(row.getString("_id"), row.getString("_source_kind"), row.getString("_source"))), "one row per document");
            }
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return rows;
    }

    private static String note(long smile, long cbor, long yaml) {
        return "note: shard 0 of " + INDEX + ": " + smile + " SMILE, " + cbor + " CBOR, " + yaml
            + " YAML documents converted to JSON (not byte-exact; _source_kind stored-smile/stored-cbor/stored-yaml)";
    }

    private static List<String> noteLines(String stderr) {
        return stderr.lines().filter(line -> line.contains("converted to JSON")).toList();
    }

    private static void assertRows(List<Stored> docs, Map<Long, Row> rows) {
        assertEquals(docs.size(), rows.size());
        var validator = new SourceValidator();
        for (int n = 0; n < docs.size(); n++) {
            var row = rows.get((long) n);
            assertEquals("doc-" + n, row.id());
            assertEquals(docs.get(n).kind(), row.kind(), "kind of doc " + n);
            assertEquals(docs.get(n).row(), row.source(), "source of doc " + n);
            byte[] bytes = row.source().getBytes(StandardCharsets.UTF_8);
            assertTrue(validator.accepts(bytes, bytes.length), "the byte path accepts row " + n);
        }
    }

    @Test
    void aMixedShardCommitsConvertedRowsWithACountOnlyNote(@TempDir Path tmp) throws Exception {
        var docs = mixed(40);
        var nonFinite = smile(SmileFactory.builder().build(), g -> {
            g.writeStartObject();
            g.writeObjectFieldStart("raw");
            g.writeNumberField("nan", Double.NaN);
            g.writeNumberField("inf", Float.POSITIVE_INFINITY);
            g.writeNumberField("ninf", Double.NEGATIVE_INFINITY);
            g.writeEndObject();
            g.writeEndObject();
        });
        docs.add(new Stored(nonFinite, "stored-smile", "{\"raw\":{\"nan\":\"NaN\",\"inf\":\"Infinity\",\"ninf\":\"-Infinity\"}}"));
        Path table = tmp.resolve("table");
        var run = run(table, shard(tmp.resolve("shard"), docs), 1, null);

        assertEquals(RunReport.Outcome.COMMITTED, run.report().shards().get(0).outcome(), String.valueOf(run.report().shards()));
        assertEquals(41, run.report().shards().get(0).rowsWritten());
        assertRows(docs, rows(table));
        assertEquals(List.of(note(11, 10, 10)), noteLines(run.stderr()));
        assertFalse(run.stderr().contains(MARKER), "counts only: " + run.stderr());
        assertFalse(run.stderr().contains("validated one by one"), "converted documents take the byte path");
    }

    @Test
    void aJsonOnlyShardPrintsNoNote(@TempDir Path tmp) throws Exception {
        var docs = new ArrayList<Stored>();
        for (int n = 0; n < 12; n += 4) docs.add(stored(n));
        Path table = tmp.resolve("table");
        var run = run(table, shard(tmp.resolve("shard"), docs), 1, null);
        assertEquals(RunReport.Outcome.COMMITTED, run.report().shards().get(0).outcome());
        assertEquals(List.of(), noteLines(run.stderr()));
        assertRows(docs, rows(table));
    }

    @Test
    void rangesExportTheSameRowsAndCountEachDocumentOnce(@TempDir Path tmp) throws Exception {
        var docs = mixed(41);
        Path shardDir = shard(tmp.resolve("shard"), docs);
        var one = run(tmp.resolve("one"), shardDir, 1, null);
        var four = run(tmp.resolve("four"), shardDir, 4, null);
        assertEquals(RunReport.Outcome.COMMITTED, four.report().shards().get(0).outcome());
        var conf = new Configuration();
        var serial = ShardRangeExportTest.read(one.report(), conf, tmp.resolve("one"));
        var ranged = ShardRangeExportTest.read(four.report(), conf, tmp.resolve("four"));
        assertEquals(4, ranged.files().size(), "four ranges, four files");
        assertEquals(ShardRangeExportTest.sorted(serial.rows()), ShardRangeExportTest.sorted(ranged.rows()));
        assertEquals(List.of(note(10, 10, 10)), noteLines(one.stderr()));
        assertEquals(List.of(note(10, 10, 10)), noteLines(four.stderr()));
    }

    @Test
    void undetectableContentStillFailsTheShard(@TempDir Path tmp) throws Exception {
        var docs = mixed(4);
        docs.add(new Stored(new byte[] {0x00, 0x01, 0x02, (byte) 0xD9}, "stored", null));
        Path table = tmp.resolve("table");
        var run = run(table, shard(tmp.resolve("shard"), docs), 1, null);
        var shard = run.report().shards().get(0);
        assertEquals(RunReport.Outcome.QUARANTINED, shard.outcome());
        assertEquals("JsonParseException", shard.errorClass(), "the legacy JSON path decides, as in 1.1.0");
        assertFalse(Files.exists(table.resolve("_delta_log")));
        assertEquals(Set.of(), ParquetFooters.parquetFilesOnDisk(table));
    }

    @Test
    void aCorruptSmileDocumentQuarantinesAndARerunCommitsOnce(@TempDir Path tmp) throws Exception {
        var docs = mixed(8);
        byte[] smile = docs.get(5).bytes();
        var corrupt = new ArrayList<>(docs);
        corrupt.set(5, new Stored(Arrays.copyOf(smile, smile.length - 4), "stored-smile", null));
        Path table = tmp.resolve("table");
        var failed = run(table, shard(tmp.resolve("corrupt"), corrupt), 1, null);
        var shard = failed.report().shards().get(0);
        assertEquals(RunReport.Outcome.QUARANTINED, shard.outcome());
        assertEquals("SourceConversionException", shard.errorClass());
        assertTrue(shard.errorMessage().contains("stored _source in SMILE format could not be converted to JSON"),
            shard.errorMessage());
        assertFalse(shard.errorMessage().contains(MARKER));
        assertFalse(failed.stderr().contains(MARKER));
        assertFalse(Files.exists(table.resolve("_delta_log")), "nothing committed");
        assertEquals(Set.of(), ParquetFooters.parquetFilesOnDisk(table), "staged files deleted");

        Path good = shard(tmp.resolve("good"), docs);
        var rerun = run(table, good, 1, null);
        assertEquals(RunReport.Outcome.COMMITTED, rerun.report().shards().get(0).outcome());
        var again = run(table, good, 1, null);
        assertEquals(RunReport.Outcome.SKIPPED_ALREADY_COMMITTED, again.report().shards().get(0).outcome());
        assertRows(docs, rows(table));
    }

    @Test
    void theGateAdmitsTheConvertedLength(@TempDir Path tmp) throws Exception {
        var docs = mixed(3);
        var large = expanding(25_000);
        assertTrue(large.bytes().length < DocumentMemoryGate.BYPASS_BYTES / 4, "stored size " + large.bytes().length);
        assertTrue(large.row().length() > DocumentMemoryGate.BYPASS_BYTES, "converted size " + large.row().length());
        docs.add(large);
        var gate = new DocumentMemoryGate(512 * MIB, 8 * MIB, 6.0);
        Path table = tmp.resolve("table");
        var run = run(table, shard(tmp.resolve("shard"), docs), 1, gate);
        assertEquals(RunReport.Outcome.COMMITTED, run.report().shards().get(0).outcome(), String.valueOf(run.report().shards()));
        assertEquals(1, gate.admittedDocuments(), "admitted at its converted length, above the bypass size");
        assertEquals(0, gate.reservedKiB());
        assertRows(docs, rows(table));
        assertEquals(List.of(note(2, 1, 0)), noteLines(run.stderr()));
    }

    @Test
    void aConvertedDocumentTooLargeForTheBudgetQuarantines(@TempDir Path tmp) throws Exception {
        var docs = mixed(3);
        docs.add(expanding(70_000));
        // Budget 32 - 2 x 8 = 16 MiB for one range; about 4.2 MB converted x 6 needs 25.
        var gate = new DocumentMemoryGate(32 * MIB, 8 * MIB, 6.0);
        Path table = tmp.resolve("table");
        var run = run(table, shard(tmp.resolve("shard"), docs), 1, gate);
        var shard = run.report().shards().get(0);
        assertEquals(RunReport.Outcome.QUARANTINED, shard.outcome());
        assertEquals("DocumentTooLargeException", shard.errorClass());
        assertFalse(Files.exists(table.resolve("_delta_log")));
    }

    /**
     * Every range starts with a document that does not fit beside the other two ranges but fits a
     * single range: the shard is read again as one range, and the note counts every document once.
     */
    @Test
    void theOneRangeRetryCountsEachDocumentOnce(@TempDir Path tmp) throws Exception {
        // Budget 64 - 4 x 8 = 32 MiB with three ranges, 48 MiB with one; about 8.2 MB converted x 5 needs 39.
        var large = expanding(136_000);
        var docs = List.of(large, stored(2), large, stored(3), large, stored(4));
        var gate = new DocumentMemoryGate(64 * MIB, 8 * MIB, 5.0);
        Path table = tmp.resolve("table");
        var run = run(table, shard(tmp.resolve("shard"), docs), 3, gate);
        assertEquals(RunReport.Outcome.COMMITTED, run.report().shards().get(0).outcome(), String.valueOf(run.report().shards()));
        assertTrue(run.stderr().contains("read again as one range"), run.stderr());
        assertEquals(List.of(note(3, 1, 1)), noteLines(run.stderr()));
        var rows = rows(table);
        assertEquals(6, rows.size());
        for (int n = 0; n < docs.size(); n++) {
            assertEquals(docs.get(n).kind(), rows.get((long) n).kind());
            assertEquals(docs.get(n).row(), rows.get((long) n).source());
        }
    }
}
