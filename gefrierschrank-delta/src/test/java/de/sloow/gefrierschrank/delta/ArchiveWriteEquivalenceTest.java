package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The archive byte row path against its oracle, the JSON row path it replaced
 * ({@link ArchiveContext#document} through {@link DeltaTableWriter#write}): the same accept and
 * reject decisions with the same exception classes and messages, and for accepted documents the
 * same decoded rows (all columns, raw bytes), Parquet footer schemas and Delta schema.
 */
class ArchiveWriteEquivalenceTest {

    private static final ArchiveContext ARCHIVE =
        new ArchiveContext("logs-äöü", "repo-index-id", "snap-uuid", "snap", "logs-äöü", "src-snap-uuid");
    private static final ArchiveContext NO_SNAPSHOT_NAME =
        new ArchiveContext("idx", "rid", "suuid", null, "idx", "suuid");

    record Doc(byte[] bytes, DocumentMetadata metadata) {}

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[]... parts) {
        var out = new java.io.ByteArrayOutputStream();
        for (byte[] part : parts) out.writeBytes(part);
        return out.toByteArray();
    }

    private static byte[] withRaw(String before, int[] raw, String after) {
        byte[] middle = new byte[raw.length];
        for (int i = 0; i < raw.length; i++) middle[i] = (byte) raw[i];
        return concat(utf8(before), middle, utf8(after));
    }

    static Map<String, byte[]> corpus() {
        Map<String, byte[]> corpus = new LinkedHashMap<>();
        corpus.put("ascii", utf8("{\"message\":\"hello world\",\"n\":1}"));
        corpus.put("cjk", utf8("{\"名前\":\"日本語のテキスト\",\"中文\":[\"字符\",\"文字\"]}"));
        corpus.put("emoji", utf8("{\"a\":\"🚀🧊❄️👩‍💻\",\"𝔘\":\"𝔫𝔦𝔠𝔬𝔡𝔢\"}"));
        corpus.put("escapes", utf8("{\"s\":\"\\ud800 \\uD83D\\uDE00 \\\" \\\\ \\/ \\b\\f\\n\\r\\t \\u0000\"}"));
        corpus.put("pretty", utf8("{\n  \"a\": {\n    \"b\": [\n      1,\n      2\n    ]\n  }\n}\n"));
        corpus.put("spaced", utf8(" {\"a\" : 1 , \"b\" : [ true , false , null ] }\n"));
        corpus.put("big-numbers", utf8("{\"u64\":18446744073709551615,\"d\":0.1000000000000000055511151231257827,"
            + "\"n\":123456789012345678901234567890123,\"e\":-1.5E+300,\"z\":-0.0}"));
        corpus.put("over-32k", utf8("{\"blob\":\"" + "x".repeat(40_000) + "\"}"));
        corpus.put("over-1mb", utf8("{\"blob\":\"" + "é€".repeat(400_000) + "\",\"n\":1}"));
        corpus.put("bom", concat(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, utf8("{\"a\":1}")));
        corpus.put("C0-80", withRaw("{\"a\":\"", new int[]{0xC0, 0x80}, "\"}"));
        corpus.put("ED-A0-80", withRaw("{\"a\":\"", new int[]{0xED, 0xA0, 0x80}, "\"}"));
        corpus.put("C3-28", withRaw("{\"a\":\"", new int[]{0xC3, 0x28}, "\"}"));
        corpus.put("lone-FF", withRaw("{\"a\":\"", new int[]{0xFF}, "\"}"));
        corpus.put("F4-90-80-80", withRaw("{\"a\":\"", new int[]{0xF4, 0x90, 0x80, 0x80}, "\"}"));
        corpus.put("name-50000", utf8("{\"" + "k".repeat(50_000) + "\":1}"));
        corpus.put("name-50001", utf8("{\"" + "k".repeat(50_001) + "\":1}"));
        corpus.put("depth-1000", utf8("{\"a\":" + "[".repeat(999) + "]".repeat(999) + "}"));
        corpus.put("depth-1001", utf8("{\"a\":" + "[".repeat(1000) + "]".repeat(1000) + "}"));
        corpus.put("blank", utf8(" \n\t "));
        corpus.put("empty", new byte[0]);
        corpus.put("array-root", utf8("[{\"a\":1}]"));
        corpus.put("string-root", utf8("\"text\""));
        corpus.put("trailing", utf8("{\"a\":1} {\"b\":2}"));
        corpus.put("truncated", utf8("{\"value\":"));
        corpus.put("malformed-in-name", withRaw("{\"", new int[]{0xC0, 0xAF}, "\":1}"));
        corpus.put("malformed-and-invalid", withRaw("{\"a\":", new int[]{0xFF}, "}"));
        return corpus;
    }

    private static List<DocumentMetadata> metadataVariants(int slot) {
        return List.of(
            new DocumentMetadata("id-" + slot, "route-ß", "stored", slot),
            new DocumentMetadata(null, null, "synthetic", slot),
            new DocumentMetadata("ünïcode-🧊-" + slot, null, "unknown", slot),
            new DocumentMetadata("id", "r", null, slot));
    }

    /** Legacy decision for one document: the row JSON, or the failure's class and message. */
    private static String legacyDecision(Doc doc, int shard, ArchiveContext archive) {
        String text = doc.bytes() == null ? null : new String(doc.bytes(), StandardCharsets.UTF_8);
        try {
            if (text == null || text.isBlank()) throw new IOException("Live document has no source payload");
            archive.document(text, shard, doc.metadata());
            return "ACCEPT";
        } catch (Exception e) {
            return e.getClass().getName() + ": " + e.getMessage();
        }
    }

    private static String byteDecision(Doc doc) {
        var rows = new ShardRanges.ArchiveRowIterator(new ListReader(List.of(doc)));
        try {
            assertTrue(rows.hasNext());
            rows.next();
            return "ACCEPT";
        } catch (IllegalStateException wrapped) {
            assertEquals("reading _source of doc 0", wrapped.getMessage());
            Throwable e = wrapped.getCause();
            return e.getClass().getName() + ": " + e.getMessage();
        }
    }

    @Test
    void decisionsAndFailuresMatchTheLegacyPath() {
        var mismatches = new ArrayList<String>();
        int accepted = 0;
        for (var entry : corpus().entrySet()) {
            var doc = new Doc(entry.getValue(), new DocumentMetadata("id", null, "stored", 0));
            String legacy = legacyDecision(doc, 0, ARCHIVE);
            String bytes = byteDecision(doc);
            if (!legacy.equals(bytes)) mismatches.add(entry.getKey() + ": legacy " + legacy + " / bytes " + bytes);
            if (legacy.equals("ACCEPT")) accepted++;
        }
        var missing = new Doc(null, new DocumentMetadata("id", null, "stored", 0));
        assertEquals(legacyDecision(missing, 0, ARCHIVE), byteDecision(missing));
        assertEquals(List.of(), mismatches);
        assertTrue(accepted >= 15, "most of the corpus exports: " + accepted);
    }

    @Test
    void rowsAndSchemasMatchTheLegacyPath(@TempDir Path tmp) throws Exception {
        List<Doc> docs = new ArrayList<>();
        int slot = 0;
        for (byte[] source : corpus().values()) {
            for (DocumentMetadata metadata : metadataVariants(slot)) {
                var doc = new Doc(source, metadata);
                if (legacyDecision(doc, 3, ARCHIVE).equals("ACCEPT")) docs.add(doc);
            }
            slot += 2;
        }
        assertTrue(docs.size() > 40);
        for (ArchiveContext archive : List.of(ARCHIVE, NO_SNAPSHOT_NAME)) {
            String legacyTable = tmp.resolve("legacy-" + archive.snapshotUuid()).toString();
            String byteTable = tmp.resolve("bytes-" + archive.snapshotUuid()).toString();
            var conf = new Configuration();
            long legacyRows = new DeltaTableWriter(legacyTable).write(conf, legacyRows(docs, 3, archive),
                ArchiveContext.mapping(), archive.key(3));
            var result = new DeltaTableWriter(byteTable).writeArchive(conf,
                List.of(new ShardRanges.ArchiveRowIterator(new ListReader(docs))), archive, 3, archive.key(3));
            assertEquals(new DeltaTableWriter.WriteResult(legacyRows, false), result);
            assertEquals(docs.size(), legacyRows);
            assertTableEquals(conf, legacyTable, byteTable);
        }
    }

    @Test
    void emptyShardsMatchTheLegacyPath(@TempDir Path tmp) throws Exception {
        var conf = new Configuration();
        String legacyTable = tmp.resolve("legacy").toString();
        String byteTable = tmp.resolve("bytes").toString();
        assertEquals(0, new DeltaTableWriter(legacyTable).write(conf, List.<String>of().iterator(),
            ArchiveContext.mapping(), ARCHIVE.key(0)));
        assertEquals(new DeltaTableWriter.WriteResult(0, false), new DeltaTableWriter(byteTable).writeArchive(conf,
            List.of(new ShardRanges.ArchiveRowIterator(new ListReader(List.of()))), ARCHIVE, 0, ARCHIVE.key(0)));
        assertTableEquals(conf, legacyTable, byteTable);
        assertEquals(1, ParquetFooters.addFilePaths(conf, byteTable).size(), "an empty shard keeps its one empty file");
    }

    @Test
    void aRejectedDocumentFailsTheShardLikeTheLegacyPath(@TempDir Path tmp) throws Exception {
        var conf = new Configuration();
        var good = new Doc(utf8("{\"a\":1}"), new DocumentMetadata("1", null, "stored", 0));
        var bad = new Doc(utf8("{\"a\":1}{"), new DocumentMetadata("2", null, "stored", 1));
        String byteTable = tmp.resolve("bytes").toString();
        var failure = assertThrows(IllegalStateException.class, () -> new DeltaTableWriter(byteTable).writeArchive(conf,
            List.of(new ShardRanges.ArchiveRowIterator(new ListReader(List.of(good, bad)))), ARCHIVE, 0, ARCHIVE.key(0)));
        assertEquals("reading _source of doc 1", failure.getMessage());
        assertEquals("Source has trailing JSON content", failure.getCause().getMessage());
        assertEquals(java.util.Set.of(), ParquetFooters.parquetFilesOnDisk(Path.of(byteTable)));
    }

    @Test
    void theByteRowPathKeepsTheReadersArraysAndCountsPerDocumentValidations() {
        byte[] exact = utf8("{\"a\":1}");
        byte[] padded = concat(exact, utf8("garbage"));
        byte[] malformed = withRaw("{\"a\":\"", new int[]{0xC3, 0x28}, "\"}");
        var reader = new ListReader(List.of(new Doc(exact, new DocumentMetadata("1", null, "stored", 0)),
            new Doc(malformed, new DocumentMetadata("2", null, "stored", 1))));
        reader.lengths.put(0, exact.length);
        reader.arrays.put(0, padded);
        var rows = new ShardRanges.ArchiveRowIterator(reader);
        ArchiveRow first = rows.next();
        assertSame(padded, first.source(), "no copy of an accepted source");
        assertEquals(exact.length, first.length());
        ArchiveRow second = rows.next();
        assertEquals("{\"a\":\"\uFFFD(\"}", new String(second.source(), 0, second.length(), StandardCharsets.UTF_8));
        assertFalse(rows.hasNext());
        assertEquals(1, rows.perDocumentValidations());
    }

    private static Iterator<String> legacyRows(List<Doc> docs, int shard, ArchiveContext archive) {
        return docs.stream().map(doc -> {
            try {
                return archive.document(new String(doc.bytes(), StandardCharsets.UTF_8), shard, doc.metadata());
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }).iterator();
    }

    static void assertTableEquals(Configuration conf, String expected, String actual) throws Exception {
        assertEquals(rows(conf, expected), rows(conf, actual), "decoded rows");
        assertEquals(footerSchemas(conf, expected), footerSchemas(conf, actual), "Parquet footer schemas");
        assertEquals(io.delta.standalone.DeltaLog.forTable(conf, expected).update().getMetadata().getSchema().toJson(),
            io.delta.standalone.DeltaLog.forTable(conf, actual).update().getMetadata().getSchema().toJson(), "Delta schema");
        assertEquals(tags(conf, expected), tags(conf, actual), "AddFile tags");
    }

    /** Every row as field=value pairs in schema order, strings as their raw UTF-8 bytes. */
    static List<String> rows(Configuration conf, String table) throws IOException {
        var rows = new ArrayList<String>();
        for (var file : ParquetFooters.addFiles(conf, table)) {
            var path = new org.apache.hadoop.fs.Path(table + "/" + file.getPath());
            try (var reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(path, conf)).withConf(conf).build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    var row = new StringBuilder();
                    for (var field : record.getSchema().getFields()) {
                        Object value = record.get(field.name());
                        row.append(field.name()).append('=');
                        if (value instanceof Utf8 text) row.append(HexFormat.of().formatHex(text.getBytes(), 0, text.getByteLength()));
                        else row.append(value);
                        row.append(';');
                    }
                    rows.add(row.toString());
                }
            }
        }
        return rows;
    }

    static List<String> footerSchemas(Configuration conf, String table) throws IOException {
        var schemas = new ArrayList<String>();
        for (var file : ParquetFooters.addFiles(conf, table)) {
            var path = new org.apache.hadoop.fs.Path(table + "/" + file.getPath());
            try (var reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
                var meta = reader.getFooter().getFileMetaData();
                schemas.add(meta.getSchema() + "\n" + meta.getKeyValueMetaData());
            }
        }
        return schemas;
    }

    private static List<Map<String, String>> tags(Configuration conf, String table) {
        return ParquetFooters.addFiles(conf, table).stream().map(io.delta.standalone.actions.AddFile::getTags).toList();
    }

    /** Serves documents in order; slot i is document i. Optionally hands out a longer array. */
    static final class ListReader implements ShardReader {
        private final List<Doc> docs;
        final Map<Integer, byte[]> arrays = new java.util.HashMap<>();
        final Map<Integer, Integer> lengths = new java.util.HashMap<>();

        ListReader(List<Doc> docs) {
            this.docs = docs;
        }

        @Override public String runtimeName() { return "list"; }
        @Override public int docCount() { return docs.size(); }
        @Override public DocumentMetadata documentMetadata(int docId) { return docs.get(docId).metadata(); }

        @Override
        public void readSource(int docId, SourceConsumer consumer) {
            if (docId >= docs.size()) throw new NoSuchElementException();
            byte[] bytes = docs.get(docId).bytes();
            if (bytes == null) return;
            byte[] array = arrays.getOrDefault(docId, bytes.clone());
            consumer.accept(array, lengths.getOrDefault(docId, bytes.length));
        }

        @Override public void close() {}
    }
}
