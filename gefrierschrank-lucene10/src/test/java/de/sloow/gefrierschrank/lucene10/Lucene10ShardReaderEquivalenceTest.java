package de.sloow.gefrierschrank.lucene10;

import com.github.luben.zstd.Zstd;
import de.sloow.gefrierschrank.core.lucene.DocumentId;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.IntPredicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins {@link Lucene10ShardReader}'s observable behaviour to the 1.0.0 implementation: the oracle
 * below is that implementation's stored-field logic inlined verbatim (a full
 * {@code StoredFields.document(docId)} per call, {@code getField}/{@code getBinaryValue}/{@code get},
 * {@code DocumentId.decode}). Every access order the pipeline, the {@code dump} command and the
 * synthetic-source fallback can produce must yield identical source bytes, identical metadata and
 * identical exception class and message.
 */
class Lucene10ShardReaderEquivalenceTest {

    static final Path CORE_FIXTURES = Path.of("../gefrierschrank-core/src/test/resources/fixtures");
    static final Path SYNTH_FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");

    /**
     * The no-_source message, reworded since 1.0.0 because both exports now reconstruct synthetic
     * source; SyntheticSourceFallbackShardReader matches its 'synthetic source' phrase.
     */
    static String legacyMissingSourceMessage(int docId) {
        return "document " + docId + " has no stored _source (synthetic source);"
            + " dump-to-delta and dump reconstruct it when the index metadata is available";
    }

    // ---------------------------------------------------------------- oracle

    sealed interface Outcome permits SourceBytes, Metadata, Failure {}

    record SourceBytes(byte[] bytes) implements Outcome {
        @Override public boolean equals(Object other) {
            return other instanceof SourceBytes that && Arrays.equals(bytes, that.bytes);
        }
        @Override public int hashCode() { return Arrays.hashCode(bytes); }
        @Override public String toString() {
            return "SourceBytes[len=" + bytes.length + ", hash=" + Arrays.hashCode(bytes) + "]";
        }
    }

    record Metadata(DocumentMetadata value) implements Outcome {}

    record Failure(String type, String message) implements Outcome {
        static Failure of(Exception e) { return new Failure(e.getClass().getName(), e.getMessage()); }
    }

    /** 1.0.0's Lucene10ShardReader stored-field logic, inlined. */
    static final class LegacyOracle implements AutoCloseable {
        private final NIOFSDirectory directory;
        private final DirectoryReader reader;
        private final StoredFields storedFields;

        LegacyOracle(Path dir) throws IOException {
            directory = new NIOFSDirectory(dir);
            var commits = DirectoryReader.listCommits(directory);
            // For creation majors 9 and 10 this is exactly DirectoryReader.open(directory); the floor
            // of 8 only admits ES 7-created shards (see Lucene10ShardReaderCreatedMajor8Test), which the
            // 1.0.0 reader could not open at all.
            reader = DirectoryReader.open(commits.get(commits.size() - 1), 8, null);
            storedFields = reader.storedFields();
        }

        int maxDoc() { return reader.maxDoc(); }

        Outcome source(int docId) {
            try {
                var source = storedFields.document(docId).getField("_source");
                if (source == null) throw new IOException(legacyMissingSourceMessage(docId));
                var value = source.binaryValue();
                byte[] bytes = value.offset == 0 ? value.bytes
                    : Arrays.copyOfRange(value.bytes, value.offset, value.offset + value.length);
                return new SourceBytes(Arrays.copyOf(bytes, value.length));
            } catch (Exception e) {
                return Failure.of(e);
            }
        }

        Outcome metadata(int docId) {
            try {
                var doc = storedFields.document(docId);
                var id = doc.getBinaryValue("_id");
                String decoded = id == null ? doc.get("_id") : DocumentId.decode(id.bytes, id.offset, id.length);
                return new Metadata(new DocumentMetadata(decoded, doc.get("_routing"),
                    doc.getField("_source") == null ? "synthetic" : "stored", docId));
            } catch (Exception e) {
                return Failure.of(e);
            }
        }

        @Override
        public void close() throws IOException {
            try (directory; reader) { /* close both */ }
        }
    }

    static Outcome source(Lucene10ShardReader reader, int docId) {
        return sourceWithArray(reader, docId).outcome();
    }

    record Handed(Outcome outcome, byte[] array) {}

    static Handed sourceWithArray(Lucene10ShardReader reader, int docId) {
        var handed = new Object[2];
        try {
            reader.readSource(docId, (bytes, length) -> {
                handed[0] = new SourceBytes(Arrays.copyOf(bytes, length));
                handed[1] = bytes;
            });
            return new Handed((Outcome) handed[0], (byte[]) handed[1]);
        } catch (Exception e) {
            return new Handed(Failure.of(e), null);
        }
    }

    static Outcome metadata(Lucene10ShardReader reader, int docId) {
        try {
            return new Metadata(reader.documentMetadata(docId));
        } catch (Exception e) {
            return Failure.of(e);
        }
    }

    // ---------------------------------------------------------------- access patterns

    /** Runs every access pattern against a fresh reader each and compares with the oracle. */
    static void assertEquivalentToLegacy(Path shardDir) throws Exception {
        try (var oracle = new LegacyOracle(shardDir)) {
            int maxDoc = oracle.maxDoc();
            Outcome[] expectedSource = new Outcome[maxDoc];
            Outcome[] expectedMetadata = new Outcome[maxDoc];
            for (int doc = 0; doc < maxDoc; doc++) {
                expectedSource[doc] = oracle.source(doc);
                expectedMetadata[doc] = oracle.metadata(doc);
            }

            try (var reader = new Lucene10ShardReader(shardDir)) {
                assertEquals(maxDoc, reader.docCount());
                // Production order: live docs ascending, readSource then documentMetadata.
                for (int doc = 0; doc < maxDoc; doc++) {
                    if (!reader.isLive(doc)) continue;
                    assertEquals(expectedSource[doc], source(reader, doc), "production source of doc " + doc);
                    assertEquals(expectedMetadata[doc], metadata(reader, doc), "production metadata of doc " + doc);
                }
            }
            try (var reader = new Lucene10ShardReader(shardDir)) {
                for (int doc = 0; doc < maxDoc; doc++) {
                    assertEquals(expectedMetadata[doc], metadata(reader, doc), "metadata-first metadata of doc " + doc);
                    assertEquals(expectedSource[doc], source(reader, doc), "metadata-first source of doc " + doc);
                    assertEquals(expectedMetadata[doc], metadata(reader, doc), "metadata-again of doc " + doc);
                }
            }
            try (var reader = new Lucene10ShardReader(shardDir)) {
                for (int doc = 0; doc < maxDoc; doc++) {
                    Handed first = sourceWithArray(reader, doc);
                    Handed second = sourceWithArray(reader, doc);
                    assertEquals(expectedSource[doc], first.outcome(), "first readSource of doc " + doc);
                    assertEquals(expectedSource[doc], second.outcome(), "second readSource of doc " + doc);
                    if (first.array() != null) {
                        assertNotSame(first.array(), second.array(), "a repeated readSource must hand out a fresh array");
                    }
                    assertEquals(expectedMetadata[doc], metadata(reader, doc), "metadata after two reads of doc " + doc);
                }
            }
            try (var reader = new Lucene10ShardReader(shardDir)) {
                for (int doc = maxDoc - 1; doc >= 0; doc--) {
                    assertEquals(expectedSource[doc], source(reader, doc), "descending source of doc " + doc);
                    assertEquals(expectedMetadata[doc], metadata(reader, doc), "descending metadata of doc " + doc);
                }
            }
            try (var reader = new Lucene10ShardReader(shardDir)) {
                var random = new Random(maxDoc * 31L + 7);
                for (int step = 0; step < Math.max(4 * maxDoc, 64); step++) {
                    int doc = random.nextInt(maxDoc + 2) - 1;
                    boolean inRange = doc >= 0 && doc < maxDoc;
                    if (random.nextBoolean()) {
                        Outcome expected = inRange ? expectedSource[doc] : oracle.source(doc);
                        assertEquals(expected, source(reader, doc), "random source of doc " + doc + " at step " + step);
                    } else {
                        Outcome expected = inRange ? expectedMetadata[doc] : oracle.metadata(doc);
                        assertEquals(expected, metadata(reader, doc), "random metadata of doc " + doc + " at step " + step);
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------- real Elasticsearch fixture shards

    record FixtureShard(String label, Path repo, String index, String snapshotUuid, int shardId) {
        @Override public String toString() { return label; }
    }

    static List<FixtureShard> fixtureShards(IntPredicate luceneMajorReadable) throws IOException {
        var repos = List.of(
            CORE_FIXTURES.resolve("demo-repo"),
            CORE_FIXTURES.resolve("reused-index-repo"),
            CORE_FIXTURES.resolve("frozen-repo"),
            CORE_FIXTURES.resolve("es7-features-repo"),
            CORE_FIXTURES.resolve("es7-upgraded-repo"),
            CORE_FIXTURES.resolve("es7-best-compression-repo"),
            SYNTH_FIXTURES.resolve("synth-repo"),
            SYNTH_FIXTURES.resolve("zstd-synth-repo"));
        var shards = new ArrayList<FixtureShard>();
        for (Path repo : repos) {
            var inspector = new RepositoryInspector(new FileSystemBlobStore(repo));
            for (var index : inspector.overview().indices()) {
                for (String snapshot : index.snapshotUuids()) {
                    for (int shardId = 0; shardId < index.shardGenerations().size(); shardId++) {
                        ShardSnapshot shard;
                        try {
                            shard = inspector.shardSnapshot(index.name(), shardId, snapshot);
                        } catch (IOException missing) {
                            continue;
                        }
                        boolean hasCommit = shard.files().stream().anyMatch(f -> f.physicalName().startsWith("segments_"));
                        boolean readable = shard.files().stream().map(RepoFileInfo::writtenBy)
                            .filter(v -> v != null)
                            .allMatch(v -> luceneMajorReadable.test(Integer.parseInt(v.substring(0, v.indexOf('.')))));
                        if (hasCommit && readable) {
                            shards.add(new FixtureShard(repo.getFileName() + "/" + index.name() + "/" + snapshot + "/" + shardId,
                                repo, index.name(), snapshot, shardId));
                        }
                    }
                }
            }
        }
        return shards;
    }

    static Path materialize(FixtureShard fixture, Path target) throws IOException {
        var blobs = new FileSystemBlobStore(fixture.repo());
        var shard = new RepositoryInspector(blobs).shardSnapshot(fixture.index(), fixture.shardId(), fixture.snapshotUuid());
        new ShardMaterializer(blobs).materialize(shard, target);
        return target;
    }

    static List<FixtureShard> luceneNineAndTenFixtureShards() throws IOException {
        return fixtureShards(major -> major >= 9);
    }

    /** Every shard Lucene10ShardReader opens; es7_completion needs lucene-suggest, only on the CLI classpath. */
    static List<FixtureShard> readableFixtureShards() throws IOException {
        return fixtureShards(major -> major >= 8).stream().filter(shard -> !shard.index().equals("es7_completion")).toList();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("readableFixtureShards")
    void fixtureShardMatchesTheLegacyOracle(FixtureShard fixture, @TempDir Path tmp) throws Exception {
        assertEquivalentToLegacy(materialize(fixture, tmp.resolve("shard")));
    }

    @Test
    void everyFixtureFamilyIsCovered() throws IOException {
        var labels = readableFixtureShards().stream().map(FixtureShard::label).toList();
        for (String expected : List.of("demo-repo/test_data_7_17_9", "demo-repo/test_data_8_17_0", "demo-repo/test_data_9_3_0",
            "reused-index-repo/", "frozen-repo/", "es7-features-repo/es7_features", "es7-features-repo/es7_hard_deletes",
            "es7-features-repo/es7_no_source", "es7-upgraded-repo/es7_mixed", "es7-upgraded-repo/es7_merged", "es7-best-compression-repo/es7_best_compression",
            "synth-repo/test_plain_synth", "synth-repo/test_tsdb_synth",
            "zstd-synth-repo/test_zstd_synth", "zstd-synth-repo/test_zstd_logsdb")) {
            assertTrue(labels.stream().anyMatch(label -> label.startsWith(expected)), expected + " missing from " + labels);
        }
    }

    // ---------------------------------------------------------------- generated shards per stored-fields codec

    enum StoredCodec {
        LZ4(new Lucene103Codec(Lucene103Codec.Mode.BEST_SPEED)),
        DEFLATE(new Lucene103Codec(Lucene103Codec.Mode.BEST_COMPRESSION)),
        ZSTD_14K(new WritableEsZstdCodec(14336, 128, 1)),
        ZSTD_240K(new WritableEsZstdCodec(245760, 2048, 3));

        final Codec codec;

        StoredCodec(Codec codec) { this.codec = codec; }
    }

    static final int DOCS_PER_SEGMENT = 400;
    static final int SLICED_DOC_BYTES = 1_100_000;

    @ParameterizedTest
    @EnumSource(StoredCodec.class)
    void generatedShardMatchesTheLegacyOracle(StoredCodec codec, @TempDir Path tmp) throws Exception {
        buildShard(tmp, codec.codec, 42);
        try (var reader = DirectoryReader.open(new NIOFSDirectory(tmp))) {
            assertTrue(reader.leaves().size() >= 3, "multiple segments");
            assertTrue(reader.numDeletedDocs() > 0, "hard deletes present");
        }
        assertEquivalentToLegacy(tmp);
    }

    @Test
    void nonBinarySourceFailsWithoutTheSyntheticSourcePhrase(@TempDir Path tmp) throws Exception {
        try (var dir = new NIOFSDirectory(tmp); var writer = new IndexWriter(dir, new IndexWriterConfig())) {
            var doc = new Document();
            doc.add(new StoredField("_id", new BytesRef(new byte[]{(byte) 254, 0x1f})));
            doc.add(new StoredField("_source", "{\"not\":\"binary\"}"));
            doc.add(new StoredField("_source", new BytesRef("{\"second\":1}")));
            writer.addDocument(doc);
        }
        try (var reader = new Lucene10ShardReader(tmp)) {
            var failure = assertThrows(IOException.class, () -> reader.readSource(0, (bytes, length) -> {}));
            assertFalse(failure.getMessage().contains("synthetic source"), failure.getMessage());
            assertEquals(new DocumentMetadata("1", null, "stored", 0), reader.documentMetadata(0));
        }
    }

    @Test
    void outOfRangeIdsFailLikeTheCompositeReader(@TempDir Path tmp) throws Exception {
        buildShard(tmp, StoredCodec.LZ4.codec, 7);
        try (var oracle = new LegacyOracle(tmp); var reader = new Lucene10ShardReader(tmp)) {
            for (int doc : new int[]{-1, oracle.maxDoc(), Integer.MAX_VALUE}) {
                var failure = assertInstanceOf(Failure.class, source(reader, doc));
                assertEquals(IllegalArgumentException.class.getName(), failure.type());
                assertEquals(oracle.source(doc), failure);
                assertEquals(oracle.metadata(doc), metadata(reader, doc));
            }
        }
    }

    /**
     * A visit that fails (a corrupted chunk header in the second segment) is never served from the
     * per-document cache: every later call for that document fails the same way, while the other
     * segment stays readable. (Lucene's block state resets itself after a failed block read.)
     */
    @Test
    void failedVisitIsNeverServedFromTheCache(@TempDir Path tmp) throws Exception {
        var config = new IndexWriterConfig().setCodec(StoredCodec.LZ4.codec)
            .setMergePolicy(NoMergePolicy.INSTANCE).setUseCompoundFile(false);
        try (var directory = new NIOFSDirectory(tmp); var writer = new IndexWriter(directory, config)) {
            for (int n = 0; n < 20; n++) {
                var doc = new Document();
                doc.add(new StoredField("_id", new BytesRef(utf8Id("doc-" + n))));
                doc.add(new StoredField("_source", new BytesRef("{\"n\":" + n + "}")));
                writer.addDocument(doc);
                if (n == 9) writer.commit();
            }
        }
        Path fdt = tmp.resolve("_1.fdt");
        byte[] bytes = Files.readAllBytes(fdt);
        int pos = 4;                // header magic
        pos += 1 + bytes[pos];      // format name
        pos += 4 + 16;              // version, segment id
        pos += 1 + bytes[pos];      // suffix
        assertEquals(0, bytes[pos], "docBase of the segment's only chunk");
        bytes[pos] = 5;
        Files.write(fdt, bytes);

        var doc9 = new DocumentMetadata("doc-9", null, "stored", 9);
        try (var reader = new Lucene10ShardReader(tmp)) {
            assertEquals(doc9, reader.documentMetadata(9));
            assertThrows(CorruptIndexException.class, () -> reader.readSource(10, (b, l) -> {}));
            assertThrows(CorruptIndexException.class, () -> reader.documentMetadata(10));
            assertThrows(CorruptIndexException.class, () -> reader.readSource(15, (b, l) -> {}));
            assertThrows(CorruptIndexException.class, () -> reader.documentMetadata(15));
            assertEquals(doc9, reader.documentMetadata(9));
            reader.readSource(9, (b, l) -> assertEquals("{\"n\":9}", new String(b, 0, l, StandardCharsets.UTF_8)));
        }
    }

    /**
     * Three or more segments (NoMergePolicy) with hard deletes, soft updates, nested children, a doc
     * larger than twice the chunk size (stored sliced), an empty _source, docs with and without
     * _routing, every binary _id encoding plus string, numeric and zero-length ids.
     */
    static void buildShard(Path dir, Codec codec, long seed) throws IOException {
        var config = new IndexWriterConfig()
            .setCodec(codec)
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setSoftDeletesField("__soft_deletes");
        var random = new Random(seed);
        try (var directory = new NIOFSDirectory(dir); var writer = new IndexWriter(directory, config)) {
            int n = 0;
            for (int segment = 0; segment < 3; segment++) {
                for (int i = 0; i < DOCS_PER_SEGMENT; i++, n++) {
                    var block = new ArrayList<Document>();
                    if (n % 10 == 0) {
                        for (int child = 0; child < 2; child++) block.add(nestedChild(n, child));
                    }
                    block.add(root(n, random, 0));
                    writer.addDocuments(block);
                }
                writer.commit();
            }
            writer.deleteDocuments(new Term("key", "doc-5"), new Term("key", "doc-" + (DOCS_PER_SEGMENT + 7)),
                new Term("key", "doc-" + (2 * DOCS_PER_SEGMENT + 1)));
            for (int updated : new int[]{9, DOCS_PER_SEGMENT + 3, 2 * DOCS_PER_SEGMENT + 11}) {
                writer.softUpdateDocument(new Term("key", "doc-" + updated), root(updated, random, 1),
                    new NumericDocValuesField("__soft_deletes", 1));
            }
            writer.commit();
        }
    }

    private static Document nestedChild(int parent, int child) {
        var doc = new Document();
        doc.add(new StringField("_nested_path", "comments", Field.Store.NO));
        doc.add(new StoredField("child_value", "child " + child + " of " + parent));
        return doc;
    }

    private static Document root(int n, Random random, int version) {
        var doc = new Document();
        doc.add(new StringField("key", "doc-" + n, Field.Store.NO));
        doc.add(new NumericDocValuesField("_primary_term", 1));
        addId(doc, n, random);
        if (n % 3 == 0) doc.add(new StoredField("_routing", "tenant-" + (n % 5)));
        if (n == 44) doc.add(new StoredField("_routing", 42));
        if (n % 4 == 0) doc.add(new StoredField("_recovery_source", new BytesRef(new byte[]{1, 2, 3})));
        if (n % 7 == 0) doc.add(new StoredField("_ignored", "field_" + n));
        doc.add(new StoredField("long_field", (long) n * 1_000_003L));
        doc.add(new StoredField("double_field", n / 7.0));
        if (n == 13) return doc; // live root without any _source
        doc.add(new StoredField("_source", new BytesRef(sourceBytes(n, random, version))));
        if (n == 21) doc.add(new StoredField("_source", "{\"shadowed\":true}"));
        return doc;
    }

    private static void addId(Document doc, int n, Random random) {
        if (n == 17) { doc.add(new StoredField("_id", new BytesRef(new byte[0]))); return; }
        if (n == 18) { doc.add(new StoredField("_id", new BytesRef(new byte[]{(byte) 254, (byte) 0xab}))); return; }
        switch (n % 8) {
            case 0 -> doc.add(new StoredField("_id", new BytesRef(packedNumericId(Integer.toString(n * 37 + 1)))));
            case 1 -> doc.add(new StoredField("_id", new BytesRef(utf8Id("id-ü-" + n + "-漢"))));
            case 2 -> {
                byte[] raw = new byte[12];
                random.nextBytes(raw);
                raw[0] = (byte) (raw[0] & 0x7f);
                doc.add(new StoredField("_id", new BytesRef(raw)));
            }
            case 3 -> {
                byte[] raw = new byte[10];
                random.nextBytes(raw);
                raw[0] = (byte) 253;
                raw[1] = (byte) (0xfd | (raw[1] & 0x03));
                doc.add(new StoredField("_id", new BytesRef(raw)));
            }
            case 4 -> doc.add(new StoredField("_id", "str-" + n));
            case 5 -> {
                doc.add(new StoredField("_id", "string-before-binary-" + n));
                doc.add(new StoredField("_id", new BytesRef(packedNumericId(Integer.toString(n)))));
            }
            case 6 -> doc.add(new StoredField("_id", n));
            default -> doc.add(new StoredField("_id", ""));
        }
    }

    static byte[] packedNumericId(String digits) {
        byte[] packed = new byte[1 + (digits.length() + 1) / 2];
        packed[0] = (byte) 254;
        for (int i = 0; i < digits.length(); i += 2) {
            int high = digits.charAt(i) - '0';
            int low = i + 1 < digits.length() ? digits.charAt(i + 1) - '0' : 15;
            packed[1 + i / 2] = (byte) (high << 4 | low);
        }
        return packed;
    }

    static byte[] utf8Id(String id) {
        byte[] text = id.getBytes(StandardCharsets.UTF_8);
        byte[] encoded = new byte[text.length + 1];
        encoded[0] = (byte) 255;
        System.arraycopy(text, 0, encoded, 1, text.length);
        return encoded;
    }

    private static final String[] WORDS = ("request served upstream latency cache miss backend timeout retry client"
        + " connection pool gateway handshake payload checksum shard replica segment merge flush refresh").split(" ");

    private static byte[] sourceBytes(int n, Random random, int version) {
        if (n % 50 == 49) return new byte[0];
        if (n == DOCS_PER_SEGMENT + 100) {
            var huge = new StringBuilder(SLICED_DOC_BYTES + 64).append("{\"n\":").append(n).append(",\"blob\":\"");
            while (huge.length() < SLICED_DOC_BYTES) huge.append((char) ('a' + random.nextInt(26)));
            return huge.append("\"}").toString().getBytes(StandardCharsets.UTF_8);
        }
        var json = new StringBuilder("{ \"n\" : ").append(n).append(", \"v\":").append(version).append(",\"message\":\"");
        int words = 150 + random.nextInt(80);
        for (int w = 0; w < words; w++) json.append(WORDS[random.nextInt(WORDS.length)]).append(' ');
        json.append("éß€\",\n  \"price\": 1.10000000000000000001, \"tags\":[\"a\",\"b\"] }");
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Writes segments the production codec {@code Elasticsearch92Lucene103} reads back: same SPI name,
     * same stored-fields format name, real ZSTD frames. Reads therefore go through the unmodified
     * EsZstdStoredFieldsFormat/EsZstdDecompressor (the chunk size is recovered from the fdm file).
     */
    static final class WritableEsZstdCodec extends FilterCodec {
        private final StoredFieldsFormat storedFields;

        WritableEsZstdCodec(int chunkSize, int maxDocsPerChunk, int level) {
            super("Elasticsearch92Lucene103", new Lucene103Codec());
            storedFields = new Lucene90CompressingStoredFieldsFormat("ZstdStoredFields814",
                new WritableZstdMode(level), chunkSize, maxDocsPerChunk, 10);
        }

        @Override
        public StoredFieldsFormat storedFieldsFormat() {
            return storedFields;
        }
    }

    private static final class WritableZstdMode extends CompressionMode {
        private final int level;

        WritableZstdMode(int level) { this.level = level; }

        @Override
        public Compressor newCompressor() {
            return new Compressor() {
                @Override
                public void compress(ByteBuffersDataInput in, DataOutput out) throws IOException {
                    byte[] src = new byte[(int) in.length()];
                    in.readBytes(src, 0, src.length);
                    byte[] compressed = Zstd.compress(src, level);
                    out.writeVInt(compressed.length);
                    out.writeBytes(compressed, 0, compressed.length);
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public Decompressor newDecompressor() {
            return new EsZstdDecompressor();
        }
    }
}
