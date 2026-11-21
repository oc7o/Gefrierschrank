package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One shard holding both stored-source and synthetic-source documents, through the whole archive
 * export. Elasticsearch accepts switching an existing index's {@code _source.mode} from stored to
 * synthetic (earlier segments keep {@code _source}, later ones do not), and merging such segments
 * yields one segment where only some documents carry {@code _source}. The shard is built from two
 * real fixture shards with the same created major and no index sort: es93-repo's
 * {@code test_es93_vectors_included} (20 stored documents, docs 0-19) and synth-repo's
 * {@code test_plain_synth} (50 synthetic documents, docs 20-69).
 *
 * <p>The export goes through {@link DumpToDeltaPipeline#runSnapshot} with an opener that swaps the
 * mixed index into the materialized shard directory, which is where reconstruction reads from. It
 * runs in a child JVM laid out like the shaded CLI jar (see {@link Es93BinaryDocValuesPipelineTest}):
 * on this module's class path Elasticsearch's vector formats fail to load outside a node, and a
 * failed {@code KnnVectorsFormat} holder would break every later test in the JVM.
 */
class MixedSourceShardTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final Path SYNTH_REPO = FIXTURES.resolve("synth-repo");
    private static final String SYNTH_INDEX = "test_plain_synth";
    private static final String STORED_INDEX = "test_es93_vectors_included";

    enum Variant {
        /** addIndexes(Directory...): the stored and the synthetic segment side by side. */
        SEGMENTS,
        /** addIndexes(CodecReader...): one merged segment in which only the stored documents have _source. */
        MERGED,
        /** The side-by-side shard under a mapping with {@code _source.enabled: false}. */
        STORED_PLUS_DISABLED
    }

    @TempDir
    static Path runs;
    private static final Map<Variant, JsonNode> REPORTS = new HashMap<>();
    private static final Map<Variant, int[]> COUNTS = new HashMap<>();

    @BeforeAll
    static void exportEveryVariantInChildJvm() throws Exception {
        List<String> command = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Djdk.util.jar.enableMultiRelease=false",
            "-cp", String.join(File.pathSeparator, Es93BinaryDocValuesPipelineTest.productionOrderedClassPath()),
            ChildMain.class.getName(), runs.toString()));
        for (String property : List.of("gefrierschrank.reconstruction.directory",
                "gefrierschrank.shard.threads", "gefrierschrank.shard.threads.minRangeLiveDocs")) {
            if (System.getProperty(property) != null) command.add(1, "-D" + property + "=" + System.getProperty(property));
        }
        Path out = runs.resolve("child.out");
        Path err = runs.resolve("child.err");
        Process process = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile()).start();
        assertTrue(process.waitFor(5, TimeUnit.MINUTES));
        assertEquals(0, process.exitValue(), Files.readString(out) + Files.readString(err));
        for (String line : Files.readAllLines(out)) {
            String[] parts = line.split(" ", 3);
            if (parts[0].equals("REPORT")) {
                REPORTS.put(Variant.valueOf(parts[1]), JSON.readTree(parts[2]));
            } else if (parts[0].equals("COUNTS")) {
                String[] counts = parts[2].split(" ");
                COUNTS.put(Variant.valueOf(parts[1]),
                    new int[]{Integer.parseInt(counts[0]), Integer.parseInt(counts[1]), Integer.parseInt(counts[2])});
            }
        }
        assertEquals(Variant.values().length, REPORTS.size(), Files.readString(out));
    }

    @ParameterizedTest
    @EnumSource(value = Variant.class, names = {"SEGMENTS", "MERGED"})
    void storedRowsStayByteExactAndSyntheticRowsMatchTheGolden(Variant variant) throws Exception {
        JsonNode report = REPORTS.get(variant);
        assertEquals(1, report.path("committedCount").asInt(), report.toString());
        assertEquals(70, report.path("rowsWrittenThisRun").asLong(), report.toString());

        Map<Long, Row> rows = rows(runs.resolve(variant.name()).resolve("table").toString());
        assertEquals(70, rows.size(), "one row per live document and no duplicate _doc");
        Map<String, Integer> kinds = new TreeMap<>();
        rows.values().forEach(row -> kinds.merge(row.kind(), 1, Integer::sum));
        assertEquals(Map.of("stored", 20, "synthetic", 50), kinds);

        Map<String, byte[]> storedById = readStoredSources(runs.resolve(variant.name()).resolve("stored-sources.bin"));
        assertEquals(20, storedById.size());
        List<String> synthetic = new ArrayList<>();
        for (Row row : rows.values()) {
            if (row.kind().equals("stored")) {
                byte[] expected = storedById.get(row.id());
                assertNotNull(expected, "stored row " + row.id() + " comes from the stored shard");
                assertArrayEquals(expected, row.source().getBytes(StandardCharsets.UTF_8), "stored row " + row.id());
            } else {
                synthetic.add(row.source());
            }
        }
        assertEquals(golden(), synthetic, "synthetic rows in _doc order");

        int[] counts = COUNTS.get(variant);
        if (variant == Variant.SEGMENTS) {
            assertEquals(50, counts[0], "documents of the segment without _source");
            assertEquals(0, counts[1], "documents of the segment without _source must go straight to reconstruction");
        } else {
            assertEquals(0, counts[0], "the merged segment has a _source field");
        }
        if (Integer.getInteger("gefrierschrank.shard.threads", 1) > 1) {
            assertTrue(counts[2] >= 2, "a forced intra-shard thread count reads the shard as ranges over forks: " + counts[2]);
        }
    }

    /**
     * {@code _source.enabled} can be switched off on an existing index, which leaves stored segments
     * next to segments without any source. This segment cannot be rebuilt from what it holds (its
     * fields need values Elasticsearch kept elsewhere), so the reconstruction refuses by type and the
     * shard is quarantined as a whole, leaving no row behind, not even its stored documents.
     */
    @Test
    void storedPlusDisabledSourceShardIsQuarantinedWithoutRows() throws Exception {
        JsonNode report = REPORTS.get(Variant.STORED_PLUS_DISABLED);
        assertEquals(1, report.path("quarantinedCount").asInt(), report.toString());
        assertEquals("UnreconstructableSourceException", report.path("shards").path(0).path("errorClass").asText(), report.toString());
        assertEquals(0, report.path("rowsWrittenThisRun").asLong());
        Path table = runs.resolve(Variant.STORED_PLUS_DISABLED.name()).resolve("table");
        if (Files.exists(table.resolve("_delta_log"))) {
            assertTrue(rows(table.toString()).isEmpty(), "a quarantined shard leaves no rows");
        }
        if (Files.exists(table)) {
            try (var files = Files.list(table)) {
                assertFalse(files.anyMatch(f -> f.toString().endsWith(".parquet")), "uncommitted Parquet files are deleted");
            }
        }
    }

    /** Runs in the child JVM: builds and exports every variant under {@code args[0]/<variant>}. */
    public static final class ChildMain {
        public static void main(String[] args) throws Exception {
            Path root = Path.of(args[0]);
            for (Variant variant : Variant.values()) {
                Path tmp = root.resolve(variant.name());
                Path mixed = mixedIndex(variant == Variant.MERGED, tmp);
                writeStoredSources(tmp.resolve("stored"), tmp.resolve("stored-sources.bin"));
                JsonNode metadata = synthMetadata();
                if (variant == Variant.STORED_PLUS_DISABLED) metadata = sourceDisabled(metadata);
                var opener = new SwappingOpener(mixed);
                RunReport report = export(tmp.resolve("table").toString(), metadata, opener);
                System.out.println("REPORT " + variant + " " + JSON.writeValueAsString(report));
                System.out.println("COUNTS " + variant + " " + opener.counts.storedSourceAbsentDocs + " "
                    + opener.counts.fastPathReadsOfSourcelessDocs + " " + opener.counts.forks);
            }
        }
    }

    private static RunReport export(String table, JsonNode metadata, SwappingOpener opener) throws Exception {
        var store = new FileSystemBlobStore(SYNTH_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot shard = inspector.shardSnapshot(SYNTH_INDEX, 0);
        DumpToDeltaPipeline.ShardCatalog catalog = new DumpToDeltaPipeline.ShardCatalog() {
            public int shardCount() { return 1; }
            public ShardSnapshot shardSnapshot(int shardId) { return shard; }
        };
        var context = new ArchiveContext(SYNTH_INDEX, "rid", "snap", "snap", SYNTH_INDEX, "snap");
        return new DumpToDeltaPipeline(inspector, store, table, new Configuration(), opener)
            .runSnapshot(context, metadata, catalog, RunReportListener.NO_OP);
    }

    private static JsonNode synthMetadata() throws IOException {
        return new RepositoryInspector(new FileSystemBlobStore(SYNTH_REPO)).indexMetadata(SYNTH_INDEX);
    }

    /** The synthetic index's metadata with its source mode setting removed and {@code _source.enabled: false}. */
    private static JsonNode sourceDisabled(JsonNode synthetic) {
        ObjectNode metadata = synthetic.deepCopy();
        ((ObjectNode) metadata.path("settings")).remove("index.mapping.source.mode");
        JsonNode mappings = metadata.path("mappings");
        if (mappings.isArray()) mappings = mappings.path(0);
        ObjectNode mapping = (ObjectNode) (mappings.has("_doc") ? mappings.path("_doc") : mappings);
        mapping.putObject("_source").put("enabled", false);
        return metadata;
    }

    /** The stored shard's segments first (docs 0-19), then the synthetic shard's (docs 20-69). */
    static Path mixedIndex(boolean merged, Path tmp) throws IOException {
        Path stored = materialize(FIXTURES.resolve("es93-repo"), STORED_INDEX, tmp.resolve("stored"));
        Path synth = materialize(SYNTH_REPO, SYNTH_INDEX, tmp.resolve("synth"));
        Path mixed = tmp.resolve("mixed");
        try (Directory target = new NIOFSDirectory(mixed);
             IndexWriter writer = new IndexWriter(target, new IndexWriterConfig()
                 .setMergePolicy(NoMergePolicy.INSTANCE).setSoftDeletesField("__soft_deletes"));
             Directory storedDir = new NIOFSDirectory(stored);
             Directory synthDir = new NIOFSDirectory(synth)) {
            if (!merged) {
                writer.addIndexes(storedDir, synthDir);
            } else {
                try (DirectoryReader storedReader = DirectoryReader.open(storedDir);
                     DirectoryReader synthReader = DirectoryReader.open(synthDir)) {
                    List<CodecReader> leaves = new ArrayList<>();
                    for (DirectoryReader reader : List.of(storedReader, synthReader)) {
                        for (var leaf : reader.leaves()) leaves.add(SlowCodecReaderWrapper.wrap(leaf.reader()));
                    }
                    writer.addIndexes(leaves.toArray(CodecReader[]::new));
                }
            }
            writer.commit();
        }
        Files.deleteIfExists(mixed.resolve("write.lock"));
        return mixed;
    }

    private static Path materialize(Path repo, String index, Path dir) throws IOException {
        var store = new FileSystemBlobStore(repo);
        Files.createDirectories(dir.getParent());
        new ShardMaterializer(store).materialize(new RepositoryInspector(store).shardSnapshot(index, 0), dir);
        return dir;
    }

    private static void writeStoredSources(Path shard, Path file) throws IOException {
        try (var reader = new Lucene10ShardReader(shard);
             var out = new DataOutputStream(Files.newOutputStream(file))) {
            for (int doc = 0; doc < reader.docCount(); doc++) {
                if (!reader.isLive(doc)) continue;
                byte[][] holder = new byte[1][];
                reader.readSource(doc, (bytes, length) -> holder[0] = java.util.Arrays.copyOf(bytes, length));
                out.writeUTF(reader.documentMetadata(doc).id());
                out.writeInt(holder[0].length);
                out.write(holder[0]);
            }
        }
    }

    private static Map<String, byte[]> readStoredSources(Path file) throws IOException {
        Map<String, byte[]> sources = new HashMap<>();
        try (var in = new DataInputStream(Files.newInputStream(file))) {
            while (true) {
                String id;
                try {
                    id = in.readUTF();
                } catch (EOFException end) {
                    return sources;
                }
                byte[] source = new byte[in.readInt()];
                in.readFully(source);
                assertNull(sources.put(id, source), "duplicate stored _id " + id);
            }
        }
    }

    private static List<String> golden() throws IOException {
        List<String> docs = new ArrayList<>();
        JSON.readTree(FIXTURES.resolve("synth-reconstructed-golden.json").toFile())
            .path("reconstructed").path("synth-repo/" + SYNTH_INDEX).forEach(doc -> docs.add(doc.asText()));
        return docs;
    }

    record Row(String id, String kind, String source) {}

    private static Map<Long, Row> rows(String table) {
        Map<Long, Row> rows = new TreeMap<>();
        try (var it = io.delta.standalone.DeltaLog.forTable(new Configuration(), table).update().open()) {
            while (it.hasNext()) {
                var row = it.next();
                assertNull(rows.put(row.getLong("_doc"),
                    new Row(row.getString("_id"), row.getString("_source_kind"), row.getString("_source"))),
                    "duplicate _doc " + row.getLong("_doc"));
            }
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return rows;
    }

    /** Replaces the materialized shard's files with the mixed index, then opens a counting reader over them. */
    private static final class SwappingOpener implements DumpToDeltaPipeline.ShardReaderOpener {
        private final Path mixed;
        final Counts counts = new Counts();

        SwappingOpener(Path mixed) {
            this.mixed = mixed;
        }

        @Override
        public ShardReader open(ShardSnapshot shard, Path shardDir) throws Exception {
            try (var files = Files.list(shardDir)) {
                for (Path file : files.toList()) Files.delete(file);
            }
            try (var files = Files.list(mixed)) {
                for (Path file : files.toList()) {
                    Files.copy(file, shardDir.resolve(file.getFileName()), StandardCopyOption.COPY_ATTRIBUTES);
                }
            }
            return new CountingReader(new Lucene10ShardReader(shardDir), counts);
        }
    }

    /** Documents whose segment has no _source field, and fast-path source reads of them, over every reader of a shard. */
    private static final class Counts {
        final AtomicInteger storedSourceAbsentDocs = new AtomicInteger();
        final AtomicInteger fastPathReadsOfSourcelessDocs = new AtomicInteger();
        final AtomicInteger forks = new AtomicInteger();
    }

    /** Counts into {@code counts}; a fork is a CountingReader over {@code delegate.fork()} with the same counts. */
    private static final class CountingReader implements ShardReader {
        private final ShardReader delegate;
        private final Counts counts;

        CountingReader(ShardReader delegate, Counts counts) {
            this.delegate = delegate;
            this.counts = counts;
        }

        public String runtimeName() { return delegate.runtimeName(); }
        public int docCount() throws IOException { return delegate.docCount(); }
        public boolean isLive(int docId) throws IOException { return delegate.isLive(docId); }
        public boolean storedSourceAbsent(int docId) throws IOException { return delegate.storedSourceAbsent(docId); }
        public DocumentMetadata documentMetadata(int docId) throws IOException {
            if (delegate.storedSourceAbsent(docId)) counts.storedSourceAbsentDocs.incrementAndGet();
            return delegate.documentMetadata(docId);
        }
        public void readSource(int docId, SourceConsumer consumer) throws IOException {
            if (delegate.storedSourceAbsent(docId)) counts.fastPathReadsOfSourcelessDocs.incrementAndGet();
            delegate.readSource(docId, consumer);
        }
        public ShardReader fork() throws IOException {
            ShardReader forked = delegate.fork();
            if (forked == null) return null;
            counts.forks.incrementAndGet();
            return new CountingReader(forked, counts);
        }
        public void close() throws IOException { delegate.close(); }
    }
}
