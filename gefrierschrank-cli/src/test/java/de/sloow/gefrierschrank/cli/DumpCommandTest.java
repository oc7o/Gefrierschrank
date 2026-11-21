package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.delta.ExportPreflight;
import de.sloow.gefrierschrank.delta.SourceVectorsPlan;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DumpCommandTest {

    static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void dumpsEs7ShardInProcessByteExactOneLinePerDocument(@TempDir Path tmp) throws Exception {
        Path output = tmp.resolve("out.jsonl");
        assertEquals(0, dump(Es7InProcessExportTest.FIXTURES.resolve("demo-repo"), "test_data_7_17_9", output));
        List<String> lines = Files.readAllLines(output, StandardCharsets.UTF_8);
        var golden = Es7InProcessExportTest.Golden.load("demo-repo", "test_data_7_17_9");
        assertEquals(500, lines.size());
        for (int i = 0; i < lines.size(); i++) {
            assertEquals("Product", JSON.readTree(lines.get(i)).path("title").asText().substring(0, 7));
            assertEquals(golden.get(i).sha256(), Es7InProcessExportTest.Golden.sha256(lines.get(i).getBytes(StandardCharsets.UTF_8)),
                "line " + i + " is the stored source, byte-exact");
        }
    }

    @Test
    void es7SourceWithCrLfStaysOnOneLine(@TempDir Path tmp) throws Exception {
        Path output = tmp.resolve("out.jsonl");
        assertEquals(0, dump(Es7InProcessExportTest.FIXTURES.resolve("es7-features-repo"), "es7_features", output));
        List<String> lines = Files.readAllLines(output, StandardCharsets.UTF_8);
        assertEquals(61, lines.size());
        JsonNode search = JSON.readTree(Es7InProcessExportTest.FIXTURES.resolve("es7-golden-rows/es7-features-search.json").toFile())
            .path("es7_features");
        JsonNode pretty = null;
        for (JsonNode hit : search) if (hit.path("_id").asText().equals("pretty")) pretty = hit.path("_source");
        int prettyLines = 0;
        for (String line : lines) {
            JsonNode parsed = JSON.readTree(line);
            if (parsed.equals(pretty)) prettyLines++;
        }
        assertEquals(1, prettyLines, "the CR/LF pretty-printed source is one line parsing to the same tree");
    }

    @Test
    void prettyPrintedSourceWithRawCrLfIsOneLineAndLeavesTheReaderArrayAlone(@TempDir Path tmp) throws Exception {
        var sources = List.of("{\"a\":1}", "{\r\n  \"b\" : [1,\n 2],\r\n  \"c\": \"x\\ny\"\r\n}", "{\n\"ü\":\"€\"}\n");
        try (var dir = new NIOFSDirectory(tmp); var writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (String source : sources) {
                var doc = new Document();
                doc.add(new StoredField("_source", new BytesRef(source.getBytes(StandardCharsets.UTF_8))));
                writer.addDocument(doc);
            }
        }
        var out = new ByteArrayOutputStream();
        try (var reader = new Lucene10ShardReader(tmp)) {
            assertEquals(3, DumpCommand.writeLines(reader, out));
        }
        String[] lines = out.toString(StandardCharsets.UTF_8).split("\n", -1);
        assertEquals(4, lines.length, "three lines plus the empty remainder after the final newline");
        for (int i = 0; i < sources.size(); i++) {
            assertFalse(lines[i].contains("\r"));
            assertEquals(JSON.readTree(sources.get(i)), JSON.readTree(lines[i]));
        }
        byte[] raw = sources.get(1).getBytes(StandardCharsets.UTF_8);
        byte[] copy = raw.clone();
        byte[] replaced = DumpCommand.singleLine(raw, raw.length);
        assertArrayEquals(copy, raw, "the consumer-owned array is not modified in place");
        byte[] clean = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
        assertSame(clean, DumpCommand.singleLine(clean, clean.length), "no copy without CR/LF");
        assertEquals(raw.length, replaced.length);
    }

    /** dump ignored the 1.1.0 worker opt-out; since the worker's removal it refuses it like dump-to-delta. */
    @Test
    void workerOptOutAndTyposFailBeforeAnyShardIsRead(@TempDir Path tmp) throws Exception {
        Path output = tmp.resolve("out.jsonl");
        var store = new RecordingBlobStore(new FileSystemBlobStore(Es7InProcessExportTest.FIXTURES.resolve("demo-repo")));
        var expected = java.util.Map.of("worker", "the Lucene 9 worker was removed in 1.2.0",
            "Worker", LuceneRuntimeSelector.INVALID_RUNTIME);
        for (var entry : expected.entrySet()) {
            for (String index : List.of("test_data_7_17_9", "test_data_9_3_0")) {
                var run = CommandRuns.captureStderr(() -> Es7InProcessExportTest.withProperties(
                    new String[]{LuceneRuntimeSelector.LUCENE8_RUNTIME_PROPERTY, entry.getKey()},
                    () -> dump(store, index, output)));
                assertEquals(2, run.exit(), entry.getKey() + " " + index);
                assertTrue(run.stderr().lines().anyMatch(entry.getValue()::equals), run.stderr());
            }
        }
        assertFalse(Files.exists(output));
        assertEquals(List.of(), store.touched, "the repository is not even opened");
    }

    /**
     * ES 9.2+ keeps dense vectors out of the stored _source by default. dump used to write
     * such rows without their embeddings and exit 0, 1.1.0 refused the index; now every line
     * carries its vectors, put back by Elasticsearch's loaders. Runs in a child JVM laid out like
     * the shaded CLI jar (no multi-release jars, the class path expanded so the isolated world finds
     * Elasticsearch's jars).
     */
    @Test
    void excludedVectorsAreDumpedWithTheirVectors(@TempDir Path tmp) throws Exception {
        Path output = tmp.resolve("out.jsonl");
        Path log = tmp.resolve("child.log");
        Process child = new ProcessBuilder(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djdk.util.jar.enableMultiRelease=false", "-cp",
                String.join(java.io.File.pathSeparator, ExcludedVectorsParallelAllIndicesTest.productionOrderedClassPath()),
                Main.class.getName(), "dump", ES93_REPO.toAbsolutePath().toString(), "--index", "test_es93_vectors_default",
                "-o", output.toString()))
            .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        assertTrue(child.waitFor(3, java.util.concurrent.TimeUnit.MINUTES), "child JVM did not finish");
        assertEquals(0, child.exitValue(), Files.readString(log));
        JsonNode expected = JSON.readTree(ES93_REPO.resolveSibling("es93-vectors-default-expected.json").toFile());
        List<JsonNode> expectedSources = new ArrayList<>();
        expected.forEach(expectedSources::add);
        List<JsonNode> actual = new ArrayList<>();
        for (String line : Files.readAllLines(output, StandardCharsets.UTF_8)) {
            JsonNode source = JSON.readTree(line);
            assertEquals(8, source.path("embedding").size(), line);
            actual.add(source);
        }
        assertEquals(20, actual.size());
        assertEquals(DumpPlanTest.sorted(expectedSources), DumpPlanTest.sorted(actual));
    }

    /**
     * dump runs dump-to-delta's whole export preflight: an ES 7 index that never stored
     * _source fails before any shard data is read, with the preflight's fixed message instead of
     * a reconstruction failure after the download.
     */
    @Test
    void sourceDisabledIndexFailsBeforeAnyShardIsRead(@TempDir Path tmp) throws Exception {
        Path output = Files.writeString(tmp.resolve("out.jsonl"), "previous export\n");
        var store = new RecordingBlobStore(new FileSystemBlobStore(Es7InProcessExportTest.FIXTURES.resolve("es7-features-repo")),
            key -> key.contains("/__"), key -> {});
        var run = CommandRuns.captureStderr(() -> dump(store, "es7_no_source", output));
        assertFalse(store.touched.isEmpty());
        assertTrue(store.touched.stream().noneMatch(key -> key.contains("/__")), "no shard data blob may be read: " + store.touched);
        assertEquals(1, run.exit());
        String expectedError = "error: SourceDisabledException: " + new ExportPreflight.SourceDisabledException().getMessage();
        assertTrue(run.stderr().lines().anyMatch(expectedError::equals), run.stderr());
        assertFalse(run.stderr().contains("\tat "), "no stack trace: " + run.stderr());
        assertEquals("previous export\n", Files.readString(output), "the existing output is unchanged");
        assertEquals(List.of(), DumpPlanTest.partialFiles(tmp));
    }

    @Test
    void everyPreflightRefusalPrintsItsFixedMessage() {
        for (Exception refusal : List.of(new ExportPreflight.IndexCreatedTooOldException(),
                new ExportPreflight.SourceDisabledException(), new SourceVectorsPlan.ExcludedSourceVectorsException())) {
            assertEquals(refusal.getClass().getSimpleName() + ": " + refusal.getMessage(),
                DumpCommand.safeFailure(new IOException("wrapped", refusal)));
        }
    }

    /**
     * Runs in a child JVM without multi-release jars, like the shaded CLI jar: on this class path
     * Elasticsearch's JDK 21+ vector format variants win Lucene's ambient codec SPI and cannot load.
     */
    @Test
    void indexThatKeepsVectorsInSourceDumpsThem(@TempDir Path tmp) throws Exception {
        Path output = tmp.resolve("out.jsonl");
        var child = CommandRuns.runChildJvm(tmp, java.util.Map.of(), List.of("-Djdk.util.jar.enableMultiRelease=false"),
            Main.class.getName(), "dump", ES93_REPO.toAbsolutePath().toString(), "--index", "test_es93_vectors_included",
            "-o", output.toString());
        assertEquals(0, child.exit(), child.output());
        JsonNode expected = JSON.readTree(ES93_REPO.resolveSibling("es93-vectors-included-expected.json").toFile());
        List<JsonNode> expectedSources = new ArrayList<>();
        expected.forEach(expectedSources::add);
        List<JsonNode> actual = new ArrayList<>();
        for (String line : Files.readAllLines(output, StandardCharsets.UTF_8)) {
            JsonNode source = JSON.readTree(line);
            assertEquals(8, source.path("embedding").size(), line);
            actual.add(source);
        }
        assertEquals(DumpPlanTest.sorted(expectedSources), DumpPlanTest.sorted(actual));
    }

    static final Path ES93_REPO = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures/es93-repo");

    private static int dump(Path repo, String index, Path output) {
        return new picocli.CommandLine(new DumpCommand()).execute(repo.toString(), "--index", index, "-o", output.toString());
    }

    static int dump(BlobStore store, String index, Path output, String... options) {
        var command = new DumpCommand();
        command.stores = (uri, endpoint, prefix) -> store;
        var args = new ArrayList<>(List.of("ignored-repository-uri", "--index", index, "-o", output.toString()));
        args.addAll(List.of(options));
        return new picocli.CommandLine(command).execute(args.toArray(String[]::new));
    }

    /** Records every key whose content or metadata is requested; can fail reads of chosen keys. */
    static final class RecordingBlobStore implements BlobStore {
        final List<String> touched = new CopyOnWriteArrayList<>();
        private final BlobStore delegate;
        private final Predicate<String> failRead;
        private final Consumer<String> onRead;

        RecordingBlobStore(BlobStore delegate) {
            this(delegate, key -> false, key -> {});
        }

        RecordingBlobStore(BlobStore delegate, Predicate<String> failRead, Consumer<String> onRead) {
            this.delegate = delegate;
            this.failRead = failRead;
            this.onRead = onRead;
        }

        @Override
        public Optional<BlobMetadata> stat(String key) {
            touched.add(key);
            return delegate.stat(key);
        }

        @Override
        public InputStream read(String key) throws IOException {
            touched.add(key);
            onRead.accept(key);
            if (failRead.test(key)) throw new IOException("injected read failure");
            return delegate.read(key);
        }

        @Override
        public List<String> list(String prefix) {
            return delegate.list(prefix);
        }

        @Override
        public List<BlobMetadata> listWithSizes(String prefix) {
            return delegate.listWithSizes(prefix);
        }

        @Override
        public void put(String key, byte[] content) {
            throw new UnsupportedOperationException("the source repository is read-only");
        }
    }
}
