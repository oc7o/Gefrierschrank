package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import de.sloow.gefrierschrank.syntheticsource.IsolatedSourceVectorsSession;
import de.sloow.gefrierschrank.syntheticsource.SourceVectors;
import de.sloow.gefrierschrank.syntheticsource.VectorRehydrationException;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs vector exports in a child JVM laid out like the shaded CLI jar (see
 * {@link Es93BinaryDocValuesPipelineTest}): only there do the ambient reader's codec stand-ins and
 * the isolated world's real ES 9.3 vector formats resolve as in production. Also reads the stored
 * {@code _source} bytes through {@link Lucene10ShardReader}, the export's own reader, and compares
 * values by their value-level form ({@link #canonical(String)}).
 */
final class VectorExportJvm {

    static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    static final ObjectMapper EXACT = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    /** Child JVM property: the first export run's cursors fail on their N-th load. */
    static final String FAIL_AFTER_LOADS = "gefrierschrank.test.vectors.failAfterLoads";

    private VectorExportJvm() {}

    /** One run's output: reports keyed {@code <index>#<run>} and stored sources keyed {@code <index>/<_id>}. */
    record Result(Map<String, JsonNode> reports, Map<String, byte[]> stored, String stdout, String stderr) {}

    /**
     * Exports {@code indices} of {@code repo} {@code runs} times each into {@code tables/<index>} at
     * the given parallelism, with {@code properties} as JVM properties of the child, and dumps the
     * stored sources of every live document.
     */
    static Result export(Path repo, Path tables, List<String> indices, int parallelism, int runs,
                         Map<String, String> properties) throws Exception {
        List<String> command = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-ea",
            "-Djdk.util.jar.enableMultiRelease=false"));
        properties.forEach((key, value) -> command.add("-D" + key + "=" + value));
        command.addAll(List.of("-cp", String.join(File.pathSeparator, Es93BinaryDocValuesPipelineTest.productionOrderedClassPath()),
            Main.class.getName(), repo.toAbsolutePath().toString(), tables.toString(), Integer.toString(parallelism),
            Integer.toString(runs)));
        command.addAll(indices);
        Files.createDirectories(tables);
        Path out = Files.createTempFile(tables, "child", ".out");
        Path err = Files.createTempFile(tables, "child", ".err");
        Process process = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile()).start();
        assertTrue(process.waitFor(10, TimeUnit.MINUTES));
        String stdout = Files.readString(out);
        String stderr = Files.readString(err);
        assertEquals(0, process.exitValue(), stdout + stderr);
        Map<String, JsonNode> reports = new HashMap<>();
        Map<String, byte[]> stored = new HashMap<>();
        for (String line : stdout.split("\n")) {
            String[] parts = line.split(" ", 3);
            if (parts[0].equals("REPORT")) reports.put(parts[1], EXACT.readTree(parts[2]));
            if (parts[0].equals("STORED")) stored.put(parts[1], Base64.getDecoder().decode(parts[2]));
        }
        return new Result(reports, stored, stdout, stderr);
    }

    record Row(String id, String source, String kind) {}

    /** Every row of the Delta table at {@code table}, by {@code _id} (duplicates fail). */
    static Map<String, Row> rows(Path table) throws IOException {
        Map<String, Row> rows = new TreeMap<>();
        try (var iterator = io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).snapshot().open()) {
            while (iterator.hasNext()) {
                var row = iterator.next();
                Row previous = rows.put(row.getString("_id"),
                    new Row(row.getString("_id"), row.getString("_source"), row.getString("_source_kind")));
                assertEquals(null, previous, "duplicate _id " + row.getString("_id"));
            }
        }
        return rows;
    }

    /** The AddFile tags of the table's current snapshot. */
    static List<Map<String, String>> fileTags(Path table) {
        List<Map<String, String>> tags = new ArrayList<>();
        for (var file : io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).update().getAllFiles()) {
            tags.add(file.getTags() == null ? Map.of() : file.getTags());
        }
        return tags;
    }

    /** {@code <fixture>.json} written one document per line: {@code "<id>": <raw bytes ES returned>}. */
    static Map<String, String> rawExpected(String file) throws IOException {
        Map<String, String> raw = new TreeMap<>();
        for (String line : Files.readAllLines(FIXTURES.resolve(file), StandardCharsets.UTF_8)) {
            if (line.equals("{") || line.equals("}")) continue;
            String entry = line.strip();
            if (entry.endsWith(",")) entry = entry.substring(0, entry.length() - 1);
            int colon = entry.indexOf("\": ");
            raw.put(EXACT.readValue(entry.substring(0, colon + 1), String.class), entry.substring(colon + 3));
        }
        return raw;
    }

    /** Value-level form: object members unordered, numbers as exact decimals. */
    static Object canonical(String json) throws IOException {
        return canonical(EXACT.readTree(json));
    }

    static Object canonical(JsonNode node) {
        if (node.isObject()) {
            Map<String, Object> members = new TreeMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                var field = fields.next();
                members.put(field.getKey(), canonical(field.getValue()));
            }
            return members;
        }
        if (node.isArray()) {
            List<Object> items = new ArrayList<>();
            node.forEach(item -> items.add(canonical(item)));
            return items;
        }
        if (node.isNumber()) return node.decimalValue().stripTrailingZeros().toPlainString();
        return node.isNull() ? "null" : node.toString();
    }

    /** Runs in the child JVM. */
    public static final class Main {
        public static void main(String[] args) throws Exception {
            Path repo = Path.of(args[0]);
            Path tables = Path.of(args[1]);
            int parallelism = Integer.parseInt(args[2]);
            int runs = Integer.parseInt(args[3]);
            List<String> indices = List.of(args).subList(4, args.length);
            int failAfter = Integer.getInteger(FAIL_AFTER_LOADS, -1);
            AtomicInteger loads = new AtomicInteger();
            boolean[] failing = {failAfter >= 0};
            if (failAfter >= 0) {
                SourceVectorsShardReader.sessions = (dir, index, metadata, fields) -> failingAfter(
                    SourceVectors.Rehydrator.open(dir, index, metadata, fields), () -> failing[0] && loads.incrementAndGet() > failAfter);
            }
            var store = new FileSystemBlobStore(repo);
            var sources = new RepositorySources(new RepositorySources.Source("primary", store));
            var exporter = new FullIndexExporter(sources, new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
            exporter.configure(new ExportSettings(SnapshotSelection.DEFAULT, parallelism, 0));
            ObjectMapper json = new ObjectMapper();
            for (String index : indices) {
                for (int run = 1; run <= runs; run++) {
                    RunReport report = exporter.run(index, tables.resolve(index).toString(), RunReportListener.NO_OP);
                    System.out.println("REPORT " + index + "#" + run + " " + json.writeValueAsString(report));
                    failing[0] = false;
                }
                printStored(store, index, tables);
            }
        }

        private static void printStored(FileSystemBlobStore store, String index, Path tables) throws Exception {
            var inspector = new RepositoryInspector(store);
            int shards = inspector.overview().indices().stream().filter(i -> i.name().equals(index)).findFirst()
                .orElseThrow().shardGenerations().size();
            for (int shard = 0; shard < shards; shard++) {
                Path dir = Files.createTempDirectory(tables, "stored");
                new ShardMaterializer(store).materialize(inspector.shardSnapshot(index, shard), dir);
                try (var reader = new Lucene10ShardReader(dir)) {
                    for (int doc = 0; doc < reader.docCount(); doc++) {
                        if (!reader.isLive(doc)) continue;
                        byte[][] bytes = new byte[1][];
                        reader.readSource(doc, (source, length) -> bytes[0] = java.util.Arrays.copyOf(source, length));
                        DocumentMetadata metadata = reader.documentMetadata(doc);
                        System.out.println("STORED " + index + "/" + metadata.id() + " "
                            + Base64.getEncoder().encodeToString(bytes[0]));
                    }
                }
            }
        }

        private static IsolatedSourceVectorsSession failingAfter(IsolatedSourceVectorsSession session,
                                                                 java.util.function.BooleanSupplier fail) {
            return new IsolatedSourceVectorsSession() {
                @Override public int docCount() { return session.docCount(); }
                @Override public void close() throws IOException { session.close(); }

                @Override
                public Cursor newCursor() {
                    Cursor cursor = session.newCursor();
                    return new Cursor() {
                        @Override
                        public int load(int globalDocId) throws IOException {
                            if (fail.getAsBoolean()) throw new VectorRehydrationException(new IllegalStateException("injected"));
                            return cursor.load(globalDocId);
                        }

                        @Override public boolean rootLeavesOnly() { return cursor.rootLeavesOnly(); }
                        @Override public String[] rootKeys() { return cursor.rootKeys(); }
                        @Override public byte[] rootMembers() throws IOException { return cursor.rootMembers(); }

                        @Override
                        public byte[] reserialize(byte[] stored, int length) throws IOException {
                            return cursor.reserialize(stored, length);
                        }
                    };
                }
            };
        }
    }
}
