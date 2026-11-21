package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ES 9.3 logsdb/TSDB shards whose BINARY doc values are ES819 with ZSTD blocks commit every row
 * through the real export path (1.0.0 quarantined every such shard with a NullPointerException),
 * the rows equal ES's synthetic {@code _search} output, and a rerun skips the committed shard.
 *
 * <p>The export runs in a child JVM laid out like the shaded CLI jar: gefrierschrank-lucene10 first
 * on the class path and multi-release jars disabled. On this module's own test class path
 * Elasticsearch's jar comes first, so Lucene's first-wins codec SPI would hand the ambient reader
 * ES's real codecs instead of gefrierschrank's read-only stand-ins, a layout the shaded CLI jar
 * never has.
 */
class Es93BinaryDocValuesPipelineTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final Map<String, String> EXPECTED = Map.of(
        "test_es93_logsdb_binary", "es93-logsdb-binary-expected.json",
        "test_es93_tsdb_hist", "es93-tsdb-hist-expected.json");

    @Test
    void everyShardCommitsWithEsSyntheticSourceAndResumes(@TempDir Path tmp) throws Exception {
        Map<String, JsonNode> reports = exportTwiceInChildJvm(tmp, EXPECTED.keySet());
        for (var entry : EXPECTED.entrySet()) {
            String index = entry.getKey();
            JsonNode expected = JSON.readTree(FIXTURES.resolve(entry.getValue()).toFile());
            JsonNode first = reports.get(index + "#1");
            assertEquals(1, first.path("committedCount").asInt(), first.toString());
            assertEquals(0, first.path("quarantinedCount").asInt(), first.toString());
            assertEquals(expected.size(), first.path("rowsWrittenThisRun").asLong());

            JsonNode rerun = reports.get(index + "#2");
            assertEquals("SKIPPED_ALREADY_COMMITTED", rerun.path("shards").path(0).path("outcome").asText(), rerun.toString());
            assertEquals(0, rerun.path("rowsWrittenThisRun").asLong());

            Map<String, JsonNode> expectedById = new HashMap<>();
            expected.fields().forEachRemaining(doc -> expectedById.put(doc.getKey(), doc.getValue()));
            Map<String, JsonNode> actual = new HashMap<>();
            try (var rows = io.delta.standalone.DeltaLog.forTable(new Configuration(), tmp.resolve(index).toString())
                     .snapshot().open()) {
                while (rows.hasNext()) {
                    var row = rows.next();
                    assertEquals("synthetic", row.getString("_source_kind"));
                    assertNull(actual.put(row.getString("_id"), JSON.readTree(row.getString("_source"))), "duplicate _id");
                }
            }
            assertEquals(expectedById, actual, index);
        }
    }

    /**
     * Exports each index of the es93 fixture twice into {@code tables/<index>} in a child JVM laid
     * out like the shaded CLI jar; returns the reports keyed {@code <index>#1} and {@code <index>#2}.
     */
    static Map<String, JsonNode> exportTwiceInChildJvm(Path tables, java.util.Collection<String> indices) throws Exception {
        List<String> command = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            // The shaded CLI jar is not Multi-Release, so ES's JDK 21+ class variants (e.g. simdvec's
            // NativeAccess-backed vector scorer) never load in the CLI; the separate jars would.
            "-Djdk.util.jar.enableMultiRelease=false",
            "-cp", String.join(File.pathSeparator, productionOrderedClassPath()),
            ExportMain.class.getName(), FIXTURES.resolve("es93-repo").toAbsolutePath().toString(), tables.toString()));
        command.addAll(indices);
        Path out = Files.createTempFile(tables, "child", ".out");
        Path err = Files.createTempFile(tables, "child", ".err");
        Process process = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile()).start();
        assertTrue(process.waitFor(5, TimeUnit.MINUTES));
        assertEquals(0, process.exitValue(), Files.readString(out) + Files.readString(err));
        Map<String, JsonNode> reports = new HashMap<>();
        for (String line : Files.readAllLines(out)) {
            if (line.startsWith("REPORT ")) {
                String[] parts = line.split(" ", 3);
                reports.put(parts[1], JSON.readTree(parts[2]));
            }
        }
        return reports;
    }

    /** Runs in the child JVM: exports each index twice and prints both reports. */
    public static final class ExportMain {
        public static void main(String[] args) throws Exception {
            var sources = new RepositorySources(
                new RepositorySources.Source("primary", new FileSystemBlobStore(Path.of(args[0]))));
            var exporter = new FullIndexExporter(sources, new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
            for (int i = 2; i < args.length; i++) {
                String table = Path.of(args[1]).resolve(args[i]).toString();
                for (int run = 1; run <= 2; run++) {
                    RunReport report = exporter.run(args[i], table, RunReportListener.NO_OP);
                    System.out.println("REPORT " + args[i] + "#" + run + " " + JSON.writeValueAsString(report));
                }
            }
        }
    }

    /**
     * This JVM's class path (expanding surefire's manifest-only jar) arranged like the shaded CLI
     * jar: gefrierschrank-lucene10 first, dnsjava absent.
     */
    static List<String> productionOrderedClassPath() throws IOException {
        List<String> entries = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            entries.add(entry);
            Path path = Path.of(entry);
            if (entry.endsWith(".jar") && Files.isRegularFile(path)) {
                try (JarFile jar = new JarFile(path.toFile())) {
                    var manifest = jar.getManifest();
                    String classPath = manifest == null ? null : manifest.getMainAttributes().getValue("Class-Path");
                    if (classPath != null) {
                        URI base = path.toAbsolutePath().getParent().toUri();
                        for (String reference : classPath.trim().split("\\s+")) {
                            entries.add(Path.of(base.resolve(reference)).toString());
                        }
                    }
                }
            }
        }
        List<String> ordered = new ArrayList<>();
        entries.stream().filter(e -> e.contains("gefrierschrank-lucene10")).forEach(ordered::add);
        // The CLI keeps dnsjava out of its jar (provided scope): its resolver SPI class exists only
        // as a multi-release variant.
        entries.stream().filter(e -> !e.contains("gefrierschrank-lucene10") && !e.contains("dnsjava"))
            .forEach(ordered::add);
        assertTrue(ordered.get(0).contains("gefrierschrank-lucene10"), ordered.toString());
        return ordered;
    }
}
