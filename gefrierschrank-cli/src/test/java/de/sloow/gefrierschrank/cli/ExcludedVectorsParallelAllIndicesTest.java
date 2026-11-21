package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code dump-to-delta --all-indices --parallelism 4} over the ES 9.3 fixture repository: every
 * index completes, the one whose dense vectors are excluded from {@code _source} with its vectors
 * put back, next to two synthetic-source ones and one that keeps its vectors in {@code _source}.
 *
 * <p>Runs the CLI in a child JVM laid out like the shaded jar: multi-release jars disabled, so
 * Elasticsearch's JDK 21+ vector classes (which cannot load outside a node) never do, and dnsjava
 * left out, as the jar leaves it out.
 */
class ExcludedVectorsParallelAllIndicesTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path ES93_REPO = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures/es93-repo");

    @Test
    void everyIndexCompletesAtParallelismFour(@TempDir Path tmp) throws Exception {
        Path out = tmp.resolve("delta");
        Path reports = tmp.resolve("reports");
        Path stdout = tmp.resolve("stdout.txt");
        Path stderr = tmp.resolve("stderr.txt");
        Process process = new ProcessBuilder(List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djdk.util.jar.enableMultiRelease=false",
                "-cp", String.join(File.pathSeparator, productionOrderedClassPath()),
                Main.class.getName(), "dump-to-delta", ES93_REPO.toAbsolutePath().toString(),
                "--all-indices", "--parallelism", "4", "-o", out.toString(), "--report", reports.toString()))
            .directory(Files.createDirectories(tmp.resolve("cwd")).toFile())
            .redirectOutput(stdout.toFile()).redirectError(stderr.toFile()).start();
        assertTrue(process.waitFor(5, TimeUnit.MINUTES));
        String output = Files.readString(stdout) + Files.readString(stderr);
        assertEquals(0, process.exitValue(), output);

        JsonNode manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        Map<String, String> outcomes = new TreeMap<>();
        for (JsonNode entry : manifest.path("indices")) {
            outcomes.put(entry.path("indexName").asText(), entry.path("outcome").asText()
                + (entry.path("errorClass").isTextual() ? ":" + entry.path("errorClass").asText() : ""));
        }
        assertEquals(Map.of(
            "test_es93_logsdb_binary", "COMPLETED",
            "test_es93_tsdb_hist", "COMPLETED",
            "test_es93_vectors_default", "COMPLETED",
            "test_es93_vectors_included", "COMPLETED"), outcomes, output);
        long rows = 0;
        try (var iterator = io.delta.standalone.DeltaLog.forTable(new org.apache.hadoop.conf.Configuration(),
                 out.resolve("test_es93_vectors_default").toString()).snapshot().open()) {
            while (iterator.hasNext()) {
                var row = iterator.next();
                assertEquals("stored+vectors", row.getString("_source_kind"));
                assertEquals(8, JSON.readTree(row.getString("_source")).path("embedding").size());
                rows++;
            }
        }
        assertEquals(20, rows);
    }

    /** This JVM's class path (expanding surefire's manifest-only jar), gefrierschrank-lucene10 first, dnsjava absent. */
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
        entries.stream().filter(e -> !e.contains("gefrierschrank-lucene10") && !e.contains("dnsjava")).forEach(ordered::add);
        return ordered;
    }
}
