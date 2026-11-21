package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A host where zstd-jni cannot load its native library (noexec {@code java.io.tmpdir}, read-only
 * image) must still export ZSTD segments, through the pure-Java decoder, instead of dying with an
 * {@link UnsatisfiedLinkError} on the first ZSTD block. A missing {@code -DZstdNativePath} file
 * reproduces that failure deterministically in a child JVM.
 */
class ZstdNativeUnavailableExportTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path ZSTD_REPO =
        Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures/zstd-synth-repo");
    private static final String WARNING = "zstd-jni native library unavailable";

    @Test
    void dumpToDeltaExportsZstdSegmentsWhenTheNativeLibraryCannotLoad(@TempDir Path tmp) throws Exception {
        Path out = tmp.resolve("delta");
        Path reports = tmp.resolve("reports");
        Path workDir = Files.createDirectories(tmp.resolve("cwd"));
        List<String> command = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-DZstdNativePath=" + tmp.resolve("missing-libzstd-jni.so"),
            "-cp", System.getProperty("java.class.path"),
            Main.class.getName(), "dump-to-delta", ZSTD_REPO.toAbsolutePath().toString(),
            "--all-indices", "-o", out.toString(), "--report", reports.toString()));
        Path stdout = tmp.resolve("stdout.txt");
        Path stderr = tmp.resolve("stderr.txt");
        Process process = new ProcessBuilder(command).directory(workDir.toFile())
            .redirectOutput(stdout.toFile()).redirectError(stderr.toFile()).start();
        assertTrue(process.waitFor(5, TimeUnit.MINUTES), "export did not finish");
        String err = Files.readString(stderr);

        assertEquals(0, process.exitValue(), Files.readString(stdout) + err);
        assertEquals(1, err.lines().filter(line -> line.startsWith(WARNING)).count(), err);
        assertTrue(err.contains(WARNING + " (UnsatisfiedLinkError)"), err);
        assertFalse(err.contains("Exception in thread"), err);

        JsonNode manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        assertEquals(2, manifest.path("completedCount").asInt(), manifest.toString());
        assertEquals(0, manifest.path("failedCount").asInt(), manifest.toString());
        Map<String, Integer> expectedRows = Map.of("test_zstd_synth", 40, "test_zstd_logsdb", 20);
        for (JsonNode entry : manifest.path("indices")) {
            assertEquals("COMPLETED", entry.path("outcome").asText(), manifest.toString());
            String index = entry.path("indexName").asText();
            JsonNode report = JSON.readTree(reports.resolve(index + ".json").toFile());
            assertEquals(expectedRows.get(index).longValue(), report.path("rowsWrittenThisRun").asLong(), report.toString());
            assertEquals(0, report.path("quarantinedCount").asInt(), report.toString());
        }
    }
}
