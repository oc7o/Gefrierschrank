package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.delta.RunReport;
import de.sloow.gefrierschrank.delta.TargetStorageException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A full or failing target disk ends the whole --all-indices run with a truthful manifest,
 * instead of letting every later index download its shards only to fail the same way.
 */
@Timeout(60)
class TargetStorageRunFatalTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String DISK_FULL = "target storage failure (no space left: FSError)";

    private static RunReport completed(String name) {
        return new RunReport(name, "t0", "t1", 5, 1, 1, 0, 0, 10, 100, List.of());
    }

    private static TargetStorageException diskFull() {
        return new TargetStorageException("no space left: FSError",
            new IOException("No space left on device: /secret/table/part-0.parquet"));
    }

    @ParameterizedTest(name = "concurrency {0}")
    @ValueSource(ints = {1, 2})
    void aTargetStorageFailureFailsItsIndexStartsNoOtherAndExitsOne(int concurrency, @TempDir Path reports)
            throws Exception {
        var names = List.of("a", "b", "c", "d");
        var processed = Collections.synchronizedList(new ArrayList<String>());
        var err = new ByteArrayOutputStream();
        var out = new ByteArrayOutputStream();
        int exit = captured(out, err, () -> new DumpToDeltaCommand().runAllIndices(names, name -> {
            processed.add(name);
            if (name.equals("b")) throw diskFull();
            // At concurrency 2, 'a' is still in flight when 'b' fails: it must finish and be recorded.
            if (name.equals("a") && concurrency > 1) awaitFailedInManifest(reports, "b");
            return completed(name);
        }, reports, concurrency, (name, report) -> {}));

        assertEquals(1, exit);
        assertEquals(List.of("a", "b"), processed.stream().sorted().toList(), "no index starts after the failure");
        var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        assertEquals(List.of("a", "b"), IndexConcurrencyTest.indexNames(manifest));
        assertEquals("COMPLETED", manifest.path("indices").path(0).path("outcome").asText());
        var failed = manifest.path("indices").path(1);
        assertEquals("FAILED", failed.path("outcome").asText());
        assertEquals("TargetStorageException", failed.path("errorClass").asText());
        assertEquals("TargetStorageException: " + DISK_FULL, failed.path("errorMessage").asText());
        assertEquals(List.of("c", "d"), IndexConcurrencyTest.pending(manifest));
        assertFalse(manifest.path("complete").asBoolean());
        assertFalse(manifest.toString().contains("secret"), "the cause's message stays out of the manifest");

        var errorLines = err.toString().lines().filter(l -> l.startsWith("error:")).toList();
        assertEquals(List.of("error: TargetStorageException: " + DISK_FULL), errorLines,
            "the same error line as a single --index run prints");
        assertFalse(out.toString().contains("secret"));
    }

    @Test
    void severalIndicesFailingOnTheTargetPrintOneErrorLine(@TempDir Path reports) throws Exception {
        var err = new ByteArrayOutputStream();
        int exit = captured(new ByteArrayOutputStream(), err, () -> new DumpToDeltaCommand().runAllIndices(
            List.of("a", "b", "c"), name -> {
                if (name.equals("a")) awaitFailedInManifest(reports, "b");
                if (name.equals("c")) return completed(name);
                // Wrapped the way a failure can reach the index loop from a shard thread.
                throw new IllegalStateException("shard failed", diskFull());
            }, reports, 2, (name, report) -> {}));

        assertEquals(1, exit);
        var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        assertEquals(2, manifest.path("failedCount").asInt());
        manifest.path("indices").forEach(entry -> assertEquals("TargetStorageException", entry.path("errorClass").asText()));
        assertEquals(List.of("c"), IndexConcurrencyTest.pending(manifest));
        assertEquals(1, err.toString().lines().filter(l -> l.startsWith("error:")).count(), err.toString());
    }

    /**
     * The pipeline records a second index-fatal shard failure as suppressed by the first: a table
     * conflict found by one shard must not hide the full disk another shard of the index found.
     */
    @Test
    void aTargetStorageFailureSuppressedByAnotherShardsFailureStillEndsTheRun(@TempDir Path reports) throws Exception {
        var processed = Collections.synchronizedList(new ArrayList<String>());
        var err = new ByteArrayOutputStream();
        int exit = captured(new ByteArrayOutputStream(), err, () -> new DumpToDeltaCommand().runAllIndices(
            List.of("a", "b", "c"), name -> {
                processed.add(name);
                if (name.equals("a")) {
                    var conflict = new IOException("another shard's failure");
                    conflict.addSuppressed(diskFull());
                    throw conflict;
                }
                return completed(name);
            }, reports, 1, (name, report) -> {}));

        assertEquals(1, exit);
        assertEquals(List.of("a"), processed);
        var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        assertEquals("TargetStorageException", manifest.path("indices").path(0).path("errorClass").asText());
        assertEquals(List.of("b", "c"), IndexConcurrencyTest.pending(manifest));
        assertEquals(List.of("error: TargetStorageException: " + DISK_FULL),
            err.toString().lines().filter(l -> l.startsWith("error:")).toList());
    }

    @Test
    void otherFailuresKeepTheirExitCodeAndLetTheRunContinue(@TempDir Path reports) throws Exception {
        int exit = new DumpToDeltaCommand().runAllIndices(List.of("a", "b", "c"), name -> {
            if (name.equals("b")) throw new IOException("unrelated", new IOException("No space left on device"));
            return completed(name);
        }, reports);
        assertEquals(4, exit);
        assertEquals(List.of(), IndexConcurrencyTest.pending(JSON.readTree(reports.resolve("manifest.json").toFile())));
    }

    /** TargetStorageException.java must stay byte for byte the contract text below. */
    @Test
    void theExceptionClassIsExactlyTheSharedContractText() throws IOException {
        String expected = """
            package de.sloow.gefrierschrank.delta;

            import java.io.IOException;

            /**
             * The Delta target could not store data: disk full, file too large, quota or I/O error. Fatal for
             * the whole run, because every later shard would be downloaded in full only to fail the same way.
             */
            public final class TargetStorageException extends IOException {
                public TargetStorageException(String reason, Throwable cause) {
                    super("target storage failure (" + reason + ")", cause);
                }
            }
            """;
        assertEquals(expected, Files.readString(Path.of(
            "../gefrierschrank-delta/src/main/java/de/sloow/gefrierschrank/delta/TargetStorageException.java")));
    }

    private static void awaitFailedInManifest(Path reports, String index) throws Exception {
        for (int i = 0; i < 3000; i++) {
            JsonNode manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
            for (JsonNode entry : manifest.path("indices"))
                if (entry.path("indexName").asText().equals(index) && entry.path("outcome").asText().equals("FAILED")) return;
            Thread.sleep(10);
        }
        fail(index + " never recorded as FAILED");
    }

    private static int captured(ByteArrayOutputStream out, ByteArrayOutputStream err, Callable<Integer> body)
            throws Exception {
        PrintStream previousOut = System.out;
        PrintStream previousErr = System.err;
        System.setOut(new PrintStream(out, true));
        System.setErr(new PrintStream(err, true));
        try {
            return body.call();
        } finally {
            System.setOut(previousOut);
            System.setErr(previousErr);
        }
    }
}
