package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.delta.ExportPreflight;
import de.sloow.gefrierschrank.delta.SourceVectorsPlan;
import de.sloow.gefrierschrank.delta.TableWriteLock;
import de.sloow.gefrierschrank.delta.TargetStorageException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The line and manifest entry of an index that failed outright: a refusal's fixed message in full,
 * as {@code dump} prints it, each class named once, and any other message never.
 */
class FailedIndexMessageTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void fixedMessagesAppearOnceAndOtherMessagesNever(@TempDir Path reports) throws Exception {
        Map<String, Exception> failures = new LinkedHashMap<>();
        failures.put("old", new ExportPreflight.IndexCreatedTooOldException());
        failures.put("nosource", new IOException("preflight", new ExportPreflight.SourceDisabledException()));
        failures.put("vectors", new SourceVectorsPlan.ExcludedSourceVectorsException());
        failures.put("vectorless", new de.sloow.gefrierschrank.delta.VectorlessTableException());
        failures.put("locked", new TableWriteLock.TableLockedException());
        failures.put("other", new IOException("No such file: /secret/table"));
        // Run-fatal, so last: no index starts after it.
        failures.put("full", new TargetStorageException("no space left, FSError", null));

        var out = new ByteArrayOutputStream();
        PrintStream previousOut = System.out;
        PrintStream previousErr = System.err;
        System.setOut(new PrintStream(out, true));
        System.setErr(new PrintStream(new ByteArrayOutputStream(), true));
        int exit;
        try {
            exit = new DumpToDeltaCommand().runAllIndices(List.copyOf(failures.keySet()), name -> {
                throw failures.get(name);
            }, reports, 1, (name, report) -> {});
        } finally {
            System.setOut(previousOut);
            System.setErr(previousErr);
        }
        assertEquals(1, exit);

        List<String> lines = out.toString().lines().filter(l -> l.contains("failed outright: ")).toList();
        JsonNode manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        assertEquals(failures.size(), manifest.path("indices").size());
        for (JsonNode entry : manifest.path("indices")) {
            String name = entry.path("indexName").asText();
            String message = entry.path("errorMessage").asText();
            if (name.equals("other")) {
                assertEquals("IOException", entry.path("errorClass").asText());
                assertTrue(message.startsWith("java.io.IOException at "), message);
                continue;
            }
            Throwable refusal = failures.get(name).getCause() == null ? failures.get(name) : failures.get(name).getCause();
            String expected = refusal.getClass().getSimpleName() + ": " + refusal.getMessage();
            assertEquals(expected, message, name);
            assertTrue(lines.contains("Index " + name + " failed outright: " + expected), String.join("\n", lines));
        }
        assertFalse(out.toString().contains("secret"), out.toString());
        assertFalse(manifest.toString().contains("secret"));
    }
}
