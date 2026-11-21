package de.sloow.gefrierschrank.cli;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static de.sloow.gefrierschrank.cli.MountedShellsExportTest.captured;
import static de.sloow.gefrierschrank.cli.MountedShellsExportTest.dumpToDelta;
import static de.sloow.gefrierschrank.cli.MountedShellsExportTest.entry;
import static de.sloow.gefrierschrank.cli.MountedShellsExportTest.manifest;
import static de.sloow.gefrierschrank.cli.MountedShellsExportTest.tables;
import static org.junit.jupiter.api.Assertions.*;

/**
 * --mounted-shells skip-if-original on retired-shell-repo (ES 9.3): logs-a is retained only in its
 * ILM snapshot ilm-backing, which the shell partial-logs-a mounts.
 */
class MountedShellsRetiredTest {

    static final Path BEFORE = RetiredShellResumeExportTest.BEFORE;
    static final Path AFTER = RetiredShellResumeExportTest.AFTER;
    static final String SHELL = RetiredShellResumeExportTest.SHELL;

    @Test
    void theShellIsAnAliasOfTheOriginalInItsIlmSnapshot(@TempDir Path tmp) throws Exception {
        for (Path repository : List.of(BEFORE, AFTER)) {
            Path root = tmp.resolve(repository.getFileName() + "-root");
            Path reports = tmp.resolve(repository.getFileName() + "-reports");
            assertEquals(0, dumpToDelta(repository, "--all-indices", "-o", root.toString(), "--report", reports.toString(),
                "--mounted-shells", "skip-if-original"), repository.toString());
            assertEquals(List.of("logs-a"), tables(root));
            var manifest = manifest(reports);
            var alias = entry(manifest, SHELL);
            assertEquals("ALIAS_OF", alias.path("outcome").asText());
            assertEquals("logs-a", alias.path("aliasOf").asText());
            assertEquals(1, manifest.path("aliasCount").asInt());
            assertTrue(manifest.path("complete").asBoolean());
        }
    }

    /** The retired-shell table of a default run is completed exactly as RetiredShellResumeExportTest has it. */
    @Test
    void aRetiredShellsTableIsCompletedNotAliased(@TempDir Path tmp) throws Exception {
        Path root = tmp.resolve("root");
        assertEquals(0, dumpToDelta(BEFORE, "--all-indices", "-o", root.toString()));
        Map<String, Long> before = RetiredShellResumeExportTest.files(root);

        Path reports = tmp.resolve("reports");
        var out = new ByteArrayOutputStream();
        assertEquals(0, captured(out, () -> dumpToDelta(AFTER, "--all-indices", "-o", root.toString(), "--report",
            reports.toString(), "--mounted-shells", "skip-if-original")), out.toString());

        assertEquals(before, RetiredShellResumeExportTest.files(root), "nothing written, the lock files aside");
        var manifest = manifest(reports);
        assertEquals("COMPLETED", entry(manifest, SHELL).path("outcome").asText());
        assertEquals(0, manifest.path("aliasCount").asInt());
        assertEquals(1, manifest.path("behindLatestCount").asInt());
        assertTrue(manifest.path("complete").asBoolean());
        assertTrue(out.toString().contains("Mounted shell " + SHELL + " already has a table; exporting it"), out.toString());
        var report = new com.fasterxml.jackson.databind.ObjectMapper().readTree(reports.resolve(SHELL + ".json").toFile());
        assertEquals(2, report.path("skippedCount").asInt());
        assertEquals(RetiredShellResumeExportTest.SLM_1, report.path("snapshot").path("snapshotUuid").asText());
        assertTrue(Files.isDirectory(root.resolve(SHELL)));
    }
}
