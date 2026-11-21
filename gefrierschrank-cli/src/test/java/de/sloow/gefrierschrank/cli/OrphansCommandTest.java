package de.sloow.gefrierschrank.cli;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end coverage of the `orphans` command against repository fixtures - both the
 * fully-clean baseline fixture (demo-repo) and a copy deliberately polluted with one leftover
 * blob to prove genuine orphans are actually flagged, not just that "zero" is always printed.
 */
class OrphansCommandTest {

    private static final Path DEMO_REPO = Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");
    private static final Path REUSED_INDEX_REPO =
        Path.of("../gefrierschrank-core/src/test/resources/fixtures/reused-index-repo");

    private ByteArrayOutputStream stdout;
    private ByteArrayOutputStream stderr;
    private PrintStream originalOut;
    private PrintStream originalErr;

    @BeforeEach
    void captureStreams() {
        stdout = new ByteArrayOutputStream();
        stderr = new ByteArrayOutputStream();
        originalOut = System.out;
        originalErr = System.err;
        System.setOut(new PrintStream(stdout, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    void restoreStreams() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    @Test
    void reportsZeroOrphansAgainstAFullyReachableRepository() {
        int exitCode = new CommandLine(new OrphansCommand()).execute(DEMO_REPO.toString());

        assertEquals(0, exitCode);
        String out = stdout.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("Orphaned (unreferenced) objects : 0"), out);
        assertTrue(out.contains("No orphaned blobs found"), out);
    }

    @Test
    void reportsZeroOrphansWhenBothIncarnationsOfAReusedIndexAreStillRetained() {
        int exitCode = new CommandLine(new OrphansCommand()).execute(REUSED_INDEX_REPO.toString());

        assertEquals(0, exitCode);
        String out = stdout.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("Orphaned (unreferenced) objects : 0"), out);
    }

    @Test
    void flagsAGenuinelyUnreferencedBlobAsAnOrphan(@TempDir Path tmp) throws IOException {
        copyDirectory(DEMO_REPO, tmp);
        Path garbageDir = tmp.resolve("indices/rCI54kLnSKCo4USf20Y76Q/0");
        Files.createDirectories(garbageDir);
        Path garbage = garbageDir.resolve("__leftover_from_interrupted_delete");
        Files.write(garbage, new byte[50_000]);

        int exitCode = new CommandLine(new OrphansCommand()).execute(tmp.toString());

        assertEquals(0, exitCode, "orphans is read-only/diagnostic: finding orphans is not a failure exit code");
        String out = stdout.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("Orphaned (unreferenced) objects : 1"), out);
        assertTrue(out.contains("indices/rCI54kLnSKCo4USf20Y76Q/0/__leftover_from_interrupted_delete"), out);
        assertTrue(out.contains("_cleanup"), "should explain the likely cause (ES's own cleanup API), got: " + out);
    }

    @Test
    void csvModeListsOrphanedKeysWithSizes(@TempDir Path tmp) throws IOException {
        copyDirectory(DEMO_REPO, tmp);
        Path garbageDir = tmp.resolve("indices/rCI54kLnSKCo4USf20Y76Q/0");
        Files.createDirectories(garbageDir);
        Files.write(garbageDir.resolve("__orphan"), new byte[1234]);

        int exitCode = new CommandLine(new OrphansCommand()).execute(tmp.toString(), "--csv");

        assertEquals(0, exitCode);
        String out = stdout.toString(StandardCharsets.UTF_8);
        assertTrue(out.contains("key,bytes"), out);
        assertTrue(out.contains("indices/rCI54kLnSKCo4USf20Y76Q/0/__orphan,1234"), out);
    }

    private static void copyDirectory(Path source, Path target) throws IOException {
        try (var walk = Files.walk(source)) {
            for (Path src : (Iterable<Path>) walk::iterator) {
                Path dest = target.resolve(source.relativize(src));
                if (Files.isDirectory(src)) {
                    Files.createDirectories(dest);
                } else {
                    Files.copy(src, dest);
                }
            }
        }
    }
}
