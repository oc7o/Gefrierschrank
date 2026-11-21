package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.TempRoot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/** The CLI's run root: where shard directories and upload blocks go, and what a start sweeps before the temp budget. */
class RunRootCleanupTest {

    private static final Path DEMO_REPO = Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");
    private static final String INDEX = "test_data_9_3_0";

    /** Child JVMs get their own java.io.tmpdir: a start sweeps it, and the shared one is not ours to sweep. */
    private static Process child(Path javaTmp, Path log, String mainClass, String... args) throws IOException {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Djava.io.tmpdir=" + javaTmp, "-cp", System.getProperty("java.class.path"), mainClass));
        command.addAll(List.of(args));
        return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    private static String value(List<String> lines, String key) {
        return lines.stream().filter(l -> l.startsWith(key + " ")).map(l -> l.substring(key.length() + 1))
            .findFirst().orElseThrow(() -> new AssertionError(key + " missing:\n" + String.join("\n", lines)));
    }

    @Test
    void shardDirectoriesAndS3aUploadBlocksGoIntoTheRunRoot(@TempDir Path scratch) throws Exception {
        Path javaTmp = Files.createDirectories(scratch.resolve("tmp"));
        Path log = scratch.resolve("probe.log");
        Process process = child(javaTmp, log, RunRootProbe.class.getName(),
            DEMO_REPO.toAbsolutePath().toString(), scratch.resolve("table").toString(), INDEX);
        assertTrue(process.waitFor(5, TimeUnit.MINUTES), "probe did not finish");
        List<String> lines = Files.readAllLines(log);
        assertEquals(0, process.exitValue(), String.join("\n", lines));

        Path root = Path.of(value(lines, "ROOT"));
        assertEquals(javaTmp.toRealPath(), root.getParent().toRealPath());
        String[] shard = value(lines, "SHARD_DIR").split(" ");
        assertEquals(root, Path.of(shard[0]).getParent(), "the shard directory is created in the run root");
        assertTrue(Path.of(shard[0]).getFileName().toString().startsWith("gefrierschrank-shard-"));
        assertEquals("true", shard[1]);
        assertEquals("1", value(lines, "QUARANTINED"));
        assertEquals(root.resolve("s3a"), Path.of(value(lines, "S3A_BUFFER_DIR")), "S3A upload blocks go to the run root");
        assertFalse(Files.exists(root), "closing the session deletes the root");
    }

    @Test
    void aStartSweepsEndedRunsBeforeTheTempBudgetIsTakenAndLeavesNothingBehind(@TempDir Path scratch) throws Exception {
        Path javaTmp = Files.createDirectories(scratch.resolve("tmp"));
        Path dead = Files.createDirectory(javaTmp.resolve("gefrierschrank-run-killed"));
        Files.writeString(dead.resolve("run.lock"), "1\n");
        Files.write(Files.createDirectory(dead.resolve("gefrierschrank-shard-1")).resolve("_0.cfs"), new byte[2 << 20]);
        Files.write(Files.createDirectory(dead.resolve("s3a")).resolve("s3ablock-0001.tmp"), new byte[1 << 20]);
        FileTime old = FileTime.from(Instant.now().minus(TempRoot.SWEEP_GRACE).minusSeconds(60));
        Files.setLastModifiedTime(dead.resolve("run.lock"), old);
        Files.setLastModifiedTime(dead, old);

        Path log = scratch.resolve("child.log");
        Process process = child(javaTmp, log, Main.class.getName(), "dump-to-delta", DEMO_REPO.toAbsolutePath().toString(),
            "--index", INDEX, "-o", scratch.resolve("table").toString(), "--parallelism", "2");
        assertTrue(process.waitFor(5, TimeUnit.MINUTES), "export did not finish");
        List<String> lines = Files.readAllLines(log);
        assertEquals(0, process.exitValue(), String.join("\n", lines));

        int swept = indexOf(lines, "Removed 1 temp run directory (4 MiB) of ended gefrierschrank processes");
        int budget = indexOf(lines, "temp-disk budget");
        assertTrue(swept >= 0 && budget > swept, "the sweep finishes before the budget is taken:\n" + String.join("\n", lines));
        assertFalse(Files.exists(dead));
        try (Stream<Path> left = Files.list(javaTmp)) {
            assertEquals(List.of(), left.filter(p -> p.getFileName().toString().startsWith("gefrierschrank-")).toList(),
                "the run root is gone after a normal exit");
        }
    }

    private static int indexOf(List<String> lines, String fragment) {
        for (int i = 0; i < lines.size(); i++) if (lines.get(i).contains(fragment)) return i;
        return -1;
    }
}
