package de.sloow.gefrierschrank.cli;

import net.bytebuddy.agent.Installer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An OutOfMemoryError where a finished task hands its result, its failure or its end to the
 * thread that waits for it: the run must still end, with the Error when a result was lost, and
 * leave no temp run directory. Before, the waiting thread waited forever at 0% CPU and kept the
 * run directory and the table locks. Each case runs the CLI in a child JVM ({@link HandOffFailureChild}).
 */
class LostHandOffTest {

    private static final Path DEMO_REPO = Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");
    private static final long BOUND_SECONDS = 120;

    private record Run(int exitCode, String output) {}

    private static Run run(Path scratch, Map<String, String> injections, List<String> jvmOptions, String... args)
            throws IOException, InterruptedException {
        Path javaTmp = Files.createDirectories(scratch.resolve("tmp"));
        Path log = scratch.resolve("child.log");
        String agent;
        try {
            agent = Path.of(Installer.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
        } catch (java.net.URISyntaxException e) {
            throw new IllegalStateException(e);
        }
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-javaagent:" + agent, "-Djava.io.tmpdir=" + javaTmp));
        injections.forEach((target, spec) -> command.add("-D" + HandOffFailureChild.PREFIX + target + "=" + spec));
        command.addAll(jvmOptions);
        command.addAll(List.of("-cp", System.getProperty("java.class.path"), HandOffFailureChild.class.getName()));
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            boolean ended = process.waitFor(BOUND_SECONDS, TimeUnit.SECONDS);
            String output = Files.readString(log);
            assertTrue(ended, "the run did not end within " + BOUND_SECONDS + " s:\n" + output);
            for (String target : injections.keySet()) {
                String type = target.substring(0, target.lastIndexOf('.'));
                assertTrue(output.contains("instrumented " + type + "\n"), "not instrumented: " + type + "\n" + output);
            }
            try (Stream<Path> left = Files.list(javaTmp)) {
                assertEquals(List.of(), left.filter(p -> p.getFileName().toString().startsWith("gefrierschrank-")).toList(),
                    "the run directory is gone:\n" + output);
            }
            return new Run(process.exitValue(), output);
        } finally {
            process.destroyForcibly().waitFor();
        }
    }

    private static List<String> allIndices(Path scratch) {
        return List.of("dump-to-delta", DEMO_REPO.toAbsolutePath().toString(), "--all-indices",
            "-o", scratch.resolve("tables").toString(), "--parallelism", "2");
    }

    private static void assertEndedWithTheInjectedError(Run run, String origin) {
        assertEquals(1, run.exitCode(), run.output());
        assertTrue(run.output().contains("java.lang.OutOfMemoryError: injected at " + origin), run.output());
    }

    /** A download's completion that never reaches the shard's completion queue. */
    @Test
    void aDownloadWhoseCompletionCannotBeQueuedNeverStallsItsShard(@TempDir Path scratch) throws Exception {
        var run = run(scratch,
            Map.of("java.util.concurrent.ExecutorCompletionService$QueueingFuture.done", "gefrierschrank-download-||1"),
            List.of("-Dgefrierschrank.download.concurrency=4"), allIndices(scratch).toArray(String[]::new));
        assertEquals(0, run.exitCode(), run.output());
        assertTrue(run.output().contains("All-indices run: 3 indices - 3 completed"), run.output());
    }

    @Test
    void aShardResultThatCannotBeQueuedEndsTheRunWithItsError(@TempDir Path scratch) throws Exception {
        var run = run(scratch,
            Map.of("java.util.concurrent.LinkedBlockingQueue.offer", "gefrierschrank-shard-|de.sloow.gefrierschrank.delta.ShardWorkPool$Result|1"),
            List.of(), allIndices(scratch).toArray(String[]::new));
        assertEndedWithTheInjectedError(run, "java.util.concurrent.LinkedBlockingQueue.offer");
    }

    @Test
    void anIndexResultThatCannotBeQueuedEndsTheRunWithItsError(@TempDir Path scratch) throws Exception {
        var run = run(scratch,
            Map.of("java.util.concurrent.LinkedBlockingQueue.offer",
                "gefrierschrank-index-|de.sloow.gefrierschrank.cli.DumpToDeltaCommand$IndexResult|1"),
            List.of(), allIndices(scratch).toArray(String[]::new));
        assertEndedWithTheInjectedError(run, "java.util.concurrent.LinkedBlockingQueue.offer");
    }

    /**
     * Every shard thread's release of its admission slot fails where it used to box the queue
     * ticket: with two slots and three indices, the third shard was never admitted.
     */
    @Test
    void releasingAShardsSlotAllocatesNothing(@TempDir Path scratch) throws Exception {
        var run = run(scratch, Map.of("java.util.TreeSet.remove", "gefrierschrank-shard-|java.lang.Long|0"),
            List.of(), allIndices(scratch).toArray(String[]::new));
        assertEquals(0, run.exitCode(), run.output());
        assertTrue(run.output().contains("All-indices run: 3 indices - 3 completed"), run.output());
    }

    /**
     * A range fails with an Error, and recording that failure fails too: before, the range's thread
     * died with it, and the shard was quarantined with a NullPointerException instead.
     */
    @Test
    void aRangeFailureThatCannotBeCollectedStillEndsTheRunWithItsError(@TempDir Path scratch) throws Exception {
        var run = run(scratch,
            Map.of("de.sloow.gefrierschrank.delta.DeltaTableWriter.stage", "gefrierschrank-range-||1",
                "java.util.concurrent.ConcurrentLinkedQueue.offer", "gefrierschrank-range-|java.lang.OutOfMemoryError|1"),
            List.of("-Dgefrierschrank.shard.threads.minRangeLiveDocs=1"),
            "dump-to-delta", DEMO_REPO.toAbsolutePath().toString(), "--index", "test_data_9_3_0",
            "-o", scratch.resolve("table").toString(), "--shard-threads", "2");
        assertEndedWithTheInjectedError(run, "de.sloow.gefrierschrank.delta.DeltaTableWriter.stage");
    }

    @Test
    void aSurveyWhoseCompletionCannotBeQueuedStillEnds(@TempDir Path scratch) throws Exception {
        var run = run(scratch,
            Map.of("java.util.concurrent.ExecutorCompletionService$QueueingFuture.done", "survey-||1"),
            List.of(), "survey", DEMO_REPO.toAbsolutePath().toString());
        assertEquals(0, run.exitCode(), run.output());
    }
}
