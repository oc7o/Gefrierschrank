package de.sloow.gefrierschrank.core;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Run roots across real processes: child JVMs with their own {@code java.io.tmpdir} are stopped by
 * SIGTERM, SIGINT and SIGKILL, a later start sweeps what a killed one left, and no other process
 * can take the lock of one that is still running.
 */
class TempRootChildJvmTest {

    @TempDir
    Path scratch;
    private final List<Process> children = new ArrayList<>();

    @AfterEach
    void killChildren() {
        children.forEach(Process::destroyForcibly);
        TempRoot.forgetEndedSession();
    }

    private record Child(Process process, Path root, Path log) {}

    private Path javaTmp() throws IOException {
        return Files.createDirectories(scratch.resolve("tmp"));
    }

    /**
     * A JVM ignores SIGINT when it starts with SIGINT ignored, which is how a build started in the
     * background passes it down. Perl, where present, restores the default before exec'ing java.
     */
    private static final List<String> DEFAULT_SIGINT = perlAvailable()
        ? List.of("perl", "-e", "$SIG{INT} = 'DEFAULT'; exec @ARGV or die $!") : List.of();

    private static boolean perlAvailable() {
        try {
            return new ProcessBuilder("perl", "-e", "exit 0").start().waitFor() == 0;
        } catch (IOException | InterruptedException e) {
            return false;
        }
    }

    private Child start(String mode) throws Exception {
        Path log = Files.createTempFile(scratch, "child-" + mode, ".log");
        var command = new ArrayList<>(DEFAULT_SIGINT);
        command.addAll(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Djava.io.tmpdir=" + javaTmp(), "-cp", System.getProperty("java.class.path"),
            TempRootChild.class.getName(), mode));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        children.add(process);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            String root = Files.readString(log).lines().filter(l -> l.startsWith("ROOT ")).findFirst().orElse(null);
            if (root != null) return new Child(process, Path.of(root.substring(5)), log);
            if (!process.isAlive() && !mode.equals("start")) fail("child ended early: " + Files.readString(log));
            if (!process.isAlive()) break;
            Thread.sleep(20);
        }
        fail("no run root reported: " + Files.readString(log));
        return null;
    }

    private static long count(Path dir, String prefix) throws IOException {
        if (!Files.exists(dir)) return 0;
        try (Stream<Path> walk = Files.walk(dir)) {
            return walk.filter(p -> p.getFileName().toString().startsWith(prefix)).count();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"TERM", "INT"})
    void aStoppedProcessLeavesNoShardDirectoryAndNoUploadBlock(String signal) throws Exception {
        Child child = start("hold");
        assertEquals(1, count(child.root(), "gefrierschrank-shard-"));
        assertEquals(1, count(child.root(), "s3ablock-"));
        if (signal.equals("TERM")) child.process().destroy();
        else new ProcessBuilder("kill", "-INT", Long.toString(child.process().pid())).start().waitFor();
        boolean exited = child.process().waitFor(20, TimeUnit.SECONDS);
        if (!exited && signal.equals("INT")) {
            child.process().destroyForcibly();
            assumeTrue(false, "SIGINT is ignored by processes started in this environment");
        }
        assertTrue(exited, "the child did not stop");
        assertEquals(signal.equals("TERM") ? 143 : 130, child.process().exitValue());
        assertFalse(Files.exists(child.root()), "the shutdown hook deletes the run root");
        assertEquals(0, count(javaTmp(), "gefrierschrank-"));
        assertEquals(0, count(javaTmp(), "s3ablock-"));
    }

    /**
     * A shard thread recreates the run root after the session closed and keeps writing into it.
     * Before, the hook was gone with the close and the root stayed after exit.
     */
    @Test
    void aRootAThreadRecreatedAfterCloseIsGoneAfterExit() throws Exception {
        Child child = start("late-close");
        assertTrue(child.process().waitFor(60, TimeUnit.SECONDS));
        String log = Files.readString(child.log());
        assertEquals(0, child.process().exitValue(), log);
        assertTrue(log.contains("RECREATED true"), log);
        assertFalse(Files.exists(child.root(), LinkOption.NOFOLLOW_LINKS), log);
        assertEquals(0, count(javaTmp(), "gefrierschrank-"), log);
    }

    /** Main's fatal-Error path: the session is abandoned while a thread that recreates missing parents still runs. */
    @Test
    void afterAFatalErrorALateThreadCannotRecreateTheRoot() throws Exception {
        Child child = start("late-error");
        assertTrue(child.process().waitFor(60, TimeUnit.SECONDS));
        String log = Files.readString(child.log());
        assertEquals(1, child.process().exitValue(), log);
        assertTrue(log.contains("simulated fatal error"), log);
        assertTrue(log.contains("REFUSED"), "the late thread ran into the name holder:\n" + log);
        assertFalse(Files.exists(child.root(), LinkOption.NOFOLLOW_LINKS), log);
        assertEquals(0, count(javaTmp(), "gefrierschrank-"), log);
    }

    @Test
    void aSignalledProcessLeavesNothingEvenWhileAThreadRecreatesMissingParents() throws Exception {
        Child child = start("hold-late");
        child.process().destroy();
        assertTrue(child.process().waitFor(20, TimeUnit.SECONDS), "the child did not stop");
        String log = Files.readString(child.log());
        assertEquals(143, child.process().exitValue(), log);
        assertTrue(log.contains("REFUSED"), "the late thread ran into the name holder:\n" + log);
        assertFalse(Files.exists(child.root(), LinkOption.NOFOLLOW_LINKS), log);
        assertEquals(0, count(javaTmp(), "gefrierschrank-"), log);
        assertEquals(0, count(javaTmp(), "s3ablock-"), log);
    }

    @Test
    void theNextStartRemovesAKilledProcesssRootButNeverALiveOne() throws Exception {
        Child killed = start("hold");
        Child live = start("hold");
        killed.process().destroyForcibly();
        assertTrue(killed.process().waitFor(20, TimeUnit.SECONDS));
        assertEquals(137, killed.process().exitValue());
        assertTrue(Files.exists(killed.root()), "SIGKILL runs no hook");

        Child young = start("start");
        assertTrue(young.process().waitFor(60, TimeUnit.SECONDS));
        assertEquals(0, young.process().exitValue(), Files.readString(young.log()));
        assertTrue(Files.exists(killed.root()), "a root younger than the grace period is kept");
        assertFalse(Files.exists(young.root()), "a normal exit deletes the run root");

        for (Child child : List.of(killed, live)) backdate(child.root());
        Child next = start("start");
        assertTrue(next.process().waitFor(60, TimeUnit.SECONDS));
        assertEquals(0, next.process().exitValue(), Files.readString(next.log()));
        assertFalse(Files.exists(killed.root()), "the killed process's root is swept");
        assertTrue(Files.readString(next.log()).contains("Removed 1 temp run directory"), Files.readString(next.log()));
        assertTrue(Files.exists(live.root().resolve(TempRoot.LOCK_FILE)), "a running process keeps its root");
        assertEquals(1, count(live.root(), "gefrierschrank-shard-"));
        assertEquals(1, count(live.root(), "s3ablock-"));

        live.process().destroy();
        assertTrue(live.process().waitFor(20, TimeUnit.SECONDS));
        assertFalse(Files.exists(live.root()));
        assertEquals(0, count(javaTmp(), "gefrierschrank-"));
    }

    @Test
    void anInterruptedCallerDoesNotReleaseTheRunLock() throws Exception {
        Child holder = start("interrupted");
        assertEquals(1, count(holder.root(), "gefrierschrank-shard-"));
        assertEquals(1, count(holder.root(), "gefrierschrank-worker-"));
        try (FileChannel probe = FileChannel.open(holder.root().resolve(TempRoot.LOCK_FILE), StandardOpenOption.WRITE)) {
            assertNull(probe.tryLock(), "another process must not get the lock of a running process");
        }
        assertEquals("REMOVED 0", sweepWithoutGrace());
        assertEquals(1, count(holder.root(), "gefrierschrank-shard-"), "a running process keeps its root");
        assertEquals(1, count(holder.root(), "s3ablock-"));

        holder.process().destroy();
        assertTrue(holder.process().waitFor(20, TimeUnit.SECONDS));
        assertFalse(Files.exists(holder.root()));
    }

    /** The session lives in this JVM; only another process can see whether its lock is still held. */
    @Test
    void theSessionsOwnFileOperationsKeepTheRunLock() throws Exception {
        try (var session = TempRoot.install(javaTmp())) {
            Path root = TempRoot.runRoot().orElseThrow();
            Path shard = TempRoot.newShardDirectory();
            TempRoot.newFile("gefrierschrank-index-metadata-", ".json");
            Thread.currentThread().interrupt();
            try {
                TempRoot.newShardDirectory();
                TempRoot.newFile("gefrierschrank-worker-", ".jsonl");
            } finally {
                assertTrue(Thread.interrupted(), "the interrupt status is kept");
            }
            assertEquals("REMOVED 0", sweepWithoutGrace());
            assertTrue(Files.isDirectory(shard));
            assertEquals(2, count(root, "gefrierschrank-shard-"));
            assertTrue(Files.exists(root.resolve(TempRoot.LOCK_FILE)));
        }
        assertEquals(0, count(javaTmp(), "gefrierschrank-"));
    }

    /** What any other start would do to this temp directory if the grace period had passed. */
    private String sweepWithoutGrace() throws Exception {
        Path log = Files.createTempFile(scratch, "sweep", ".log");
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp", System.getProperty("java.class.path"), TempRootChild.class.getName(), "sweep", javaTmp().toString())
            .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        children.add(process);
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "the sweep did not finish");
        assertEquals(0, process.exitValue(), Files.readString(log));
        return Files.readString(log).lines().filter(l -> l.startsWith("REMOVED ")).findFirst().orElse(Files.readString(log));
    }

    private static void backdate(Path root) throws IOException {
        FileTime old = FileTime.from(Instant.now().minus(TempRoot.SWEEP_GRACE).minusSeconds(60));
        Files.setLastModifiedTime(root.resolve(TempRoot.LOCK_FILE), old);
        Files.setLastModifiedTime(root, old);
    }
}
