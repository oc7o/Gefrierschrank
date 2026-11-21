package de.sloow.gefrierschrank.core;

import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.UserPrincipal;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/** The run root's lifecycle and the sweep's safety rules, in process. */
class TempRootTest {

    @TempDir
    Path tmp;

    @AfterEach
    void noSessionLeaks() {
        assertTrue(TempRoot.runRoot().isEmpty(), "a test left a run root installed");
        TempRoot.forgetEndedSession();
    }

    private static final Instant NOW = Instant.now();
    private static final Duration GRACE = TempRoot.SWEEP_GRACE;
    private static final Path DEMO_REPO = Path.of("src/test/resources/fixtures/demo-repo");

    /** A run root as a process leaves it: lock file, one shard directory with a file, an S3A block. */
    private Path deadRoot(String name, boolean stale) throws IOException {
        Path root = Files.createDirectory(tmp.resolve(TempRoot.RUN_PREFIX + name));
        Files.writeString(root.resolve(TempRoot.LOCK_FILE), "1\n");
        Path shard = Files.createDirectory(root.resolve("gefrierschrank-shard-1"));
        Files.write(shard.resolve("_0.fdt"), new byte[4096]);
        Files.write(Files.createDirectory(root.resolve("s3a")).resolve("s3ablock-0001.tmp"), new byte[1024]);
        if (stale) age(root);
        return root;
    }

    private static void age(Path root) throws IOException {
        FileTime old = FileTime.from(NOW.minus(GRACE).minusSeconds(1));
        if (Files.exists(root.resolve(TempRoot.LOCK_FILE))) Files.setLastModifiedTime(root.resolve(TempRoot.LOCK_FILE), old);
        Files.setLastModifiedTime(root, old);
    }

    private static UserPrincipal me() throws IOException {
        return TempRoot.currentUser();
    }

    @Test
    void theSweepDeletesAStaleRootWhoseLockIsFree() throws IOException {
        Path root = deadRoot("dead", true);
        var removed = TempRoot.sweep(tmp, null, me(), NOW, GRACE);
        assertFalse(Files.exists(root));
        assertEquals(1, removed.count());
        assertEquals(4096 + 1024 + 2, removed.bytes());
    }

    @Test
    void theSweepKeepsYoungLockedAndOwnRoots() throws IOException {
        Path young = deadRoot("young", false);
        Path own = deadRoot("own", true);
        Path locked = deadRoot("locked", true);
        try (FileChannel holder = FileChannel.open(locked.resolve(TempRoot.LOCK_FILE), StandardOpenOption.WRITE)) {
            assertNotNull(holder.tryLock());
            age(locked);
            var removed = TempRoot.sweep(tmp, own, me(), NOW, GRACE);
            assertEquals(0, removed.count());
        }
        for (Path root : new Path[]{young, own, locked}) {
            assertTrue(Files.exists(root.resolve("gefrierschrank-shard-1/_0.fdt")), root.toString());
        }
    }

    @Test
    void aRecentLockFileKeepsAnOldRoot() throws IOException {
        Path root = deadRoot("busy", true);
        Files.setLastModifiedTime(root.resolve(TempRoot.LOCK_FILE), FileTime.from(NOW));
        assertEquals(0, TempRoot.sweep(tmp, null, me(), NOW, GRACE).count());
        assertTrue(Files.exists(root));
    }

    @Test
    void theSweepNeverFollowsSymbolicLinks() throws IOException {
        Path outside = Files.createDirectories(tmp.resolve("outside"));
        Path victim = deadRoot("victim-target", true);
        Files.move(victim, outside.resolve("target"));
        Path linkedRoot = Files.createSymbolicLink(tmp.resolve(TempRoot.RUN_PREFIX + "link"), outside.resolve("target"));
        Path keep = Files.writeString(outside.resolve("keep.txt"), "not ours to delete");
        Path root = deadRoot("with-link", false);
        Files.createSymbolicLink(root.resolve("escape"), outside);
        age(root);

        var removed = TempRoot.sweep(tmp, null, me(), NOW, GRACE);
        assertEquals(1, removed.count());
        assertFalse(Files.exists(root, java.nio.file.LinkOption.NOFOLLOW_LINKS));
        assertTrue(Files.isSymbolicLink(linkedRoot), "a symlinked root is not a root");
        assertTrue(Files.exists(outside.resolve("target/gefrierschrank-shard-1/_0.fdt")));
        assertEquals("not ours to delete", Files.readString(keep));
    }

    @Test
    void rootsOfOtherUsersAreIgnored() throws IOException {
        Path root = deadRoot("other", true);
        UserPrincipal someoneElse = () -> "someone-else";
        assertEquals(0, TempRoot.sweep(tmp, null, someoneElse, NOW, GRACE).count());
        assertTrue(Files.exists(root.resolve("s3a/s3ablock-0001.tmp")));
    }

    @Test
    void aStaleRootWithoutALockFileIsRemoved() throws IOException {
        Path root = deadRoot("unlocked", false);
        Files.delete(root.resolve(TempRoot.LOCK_FILE));
        age(root);
        assertEquals(1, TempRoot.sweep(tmp, null, me(), NOW, GRACE).count());
        assertFalse(Files.exists(root));
    }

    @Test
    void installCreatesAPrivateLockedRootThatCloseRemoves() throws IOException {
        String before = System.getProperty(TempRoot.RUN_DIRECTORY_PROPERTY);
        Path root;
        Path shard;
        Path file;
        TempRoot.Session session = TempRoot.install(tmp);
        try {
            root = TempRoot.runRoot().orElseThrow();
            assertEquals(tmp, root.getParent());
            assertTrue(root.getFileName().toString().startsWith(TempRoot.RUN_PREFIX));
            if (root.getFileSystem().supportedFileAttributeViews().contains("posix")) {
                assertEquals("rwx------", PosixFilePermissions.toString(Files.getPosixFilePermissions(root)));
            }
            assertEquals(root.toString(), System.getProperty(TempRoot.RUN_DIRECTORY_PROPERTY));
            // Opening run.lock here would release the session's lock; TempRootChildJvmTest checks it from other processes.
            shard = TempRoot.newShardDirectory();
            file = TempRoot.newFile("gefrierschrank-worker-", ".jsonl");
            assertEquals(root, shard.getParent());
            assertEquals(root, file.getParent());
            assertThrows(IllegalStateException.class, () -> TempRoot.install(tmp));
        } finally {
            session.close();
        }
        assertFalse(Files.exists(root));
        assertTrue(TempRoot.runRoot().isEmpty());
        assertTrue(TempRoot.hasEnded());
        assertEquals(before, System.getProperty(TempRoot.RUN_DIRECTORY_PROPERTY));
        session.close();
        assertTrue(Runtime.getRuntime().removeShutdownHook(session.hook),
            "the shutdown hook stays registered: a thread that outlived the session may recreate the root");
    }

    @Test
    void afterTheSessionEndsNothingIsCreatedOutsideTheRoot() throws IOException {
        TempRoot.install(tmp).close();
        assertThrows(IOException.class, TempRoot::newShardDirectory);
        assertThrows(IOException.class, () -> TempRoot.newFile("gefrierschrank-worker-", ".jsonl"));
        try (var entries = Files.list(tmp)) {
            assertEquals(0, entries.count());
        }
    }

    @Test
    void theShutdownHookEndsTheSessionAndHoldsTheRootsNameUntilTheJvmHalts() throws IOException {
        Path root;
        try (var session = TempRoot.install(tmp)) {
            root = TempRoot.runRoot().orElseThrow();
            Path shard = TempRoot.newShardDirectory();
            Files.write(shard.resolve("_0.fdt"), new byte[4096]);
            session.hook.run();
            assertHeldByAnEmptyFile(root);
            assertThrows(IOException.class, TempRoot::newShardDirectory, "a shard started during exit fails at once");
            assertThrows(IOException.class, () -> Files.createDirectories(shard), "nothing recreates the root");
        }
        assertHeldByAnEmptyFile(root);
        deletedOnExit(root);
        try (var entries = Files.list(tmp)) {
            assertEquals(0, entries.count());
        }
    }

    /**
     * A fatal Error in one shard, such as an OutOfMemoryError, ends the command; a shard thread
     * got its directory before the session closed and starts materializing after it. It must fail
     * instead of recreating the deleted root and downloading a whole shard into it.
     */
    @Test
    void aShardMaterializedAfterTheSessionEndedFailsWithoutRecreatingTheRoot() throws Exception {
        var session = TempRoot.install(tmp);
        Path root = TempRoot.runRoot().orElseThrow();
        Path shardDir = TempRoot.newShardDirectory();
        session.close();
        var store = new FileSystemBlobStore(DEMO_REPO);
        var shard = new RepositoryInspector(store).shardSnapshot("test_data_9_3_0", 0);
        var failure = new AtomicReference<Throwable>();
        Thread shardThread = new Thread(() -> {
            try {
                new ShardMaterializer(store, 4).materialize(shard, shardDir);
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        shardThread.start();
        shardThread.join();
        assertInstanceOf(IOException.class, failure.get());
        assertFalse(Files.exists(root, LinkOption.NOFOLLOW_LINKS), "the run root stays deleted");
    }

    /** After a close, a thread recreates the root and writes into it; the exit hook still removes it. */
    @Test
    void theShutdownHookRemovesARootThatAThreadRecreatedAfterClose() throws IOException {
        var session = TempRoot.install(tmp);
        Path root = TempRoot.runRoot().orElseThrow();
        Path shard = TempRoot.newShardDirectory();
        session.close();
        Files.createDirectories(shard);
        Files.write(shard.resolve("_0.fdt"), new byte[1 << 20]);

        assertTrue(Runtime.getRuntime().removeShutdownHook(session.hook), "close keeps the hook");
        session.hook.run();
        assertHeldByAnEmptyFile(root);
        deletedOnExit(root);
        try (var entries = Files.list(tmp)) {
            assertEquals(0, entries.count());
        }
    }

    @Test
    void anAbandonedSessionHoldsTheRootsNameSoThatNoLateThreadCanRecreateIt() throws IOException {
        var session = TempRoot.install(tmp);
        Path root = TempRoot.runRoot().orElseThrow();
        Path shard = TempRoot.newShardDirectory();
        Files.write(shard.resolve("_0.fdt"), new byte[4096]);
        session.abandon();

        assertTrue(TempRoot.hasEnded());
        assertHeldByAnEmptyFile(root);
        // What Lucene's FSDirectory, Hadoop's LocalDirAllocator and DiskChecker do with a missing directory.
        assertThrows(IOException.class, () -> Files.createDirectories(shard));
        assertFalse(root.resolve("s3a").toFile().mkdirs());
        assertThrows(IOException.class, TempRoot::newShardDirectory);
        session.close();
        session.hook.run();
        assertHeldByAnEmptyFile(root);
        deletedOnExit(root);
        try (var entries = Files.list(tmp)) {
            assertEquals(0, entries.count());
        }
    }

    @Test
    void aCommandThatThrowsAbandonsTheSessionAndOneThatReturnsDoesNot() throws IOException {
        try (var session = TempRoot.install(tmp)) {
            assertEquals(3, session.run(() -> 3));
            assertTrue(TempRoot.runRoot().isPresent());
        }
        TempRoot.forgetEndedSession();

        var session = TempRoot.install(tmp);
        Path root = TempRoot.runRoot().orElseThrow();
        var fatal = new OutOfMemoryError("simulated");
        assertSame(fatal, assertThrows(OutOfMemoryError.class, () -> session.run(() -> {
            throw fatal;
        })));
        assertHeldByAnEmptyFile(root);
        session.close();
        assertHeldByAnEmptyFile(root);
        deletedOnExit(root);
    }

    /** A thread that is still shutting down keeps adding files; deletion outlasts it instead of giving up after three walks. */
    @Test
    void deletionWalksAgainWhileAThreadStillAddsFiles() throws Exception {
        Path root = Files.createDirectory(tmp.resolve(TempRoot.RUN_PREFIX + "busy"));
        Path shard = Files.createDirectory(root.resolve("gefrierschrank-shard-1"));
        var started = new java.util.concurrent.CountDownLatch(1);
        Thread writer = new Thread(() -> {
            long end = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(150);
            for (int i = 0; System.nanoTime() < end; i++) {
                try {
                    Files.write(shard.resolve("_" + i + ".fdt"), new byte[16]);
                } catch (IOException gone) {
                    // the directory went: a writer that is shutting down stops here or retries
                }
                started.countDown();
            }
        });
        writer.start();
        started.await();
        TempRoot.deleteTree(root);
        writer.join();
        assertFalse(Files.exists(root, LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    void theSweepRemovesAStaleEmptyNameHolderWithoutCountingIt() throws IOException {
        Path stale = Files.createFile(tmp.resolve(TempRoot.RUN_PREFIX + "killed-while-exiting"));
        Files.setLastModifiedTime(stale, FileTime.from(NOW.minus(GRACE).minusSeconds(1)));
        Path young = Files.createFile(tmp.resolve(TempRoot.RUN_PREFIX + "exiting"));
        Path notOurs = Files.writeString(tmp.resolve(TempRoot.RUN_PREFIX + "notes.txt"), "not a name holder");
        Files.setLastModifiedTime(notOurs, FileTime.from(NOW.minus(GRACE).minusSeconds(1)));

        assertEquals(0, TempRoot.sweep(tmp, null, me(), NOW, GRACE).count());
        assertFalse(Files.exists(stale));
        assertTrue(Files.exists(young), "a process may still be exiting");
        assertTrue(Files.exists(notOurs));
    }

    private static void assertHeldByAnEmptyFile(Path root) throws IOException {
        assertTrue(Files.isRegularFile(root, LinkOption.NOFOLLOW_LINKS), "an empty file holds the root's name");
        assertEquals(0, Files.size(root));
    }

    /** What File.deleteOnExit, registered by install, does after every shutdown hook. */
    private static void deletedOnExit(Path root) {
        assertTrue(root.toFile().delete());
    }

    @Test
    void withoutARootTempFilesGoToJavaIoTmpdirAsBefore() throws IOException {
        Path javaTmp = Path.of(System.getProperty("java.io.tmpdir")).toRealPath();
        Path shard = TempRoot.newShardDirectory();
        Path file = TempRoot.newFile("gefrierschrank-index-metadata-", ".json");
        try {
            assertEquals(javaTmp, shard.toRealPath().getParent());
            assertTrue(shard.getFileName().toString().startsWith("gefrierschrank-shard-"));
            assertEquals(javaTmp, file.toRealPath().getParent());
        } finally {
            Files.delete(shard);
            Files.delete(file);
        }
    }

    /**
     * A library caller without Main's session may name its own run directory, as dump's and the
     * worker's tests do; every creation site then uses it, like the installed root.
     */
    @Test
    void withoutARootTempFilesGoToADirectoryTheCallerNamed() throws IOException {
        Path chosen = Files.createDirectory(tmp.resolve("chosen"));
        String before = System.setProperty(TempRoot.RUN_DIRECTORY_PROPERTY, chosen.toString());
        try {
            assertEquals(chosen, TempRoot.newShardDirectory().getParent());
            assertEquals(chosen, TempRoot.newFile("gefrierschrank-worker-", ".jsonl").getParent());
        } finally {
            if (before == null) System.clearProperty(TempRoot.RUN_DIRECTORY_PROPERTY);
            else System.setProperty(TempRoot.RUN_DIRECTORY_PROPERTY, before);
        }
        try (var entries = Files.list(chosen)) {
            assertEquals(2, entries.count());
        }
    }

    @Test
    void aFailedInstallWarnsAndRunsWithoutARoot() throws IOException {
        Path notADirectory = Files.writeString(tmp.resolve("file"), "x");
        String err = stderrOf(() -> {
            try (var session = TempRoot.install(notADirectory)) {
                assertTrue(TempRoot.runRoot().isEmpty());
            }
            return null;
        });
        assertTrue(err.contains("warning: cannot create a private temp directory"), err);
    }

    @Test
    void legacyLeftoversAreReportedAndNeverDeleted() throws IOException {
        Path shard = Files.createDirectory(tmp.resolve("gefrierschrank-shard-123"));
        Files.write(shard.resolve("_0.cfs"), new byte[3 << 20]);
        Path spool = Files.write(tmp.resolve("gefrierschrank-worker-9.jsonl"), new byte[1 << 20]);
        Path metadata = Files.writeString(tmp.resolve("gefrierschrank-index-metadata-4.json"), "{}");
        for (Path entry : new Path[]{shard, spool, metadata}) Files.setLastModifiedTime(entry, FileTime.from(NOW.minus(Duration.ofDays(3))));
        assertFalse(TempRoot.DELETE_LEGACY_LEFTOVERS);

        String err = stderrOf(() -> {
            TempRoot.install(tmp).close();
            return null;
        });
        assertTrue(err.contains("note: 3 temp entries (5 MiB) of earlier gefrierschrank versions remain in " + tmp), err);
        assertEquals(1, err.lines().filter(l -> l.startsWith("note:")).count(), err);
        assertTrue(Files.exists(shard.resolve("_0.cfs")) && Files.exists(spool) && Files.exists(metadata));
    }

    @Test
    void installSweepsBeforeCreatingItsOwnRootAndReportsTheReclaimedSpace() throws IOException {
        Path dead = deadRoot("dead", true);
        String err = stderrOf(() -> {
            TempRoot.install(tmp).close();
            return null;
        });
        assertFalse(Files.exists(dead));
        assertTrue(err.contains("Removed 1 temp run directory (1 MiB) of ended gefrierschrank processes from " + tmp), err);
        try (var entries = Files.list(tmp)) {
            assertEquals(0, entries.count(), "nothing is left behind");
        }
    }

    private static String stderrOf(Supplier<Void> action) {
        PrintStream original = System.err;
        var captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true));
        try {
            action.get();
        } finally {
            System.setErr(original);
        }
        return captured.toString();
    }
}
