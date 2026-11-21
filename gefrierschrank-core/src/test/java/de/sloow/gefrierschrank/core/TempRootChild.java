package de.sloow.gefrierschrank.core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * The child JVM of {@link TempRootChildJvmTest}. {@code hold}: installs a run root, fills it like
 * an export in flight (a shard directory with a file, an S3A block file), prints
 * {@code ROOT <path>} and waits to be signalled. {@code interrupted}: the same, but the shard
 * directory and a spool file are created by a thread whose interrupt status is set.
 * {@code start}: installs and closes a run root, which is what every CLI start does before its
 * command. {@code sweep <dir>}: without a root of its own, sweeps {@code dir} with no grace
 * period and prints {@code REMOVED <count>}.
 *
 * <p>Threads that outlive the session, each started once the root is gone, while a slow shutdown
 * hook (like Hadoop's, which closes its filesystems) keeps the JVM from halting at once:
 * {@code late-close} is a normal close after which a shard thread creates its directory with
 * {@code Files.createDirectories} and writes 1 MiB files until a write fails;
 * {@code late-error} ends the command with an Error as {@code Main} does, and {@code hold-late}
 * waits to be signalled; in both a thread recreates missing parents before every file, as Lucene's
 * FSDirectory and Hadoop's LocalDirAllocator do, until the first failure, then prints
 * {@code REFUSED <exception>}.
 */
public final class TempRootChild {

    public static void main(String[] args) throws Exception {
        if (args[0].equals("sweep")) {
            var removed = TempRoot.sweep(Path.of(args[1]), null, TempRoot.currentUser(), Instant.now(), Duration.ZERO);
            System.out.println("REMOVED " + removed.count());
            return;
        }
        if (args[0].equals("late-close")) {
            lateClose();
            return;
        }
        if (args[0].equals("late-error") || args[0].equals("hold-late")) {
            lateThreadOfARunningCommand(args[0].equals("hold-late"));
            return;
        }
        try (var session = TempRoot.install()) {
            Path root = TempRoot.runRoot().orElseThrow();
            if (args[0].equals("hold")) fill(root, TempRoot.newShardDirectory());
            if (args[0].equals("interrupted")) {
                var shard = new AtomicReference<Path>();
                Thread caller = new Thread(() -> {
                    Thread.currentThread().interrupt();
                    try {
                        shard.set(TempRoot.newShardDirectory());
                        TempRoot.newFile("gefrierschrank-worker-", ".jsonl");
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                    if (!Thread.currentThread().isInterrupted()) throw new AssertionError("interrupt status lost");
                });
                caller.start();
                caller.join();
                // Filled from this thread: an interrupted thread cannot write through a file channel at all.
                fill(root, shard.get());
            }
            System.out.println("ROOT " + root);
            System.out.flush();
            if (!args[0].equals("start")) Thread.sleep(120_000);
        }
    }

    private static void lateClose() throws Exception {
        var session = TempRoot.install();
        Path root = TempRoot.runRoot().orElseThrow();
        Path shard = TempRoot.newShardDirectory();
        var wrote = new CountDownLatch(1);
        daemon(() -> {
            awaitDeletion(root);
            try {
                Files.createDirectories(shard);
                for (int i = 0; i < 1000; i++) {
                    Files.write(shard.resolve("_" + i + ".fdt"), new byte[1 << 20]);
                    wrote.countDown();
                    Thread.sleep(3);
                }
            } catch (Exception e) {
                System.out.println("REFUSED " + e.getClass().getSimpleName());
            }
        });
        session.close();
        if (!wrote.await(30, TimeUnit.SECONDS)) throw new AssertionError("the late thread never wrote");
        slowShutdownHook();
        System.out.println("ROOT " + root);
        System.out.println("RECREATED " + Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS));
        System.out.flush();
    }

    private static void lateThreadOfARunningCommand(boolean waitForSignal) throws Exception {
        var refused = new CountDownLatch(1);
        try (var session = TempRoot.install()) {
            Path root = TempRoot.runRoot().orElseThrow();
            try {
                session.run(() -> {
                    try {
                        Path shard = TempRoot.newShardDirectory();
                        fill(root, shard);
                        daemon(() -> recreateBeforeEveryFile(root, shard, refused));
                        slowShutdownHook();
                        System.out.println("ROOT " + root);
                        System.out.flush();
                        if (waitForSignal) Thread.sleep(120_000);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                    throw new Error("simulated fatal error, as an OutOfMemoryError in another shard ends the command");
                });
            } finally {
                // Main's Error propagates while the thread still runs; it has met the name holder by now.
                refused.await(30, TimeUnit.SECONDS);
            }
        }
    }

    private static void recreateBeforeEveryFile(Path root, Path shard, CountDownLatch refused) {
        awaitDeletion(root);
        for (int i = 0; ; i++) {
            try {
                Files.createDirectories(shard);
                Files.write(shard.resolve("_late" + i + ".fdt"), new byte[64 << 10]);
            } catch (IOException | RuntimeException e) {
                System.out.println("REFUSED " + e.getClass().getSimpleName());
                refused.countDown();
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }

    private static void awaitDeletion(Path root) {
        while (Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(200));
    }

    private static void daemon(Runnable body) {
        Thread thread = new Thread(body, "late-shard-thread");
        thread.setDaemon(true);
        thread.start();
    }

    private static void slowShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500))));
    }

    private static void fill(Path root, Path shard) throws IOException {
        Files.write(shard.resolve("_0.fdt"), new byte[1 << 20]);
        Path blocks = Files.createDirectories(root.resolve("s3a"));
        Files.write(blocks.resolve("s3ablock-0001-4242.tmp"), new byte[64 << 10]);
    }
}
