package de.sloow.gefrierschrank.core;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.UserPrincipal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntSupplier;

/**
 * The per-process root for temporary files: {@code <java.io.tmpdir>/gefrierschrank-run-<random>},
 * owner-only, holding the shard directories, large documents waiting for heap and S3A upload
 * blocks of one CLI process. Its {@value #LOCK_FILE} stays locked while the process lives; the
 * operating system releases the lock on any exit, SIGKILL included.
 *
 * <p>{@link #install()} (only the CLI's main does this) first removes the run roots of processes
 * that have ended, then creates this process's root. The root is deleted when the session closes,
 * and by a shutdown hook on SIGTERM, SIGINT or an exit after an Error; after SIGKILL the next
 * start removes it. Once the session has ended, creating a temporary file fails, so no thread that
 * outlives it can leave files outside the root. Without an installed root (library use, in-process
 * tests) temporary files are created in the directory {@value #RUN_DIRECTORY_PROPERTY} names, if a
 * caller set it, and otherwise directly in {@code java.io.tmpdir}, exactly as before.
 *
 * <p>Threads of the run can outlive the session: an Error ends the command without waiting for
 * other shards, and a signal ends the JVM under running ones. Lucene's {@code FSDirectory},
 * Hadoop's buffer directory allocator and {@code Files.createDirectories} create missing parent
 * directories, so such a thread would recreate a deleted root and fill it until the JVM halts. An
 * abandoned session and the shutdown hook therefore put an empty file ({@link Session#abandon()})
 * in the root's place, against which that creation fails. {@link java.io.File#deleteOnExit()}
 * removes the file: the JVM runs it after every shutdown hook, Hadoop's included, right before it
 * halts.
 *
 * <p>{@code java.io.tmpdir} cannot simply be pointed at the root: the JDK reads it once at VM
 * start. Every creation site of the export therefore asks this class.
 */
public final class TempRoot {

    /**
     * A run root younger than this is never removed: its process may have created it but not yet
     * locked it.
     */
    public static final Duration SWEEP_GRACE = Duration.ofMinutes(10);
    /**
     * Temp files of builds without a run root (top-level {@code gefrierschrank-shard-*},
     * {@code gefrierschrank-worker-*.jsonl}, {@code gefrierschrank-index-metadata-*.json}) carry
     * no owner marker, so a live older process cannot be told apart from a dead one. They are
     * reported at start and, while this is false, never deleted.
     */
    public static final boolean DELETE_LEGACY_LEFTOVERS = false;
    /**
     * The installed run root while a session is installed; without one, a directory a library
     * caller chose for the temporary files.
     */
    public static final String RUN_DIRECTORY_PROPERTY = "gefrierschrank.runDirectory";

    static final String RUN_PREFIX = "gefrierschrank-run-";
    static final String LOCK_FILE = "run.lock";
    static final List<String> LEGACY_PATTERNS =
        List.of("gefrierschrank-shard-*", "gefrierschrank-worker-*.jsonl", "gefrierschrank-index-metadata-*.json");
    private static final String SHARD_PREFIX = "gefrierschrank-shard-";
    private static final long MIB = 1L << 20;
    /** Walks over a tree that threads may still be adding to, 10 ms apart after the first two. */
    private static final int DELETE_PASSES = 50;
    private static final long DELETE_PASS_PAUSE_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final int BLOCKER_ATTEMPTS = 20;

    /** The current session, or the ended one, which keeps later creations from escaping to java.io.tmpdir. */
    private static volatile Session installed;

    private TempRoot() {}

    /**
     * Sweeps ended processes' run roots, then creates and locks this process's root. A failure
     * only warns; the session then has no root. Only a second install before close throws.
     */
    public static Session install() {
        return install(Path.of(System.getProperty("java.io.tmpdir")));
    }

    static synchronized Session install(Path tmp) {
        if (installed != null && !installed.ended) throw new IllegalStateException("a temp run root is already installed");
        sweepAndReport(tmp);
        try {
            installed = Session.create(tmp);
            return installed;
        } catch (IOException | RuntimeException e) {
            installed = null;
            System.err.println("warning: cannot create a private temp directory in " + tmp + " ("
                + e.getClass().getSimpleName() + "); temporary files go there directly and are not removed after a crash");
            return Session.NONE;
        }
    }

    /**
     * Forgets an ended session, so that creation falls back to java.io.tmpdir again, and unregisters
     * its shutdown hook; for in-process tests.
     */
    static synchronized void forgetEndedSession() {
        if (installed != null && installed.ended) {
            if (installed.root != null) {
                try {
                    Runtime.getRuntime().removeShutdownHook(installed.hook);
                } catch (IllegalStateException shuttingDown) {
                    // the hook runs anyway
                }
            }
            installed = null;
        }
    }

    /** The installed run root, if any. */
    public static Optional<Path> runRoot() {
        Session session = installed;
        return session == null || session.ended ? Optional.empty() : Optional.of(session.root);
    }

    /**
     * Whether this process's installed session has ended: it is exiting, and a failure to create a
     * directory under the root is the shutdown's doing, not the disk's.
     */
    public static boolean hasEnded() {
        Session session = installed;
        return session != null && session.root != null && session.ended;
    }

    /** A new directory for one shard: in the run root, or without one as the class comment says. */
    public static Path newShardDirectory() throws IOException {
        Session session = installed;
        if (session != null) return Files.createTempDirectory(session.liveRoot(), SHARD_PREFIX);
        Path chosen = chosenDirectory();
        return chosen == null ? Files.createTempDirectory(SHARD_PREFIX) : Files.createTempDirectory(chosen, SHARD_PREFIX);
    }

    /** A new empty file: in the run root, or without one as the class comment says. */
    public static Path newFile(String prefix, String suffix) throws IOException {
        Session session = installed;
        if (session != null) return Files.createTempFile(session.liveRoot(), prefix, suffix);
        Path chosen = chosenDirectory();
        return chosen == null ? Files.createTempFile(prefix, suffix) : Files.createTempFile(chosen, prefix, suffix);
    }

    private static Path chosenDirectory() {
        String chosen = System.getProperty(RUN_DIRECTORY_PROPERTY);
        return chosen == null || chosen.isBlank() ? null : Path.of(chosen);
    }

    /**
     * One installed run root; closing it deletes the root.
     *
     * <p>The lock is a POSIX record lock, which the OS drops when the process closes any
     * descriptor of the lock file. So nothing in this process opens that file again, and its
     * descriptor is never used through a {@link FileChannel} after locking: an interruptible
     * channel closes itself, releasing the lock, when a thread with its interrupt status set uses
     * it. The timestamp is refreshed through the {@link RandomAccessFile}, which is not
     * interruptible.
     */
    public static final class Session implements AutoCloseable {
        static final Session NONE = new Session(null, null, null, null);

        final Path root;
        private final RandomAccessFile lockFile;
        private final FileLock lock;
        /** Stays registered after close: a thread that outlived the session may have recreated the root. */
        final Thread hook;
        private final String previousProperty;
        /** Set while holding this by close(), abandon() and the shutdown hook, so no refresh follows. */
        private volatile boolean ended;
        private boolean closed; // guarded by this
        /** The root's name is held by an empty file until the JVM halts; guarded by this. */
        private boolean blocked;
        private boolean refreshWarned; // guarded by this
        private final byte[] pid = (ProcessHandle.current().pid() + "\n").getBytes(StandardCharsets.US_ASCII);

        private Session(Path root, RandomAccessFile lockFile, FileLock lock, String previousProperty) {
            this.root = root;
            this.lockFile = lockFile;
            this.lock = lock;
            this.previousProperty = previousProperty;
            this.hook = new Thread(this::exiting, "gefrierschrank-temp-cleanup");
        }

        /**
         * Creates the root with mode 0700 (the JDK's default for temp directories on POSIX) and
         * locks its lock file before anything else goes in.
         */
        static Session create(Path tmp) throws IOException {
            Path root = Files.createTempDirectory(tmp, RUN_PREFIX);
            RandomAccessFile lockFile = null;
            String previous = System.getProperty(RUN_DIRECTORY_PROPERTY);
            try {
                lockFile = new RandomAccessFile(Files.createFile(root.resolve(LOCK_FILE)).toFile(), "rw");
                FileLock lock = lockFile.getChannel().tryLock();
                if (lock == null) throw new IOException("run lock is held");
                var session = new Session(root, lockFile, lock, previous);
                session.liveRoot();
                System.setProperty(RUN_DIRECTORY_PROPERTY, root.toString());
                Runtime.getRuntime().addShutdownHook(session.hook);
                // Deletes the empty file that holds the root's name during exit; a no-op otherwise.
                root.toFile().deleteOnExit();
                return session;
            } catch (IOException | RuntimeException e) {
                restoreProperty(previous);
                if (lockFile != null) closeQuietly(lockFile);
                deleteTree(root);
                throw e;
            }
        }

        /**
         * The root for a new entry. Rewrites the pid into the lock file first, which keeps it
         * recent for age-based temp cleaners during a long run.
         */
        synchronized Path liveRoot() throws IOException {
            if (ended) throw new IOException("the temp run directory of this process has been removed: it is exiting");
            if (!lock.isValid()) throw new IOException("the temp run directory of this process lost its lock");
            try {
                lockFile.seek(0);
                lockFile.write(pid);
            } catch (IOException e) {
                if (!refreshWarned) {
                    refreshWarned = true;
                    System.err.println("warning: cannot refresh " + root.resolve(LOCK_FILE) + " (" + e.getClass().getSimpleName()
                        + "); the run keeps its lock, but an age-based temp cleaner may remove the file during a long run");
                }
            }
            return root;
        }

        /**
         * Runs the session's command. A Throwable escaping it can leave the command's daemon
         * threads running until the JVM halts, so the session is {@linkplain #abandon() abandoned}
         * before the Throwable propagates.
         */
        public int run(IntSupplier command) {
            try {
                return command.getAsInt();
            } catch (Throwable fatal) {
                try {
                    abandon();
                } catch (Throwable cleanup) {
                    fatal.addSuppressed(cleanup);
                }
                throw fatal;
            }
        }

        /**
         * Ends the session while threads of the run may still write: deletes the root and puts an
         * empty file in its place, which the JVM deletes right before it halts (see the class
         * comment). A later {@link #close()} leaves that file alone.
         */
        public void abandon() {
            if (root == null) return;
            synchronized (this) {
                ended = true;
                if (!blocked) blocked = replaceWithBlocker(root);
            }
        }

        @Override
        public void close() {
            if (root == null) return;
            synchronized (this) {
                if (closed) return;
                closed = true;
                ended = true;
                try {
                    restoreProperty(previousProperty);
                    if (!blocked) deleteTree(root);
                } finally {
                    closeQuietly(lockFile);
                }
            }
        }

        /**
         * The shutdown hook: on SIGTERM and SIGINT the run's threads are still working, and after
         * an Error or even a normal close one may have recreated the root. Deletes without waiting
         * for them: files removed under them only make them fail, and no commit references them.
         */
        private void exiting() {
            if (root == null) return;
            synchronized (this) {
                ended = true;
                if (!blocked) blocked = replaceWithBlocker(root);
            }
        }
    }

    /**
     * Deletes the root and creates an empty file under its name, again if a thread recreated the
     * directory in between; false if no file could be created there.
     */
    private static boolean replaceWithBlocker(Path root) {
        for (int attempt = 0; attempt < BLOCKER_ATTEMPTS; attempt++) {
            deleteTree(root);
            try {
                Files.createFile(root);
                return true;
            } catch (FileAlreadyExistsException recreated) {
                // deleted again by the next attempt
            } catch (IOException | RuntimeException e) {
                return false;
            }
        }
        return false;
    }

    private static void restoreProperty(String previous) {
        if (previous == null) System.clearProperty(RUN_DIRECTORY_PROPERTY);
        else System.setProperty(RUN_DIRECTORY_PROPERTY, previous);
    }

    private static void sweepAndReport(Path tmp) {
        UserPrincipal user;
        try {
            user = currentUser();
        } catch (IOException | RuntimeException e) {
            System.err.println("warning: temp directory sweep skipped (" + e.getClass().getSimpleName() + ")");
            return;
        }
        try {
            Tally removed = sweep(tmp, null, user, Instant.now(), SWEEP_GRACE);
            if (removed.count() > 0) {
                System.err.println("Removed " + removed.count() + " temp run director" + (removed.count() == 1 ? "y" : "ies")
                    + " (" + mib(removed.bytes()) + ") of ended gefrierschrank processes from " + tmp);
            }
        } catch (IOException | RuntimeException e) {
            System.err.println("warning: temp directory sweep failed (" + e.getClass().getSimpleName() + ")");
        }
        try {
            Tally legacy = legacyLeftovers(tmp, user, Instant.now());
            if (legacy.count() > 0) {
                System.err.println("note: " + legacy.count() + " temp entr" + (legacy.count() == 1 ? "y" : "ies") + " ("
                    + mib(legacy.bytes()) + ") of earlier gefrierschrank versions remain in " + tmp + " ("
                    + String.join(", ", LEGACY_PATTERNS) + "); they are not removed automatically, delete them"
                    + " once no gefrierschrank process of an earlier version is running");
            }
        } catch (IOException | RuntimeException e) {
            System.err.println("warning: cannot list earlier temp files (" + e.getClass().getSimpleName() + ")");
        }
    }

    record Tally(int count, long bytes) {}

    /**
     * Deletes every sibling run root of {@code user} other than {@code own} that is a real
     * directory (never a symbolic link), older than {@code grace}, and whose lock file this
     * process can lock, which means its owner has ended. Held locks, including this JVM's own
     * ({@link OverlappingFileLockException}), keep a root. Nothing is followed through a link.
     * An empty regular file under the prefix, older than {@code grace}, is the name holder of a
     * process killed while it was exiting ({@link Session#abandon()}); it is deleted, not counted.
     */
    static Tally sweep(Path tmp, Path own, UserPrincipal user, Instant now, Duration grace) throws IOException {
        int count = 0;
        long bytes = 0;
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(tmp, RUN_PREFIX + "*")) {
            for (Path candidate : entries) {
                if (candidate.equals(own)) continue;
                if (ownedEmptyFile(candidate, user)) {
                    if (olderThan(candidate, now.minus(grace))) deleteQuietly(candidate);
                    continue;
                }
                if (!ownedDirectory(candidate, user)) continue;
                if (!olderThan(candidate, now.minus(grace))) continue;
                long reclaimed = removeIfAbandoned(candidate);
                if (reclaimed >= 0) {
                    count++;
                    bytes += reclaimed;
                }
            }
        }
        return new Tally(count, bytes);
    }

    /** Reports (and with {@link #DELETE_LEGACY_LEFTOVERS} removes) earlier versions' top-level temp entries. */
    static Tally legacyLeftovers(Path tmp, UserPrincipal user, Instant now) throws IOException {
        int count = 0;
        long bytes = 0;
        for (String pattern : LEGACY_PATTERNS) {
            try (DirectoryStream<Path> entries = Files.newDirectoryStream(tmp, pattern)) {
                for (Path entry : entries) {
                    BasicFileAttributes attributes = attributes(entry);
                    if (attributes == null || attributes.isSymbolicLink() || !owner(entry, user)) continue;
                    long size = sizeOf(entry);
                    if (DELETE_LEGACY_LEFTOVERS && olderThan(entry, now.minus(SWEEP_GRACE))) deleteTree(entry);
                    count++;
                    bytes += size;
                }
            }
        }
        return new Tally(count, bytes);
    }

    /** The reclaimed size, or -1 when the root's process still holds its lock. */
    private static long removeIfAbandoned(Path root) {
        Path lockFile = root.resolve(LOCK_FILE);
        if (Files.isSymbolicLink(lockFile)) return -1;
        // A root without a lock file lost it after the grace period: its process ended between
        // creating the root and locking it, or its shutdown hook raced a late writer.
        try (FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS)) {
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException heldByThisJvm) {
                return -1;
            }
            if (lock == null) return -1;
            long size = sizeOf(root);
            deleteTree(root);
            return size;
        } catch (IOException | RuntimeException e) {
            return -1;
        }
    }

    private static boolean ownedDirectory(Path path, UserPrincipal user) {
        BasicFileAttributes attributes = attributes(path);
        return attributes != null && attributes.isDirectory() && !attributes.isSymbolicLink() && owner(path, user);
    }

    private static boolean ownedEmptyFile(Path path, UserPrincipal user) {
        BasicFileAttributes attributes = attributes(path);
        return attributes != null && attributes.isRegularFile() && attributes.size() == 0 && owner(path, user);
    }

    private static boolean owner(Path path, UserPrincipal user) {
        try {
            return user.equals(Files.getOwner(path, LinkOption.NOFOLLOW_LINKS));
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    /** The newest of the entry's own modification time and its lock file's. */
    private static boolean olderThan(Path path, Instant cutoff) {
        BasicFileAttributes attributes = attributes(path);
        if (attributes == null) return false;
        Instant newest = attributes.lastModifiedTime().toInstant();
        BasicFileAttributes lock = attributes.isDirectory() ? attributes(path.resolve(LOCK_FILE)) : null;
        if (lock != null && lock.lastModifiedTime().toInstant().isAfter(newest)) newest = lock.lastModifiedTime().toInstant();
        return newest.isBefore(cutoff);
    }

    private static BasicFileAttributes attributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    static UserPrincipal currentUser() throws IOException {
        // The process's own account, not the overridable user.name property.
        String name = ProcessHandle.current().info().user().orElseGet(() -> System.getProperty("user.name"));
        return FileSystems.getDefault().getUserPrincipalLookupService().lookupPrincipalByName(name);
    }

    private static long sizeOf(Path path) {
        long[] total = {0};
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
                    if (attributes.isRegularFile()) total[0] += attributes.size();
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException e) {
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException | RuntimeException e) {
            // a partial size is enough for the report
        }
        return total[0];
    }

    /**
     * Best-effort recursive delete that never follows a symbolic link: a link is removed, not its
     * target. Walks again, up to {@value #DELETE_PASSES} times, while threads that are shutting
     * down still add files. A pass that runs out of heap (other threads' shards may still fill it)
     * is retried like any other.
     */
    static void deleteTree(Path root) {
        for (int pass = 0; pass < DELETE_PASSES && Files.exists(root, LinkOption.NOFOLLOW_LINKS); pass++) {
            if (pass >= 2) LockSupport.parkNanos(DELETE_PASS_PAUSE_NANOS);
            try {
                Files.walkFileTree(root, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
                        deleteQuietly(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException e) {
                        deleteQuietly(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException e) {
                        deleteQuietly(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (IOException | RuntimeException | OutOfMemoryError ignored) {
                // retried, then left for the next start's sweep
            }
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException | RuntimeException ignored) {
            // a directory that is not empty yet is retried by the next pass
        }
    }

    private static void closeQuietly(Closeable lockFile) {
        try {
            lockFile.close();
        } catch (IOException ignored) {
            // releases the lock either way
        }
    }

    private static String mib(long bytes) {
        return (bytes + MIB - 1) / MIB + " MiB";
    }
}
