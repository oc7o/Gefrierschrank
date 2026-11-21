package de.sloow.gefrierschrank.delta;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * One writer per local table: an exclusive {@link FileChannel#tryLock} on
 * {@code <table>/_gefrierschrank.lock}, held while an index is exported. A second writer, in
 * another process or this one, fails at once instead of corrupting the log or downloading and
 * decoding every shard only to find it committed. Delta readers and vacuum ignore files whose
 * name starts with an underscore.
 *
 * <p>The OS lock belongs to the process, and POSIX drops it when the process closes ANY
 * descriptor of the file. So while a lock is held nothing in this JVM opens the file again: a
 * second acquisition is refused by the table directory's identity before it opens anything, and
 * the held file is only ever inspected through its path's attributes.
 *
 * <p>A table the export wrote nothing into loses the file (and the directory, if the export
 * created it) again on release, still under the lock. A writer that opened the file just before
 * that unlink would lock a file no longer at the path, so every writer checks after locking that
 * the path still names the file it opened, and retries otherwise.
 */
public final class TableWriteLock implements AutoCloseable {

    static final String FILE_NAME = "_gefrierschrank.lock";
    private static final int ATTEMPTS = 8;

    private static final Set<Object> HELD = ConcurrentHashMap.newKeySet();

    /** Another writer holds the table: fixed message, so it may appear in reports and on stderr. */
    public static final class TableLockedException extends IOException {
        public TableLockedException() {
            super("another process is writing this table (" + FILE_NAME + " is locked); run one export per table");
        }
    }

    private final Path directory;
    private final Object key;
    private final boolean createdDirectory;
    private final FileChannel channel;
    private final FileLock lock;

    private TableWriteLock(Path directory, Object key, boolean createdDirectory, FileChannel channel, FileLock lock) {
        this.directory = directory;
        this.key = key;
        this.createdDirectory = createdDirectory;
        this.channel = channel;
        this.lock = lock;
    }

    /**
     * Locks a local table (no scheme, or {@code file:}), creating its directory if needed. Returns
     * null, locking nothing, for any other scheme, where the file system refuses locks, and for a
     * table directory this process cannot write: nothing it does can change such a table, and a
     * rerun that only verifies a complete, read-only archive must keep working.
     */
    static TableWriteLock acquire(String tableUri) throws IOException {
        Path directory = localDirectory(tableUri);
        if (directory == null) return null;
        boolean created = !Files.isDirectory(directory);
        Files.createDirectories(directory);
        Object key = identity(directory);
        if (!HELD.add(key)) throw new TableLockedException();
        try {
            Path file = directory.resolve(FILE_NAME);
            for (int attempt = 1; attempt <= ATTEMPTS; attempt++) {
                Files.createDirectories(directory);
                try {
                    Files.createFile(file);
                } catch (FileAlreadyExistsException existing) {
                    // Kept from an earlier export of this table.
                } catch (FileSystemException refused) {
                    if (Files.isWritable(directory)) throw refused;
                    return unwritable(key, refused);
                }
                Object opened = fileKey(file);
                if (opened == null) continue;
                FileChannel channel;
                try {
                    channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
                } catch (NoSuchFileException unlinked) {
                    continue;
                } catch (FileSystemException refused) {
                    if (Files.isWritable(directory)) throw refused;
                    return unwritable(key, refused);
                }
                FileLock lock;
                try {
                    lock = channel.tryLock();
                } catch (OverlappingFileLockException e) {
                    channel.close();
                    throw new TableLockedException();
                } catch (IOException unsupported) {
                    // A file system without locks: export exactly as before locks existed.
                    channel.close();
                    System.err.println("warning: cannot lock the table (" + unsupported.getClass().getSimpleName()
                        + "); concurrent writers on it are not detected");
                    HELD.remove(key);
                    return null;
                }
                if (lock == null) {
                    channel.close();
                    throw new TableLockedException();
                }
                try {
                    if (opened.equals(fileKey(file))) {
                        channel.truncate(0);
                        channel.write(ByteBuffer.wrap(("gefrierschrank pid " + ProcessHandle.current().pid() + "\n")
                            .getBytes(StandardCharsets.US_ASCII)), 0);
                        return new TableWriteLock(directory, key, created, channel, lock);
                    }
                } catch (IOException | RuntimeException | Error failure) {
                    // Closing the channel releases the lock; kept open, it would hold the table until exit.
                    try {
                        channel.close();
                    } catch (IOException close) {
                        failure.addSuppressed(close);
                    }
                    throw failure;
                }
                channel.close();
            }
            throw new TableLockedException();
        } catch (IOException | RuntimeException | Error e) {
            HELD.remove(key);
            throw e;
        }
    }

    private static TableWriteLock unwritable(Object key, FileSystemException refused) {
        System.err.println("warning: the table directory is not writable (" + refused.getClass().getSimpleName()
            + "), so it is exported without the table lock; an export that has to write to it fails");
        HELD.remove(key);
        return null;
    }

    /**
     * Releases the lock. A table this export left empty goes back to how it was found; failures
     * only warn, since the lock dies with the channel and the process anyway.
     */
    @Override
    public void close() {
        try {
            if (onlyTheLockFile(directory)) {
                Files.deleteIfExists(directory.resolve(FILE_NAME));
                if (createdDirectory) Files.deleteIfExists(directory);
            }
        } catch (DirectoryNotEmptyException raced) {
            // Something else created a file meanwhile: the directory stays.
        } catch (IOException | RuntimeException e) {
            System.err.println("warning: cannot remove an unused table lock file (" + e.getClass().getSimpleName() + ")");
        } finally {
            try {
                lock.release();
                channel.close();
            } catch (IOException e) {
                System.err.println("warning: cannot release a table lock (" + e.getClass().getSimpleName() + ")");
            } finally {
                HELD.remove(key);
            }
        }
    }

    private static boolean onlyTheLockFile(Path directory) throws IOException {
        try (var entries = Files.list(directory)) {
            return entries.allMatch(entry -> entry.getFileName().toString().equals(FILE_NAME));
        }
    }

    /** The table's directory when {@code tableUri} is a local path or {@code file:} URI, else null. */
    static Path localDirectory(String tableUri) {
        java.net.URI uri = new org.apache.hadoop.fs.Path(tableUri).toUri();
        String scheme = uri.getScheme();
        if (scheme != null && !scheme.equalsIgnoreCase("file")) return null;
        return Path.of(uri.getPath()).toAbsolutePath().normalize();
    }

    /**
     * Device and inode where the file system has them, so no spelling of the path escapes the
     * check, together with the real path: a file system that reuses a just-freed inode at once
     * (ext4, XFS) must not make another table look held while a released one is being removed.
     */
    private static Object identity(Path directory) throws IOException {
        Object fileKey = Files.readAttributes(directory, BasicFileAttributes.class).fileKey();
        Path realPath = directory.toRealPath();
        return fileKey != null ? java.util.List.of(fileKey, realPath) : realPath;
    }

    /**
     * The file's identity, read without opening it (a close would drop a held lock): null once it
     * is unlinked, and one shared value on file systems without file keys, which then trust the lock.
     */
    static Object fileKey(Path file) throws IOException {
        try {
            return Objects.requireNonNullElse(Files.readAttributes(file, BasicFileAttributes.class).fileKey(), "");
        } catch (NoSuchFileException unlinked) {
            return null;
        }
    }
}
