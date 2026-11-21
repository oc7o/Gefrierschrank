package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TableWriteLockTest {

    private static final Path FROZEN_REPO = Path.of("../gefrierschrank-core/src/test/resources/fixtures/frozen-repo");
    private static final String INDEX = "frozen-docs-f5ffca09";

    @Test
    void aTableLeftEmptyIsRemovedAgainOnRelease(@TempDir Path tmp) throws Exception {
        Path table = tmp.resolve("root/table");
        try (var lock = TableWriteLock.acquire(table.toString())) {
            assertNotNull(lock);
            assertTrue(Files.isRegularFile(table.resolve(TableWriteLock.FILE_NAME)));
        }
        assertFalse(Files.exists(table), "the export created the directory and wrote nothing into it");
        assertTrue(Files.isDirectory(tmp.resolve("root")), "only the directory the lock created is removed");

        Path existing = Files.createDirectories(tmp.resolve("existing"));
        TableWriteLock.acquire(existing.toString()).close();
        assertTrue(Files.isDirectory(existing), "a directory that was already there stays");
        try (var entries = Files.list(existing)) {
            assertEquals(0, entries.count());
        }
    }

    /**
     * A rerun that only verifies a complete archive made read-only must keep working, as before the
     * lock existed: nothing this process does can change a directory it cannot write. A writable
     * directory whose lock file alone is read-only is still refused.
     */
    @Test
    void aTableDirectoryThisProcessCannotWriteIsExportedWithoutALock(@TempDir Path tmp) throws Exception {
        var readOnly = java.nio.file.attribute.PosixFilePermissions.fromString("r-xr-xr-x");
        var writable = java.nio.file.attribute.PosixFilePermissions.fromString("rwxr-xr-x");
        Path withLockFile = Files.createDirectories(tmp.resolve("with-lock-file"));
        Path lockFile = Files.writeString(withLockFile.resolve(TableWriteLock.FILE_NAME), "gefrierschrank pid 1\n");
        Files.setPosixFilePermissions(lockFile, java.nio.file.attribute.PosixFilePermissions.fromString("r--r--r--"));
        Path withoutLockFile = Files.createDirectories(tmp.resolve("without-lock-file"));
        var err = new ByteArrayOutputStream();
        PrintStream previous = System.err;
        try {
            assertThrows(java.nio.file.AccessDeniedException.class, () -> TableWriteLock.acquire(withLockFile.toString()),
                "a writable table whose lock file cannot be opened is not silently unlocked");
            Files.setPosixFilePermissions(withLockFile, readOnly);
            Files.setPosixFilePermissions(withoutLockFile, readOnly);
            System.setErr(new PrintStream(err, true));
            assertNull(TableWriteLock.acquire(withLockFile.toString()));
            assertNull(TableWriteLock.acquire(withoutLockFile.toString()));
        } finally {
            System.setErr(previous);
            Files.setPosixFilePermissions(withLockFile, writable);
            Files.setPosixFilePermissions(withoutLockFile, writable);
            Files.setPosixFilePermissions(lockFile, java.nio.file.attribute.PosixFilePermissions.fromString("rw-r--r--"));
        }
        assertEquals(2, err.toString().lines().filter(l -> l.startsWith("warning: the table directory is not writable")).count(),
            err.toString());
        TableWriteLock.acquire(withLockFile.toString()).close();
        TableWriteLock.acquire(withoutLockFile.toString()).close();
    }

    @Test
    void aSecondWriterInThisJvmIsRefusedHoweverTheTableIsNamed(@TempDir Path tmp) throws Exception {
        Path table = tmp.resolve("table");
        Path link = Files.createSymbolicLink(tmp.resolve("link"), Files.createDirectories(tmp.resolve("real")));
        try (var lock = TableWriteLock.acquire(table.toString());
             var viaLink = TableWriteLock.acquire(link.resolve("t").toString())) {
            for (String spelling : List.of(table.toString(), table.toUri().toString(), "file:" + table,
                    tmp.resolve("x/../table").toString(), table + "/")) {
                var refused = assertThrows(TableWriteLock.TableLockedException.class,
                    () -> TableWriteLock.acquire(spelling), spelling);
                assertFalse(refused.getMessage().contains(tmp.toString()), "fixed message without the path");
            }
            assertThrows(TableWriteLock.TableLockedException.class,
                () -> TableWriteLock.acquire(tmp.resolve("real/t").toString()), "the symlink's target");
        }
        TableWriteLock.acquire(table.toString()).close();
    }

    @Test
    void theLockFileStaysBesideTheTableAndDeltaReadsIgnoreIt(@TempDir Path tmp) throws Exception {
        var conf = new Configuration();
        Path table = tmp.resolve("table");
        var context = new ArchiveContext(INDEX, "rid", "snap", "name", INDEX, "snap");
        try (var lock = TableWriteLock.acquire(table.toString())) {
            ArchiveGuardTest.writeShard(new DeltaTableWriter(table.toString()), conf, context, 0);
        }
        assertTrue(Files.isRegularFile(table.resolve(TableWriteLock.FILE_NAME)));
        var log = io.delta.standalone.DeltaLog.forTable(conf, table.toString());
        try (var rows = log.snapshot().open()) {
            assertTrue(rows.hasNext());
        }
        assertEquals(ParquetFooters.addFilePaths(conf, table.toString()), ParquetFooters.parquetFilesOnDisk(table));
        assertEquals(java.util.Set.of(context.key(0)), new DeltaTableWriter(table.toString()).committedShardKeys(conf));
        TableWriteLock.acquire(table.toString()).close();
        assertTrue(Files.isRegularFile(table.resolve(TableWriteLock.FILE_NAME)), "reused, not removed");
    }

    @Test
    void aFileReplacedAtThePathIsNotTheSameFile(@TempDir Path tmp) throws Exception {
        Path file = Files.createFile(tmp.resolve(TableWriteLock.FILE_NAME));
        Object opened = TableWriteLock.fileKey(file);
        assertEquals(opened, TableWriteLock.fileKey(file));
        try (var stale = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            Files.delete(file);
            assertNull(TableWriteLock.fileKey(file), "unlinked by the previous holder");
            Files.writeString(file, "a newer holder's file");
            assertNotEquals(opened, TableWriteLock.fileKey(file));
        }
    }

    /**
     * POSIX drops a process's lock once it closes any descriptor of the file, so refusing a second
     * writer in this JVM must not open the file. Another process then still finds it locked.
     */
    @Test
    void refusingASecondWriterInThisJvmKeepsTheLockForOtherProcesses(@TempDir Path tmp) throws Exception {
        Path table = tmp.resolve("table");
        Path probe = Files.writeString(tmp.resolve("LockProbe.java"), """
            public class LockProbe {
                public static void main(String[] args) throws Exception {
                    try (var channel = java.nio.channels.FileChannel.open(java.nio.file.Path.of(args[0]),
                            java.nio.file.StandardOpenOption.WRITE)) {
                        System.exit(channel.tryLock() == null ? 0 : 1);
                    }
                }
            }
            """);
        try (var lock = TableWriteLock.acquire(table.toString())) {
            for (int i = 0; i < 3; i++) {
                assertThrows(TableWriteLock.TableLockedException.class, () -> TableWriteLock.acquire(table.toString()));
                assertThrows(TableWriteLock.TableLockedException.class,
                    () -> TableWriteLock.acquire(table.toUri().toString()));
            }
            var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                probe.toString(), table.resolve(TableWriteLock.FILE_NAME).toString()).inheritIO().start();
            assertTrue(process.waitFor(60, java.util.concurrent.TimeUnit.SECONDS));
            assertEquals(0, process.exitValue(), "another process must still find the table locked");
        }
    }

    @Test
    void onlyLocalTablesAreLocked() throws Exception {
        assertNull(TableWriteLock.acquire("s3a://bucket/prefix/table"));
        assertNull(TableWriteLock.localDirectory("s3a://bucket/prefix/table"));
        assertEquals(Path.of("/data/table"), TableWriteLock.localDirectory("file:///data/table"));
        assertEquals(Path.of("/data/table"), TableWriteLock.localDirectory("/data/./table"));
        assertEquals(Path.of("rel").toAbsolutePath(), TableWriteLock.localDirectory("rel"));
    }

    @Test
    void anS3aTargetWarnsOncePerRunThatWritersAreNotDetected(@TempDir Path tmp) throws Exception {
        var conf = new Configuration();
        // Any s3a:// access fails at once instead of reaching the network.
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        var sources = new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(FROZEN_REPO)));
        var exporter = new FullIndexExporter(sources, conf, (shard, dir) -> new Lucene10ShardReader(dir));
        var err = new ByteArrayOutputStream();
        PrintStream previous = System.err;
        System.setErr(new PrintStream(err, true));
        try {
            for (int run = 0; run < 2; run++) {
                assertThrows(Exception.class, () -> exporter.run(INDEX, "s3a://bucket/root/" + INDEX, RunReportListener.NO_OP));
            }
        } finally {
            System.setErr(previous);
        }
        var warnings = err.toString().lines().filter(l -> l.contains("are not detected")).toList();
        assertEquals(List.of("warning: concurrent writers on one s3a:// table are not detected; run one export per table"),
            warnings);
    }
}
