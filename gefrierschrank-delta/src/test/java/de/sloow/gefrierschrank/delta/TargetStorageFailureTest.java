package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.DiskChecker;
import org.apache.hadoop.util.Progressable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A full or failing target: Hadoop's local filesystem throws FSError, an Error, for any
 * I/O error on a stream, and S3A's block buffers a DiskErrorException. Both become a
 * TargetStorageException that ends the index instead of escaping as an Error or quarantining
 * shard after shard; no staged file survives unless the log references it.
 */
class TargetStorageFailureTest {

    private static final java.nio.file.Path FIXTURE_REPO =
        java.nio.file.Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");
    private static final String INDEX_NAME = "test_data_7_17_9";
    private static final ArchiveContext ARCHIVE =
        new ArchiveContext(INDEX_NAME, "repo-index-id", "snap-uuid", "snap", INDEX_NAME, "snap-uuid");

    @AfterEach
    void reset() {
        FaultyFileSystem.reset();
        FaultInjectingLogStore.reset();
        DeltaTableWriter.runRootEnded = de.sloow.gefrierschrank.core.TempRoot::hasEnded;
    }

    private static Configuration faultyConf() {
        var conf = new Configuration();
        conf.setClass("fs.file.impl", FaultyFileSystem.class, FileSystem.class);
        conf.setBoolean("fs.file.impl.disable.cache", true);
        return conf;
    }

    private static List<Iterator<ArchiveRow>> rows(int count) {
        var rows = new ArrayList<ArchiveRow>();
        for (int doc = 0; doc < count; doc++) {
            byte[] source = ("{\"doc\":" + doc + ",\"text\":\"" + "x".repeat(2000) + "\"}").getBytes(StandardCharsets.UTF_8);
            rows.add(new ArchiveRow(source, source.length, new DocumentMetadata("id-" + doc, null, "stored", doc)));
        }
        return List.of(rows.iterator());
    }

    private static void assertIsStorageFailure(Throwable failure, String reason) {
        var storage = assertInstanceOf(TargetStorageException.class, failure);
        assertEquals("target storage failure (" + reason + ")", storage.getMessage());
        assertSame(storage, de.sloow.gefrierschrank.core.FailureDiagnostics.rootCause(storage),
            "reports name TargetStorageException as the root cause");
        assertEquals(1, storage.getSuppressed().length, "the original failure stays attached");
    }

    @Test
    void anFsErrorWhileStagingEndsAsTargetStorageExceptionWithoutFiles(@TempDir java.nio.file.Path tmp) throws Exception {
        var conf = faultyConf();
        FaultyFileSystem.failWrites(name -> name.endsWith(".parquet"),
            () -> FaultInjectingLogStore.fsError("No space left on device"));
        String table = tmp.resolve("table").toString();
        var writer = new DeltaTableWriter(table);

        var failure = assertThrows(TargetStorageException.class,
            () -> writer.writeArchive(conf, rows(500), ARCHIVE, 0, ARCHIVE.key(0)));
        assertIsStorageFailure(failure, "no space left, FSError");
        assertInstanceOf(FSError.class, failure.getSuppressed()[0]);
        assertEquals(Set.of(), ParquetFooters.parquetFilesOnDisk(tmp.resolve("table")), "staged files are gone");
        assertFalse(Files.exists(tmp.resolve("table/_delta_log")), "nothing was committed");

        var legacy = assertThrows(TargetStorageException.class, () -> writer.write(conf,
            List.of("{\"a\":1}").iterator(), ArchiveContext.mapping(), ARCHIVE.key(1)));
        assertIsStorageFailure(legacy, "no space left, FSError");
        assertEquals(Set.of(), ParquetFooters.parquetFilesOnDisk(tmp.resolve("table")));

        FaultyFileSystem.reset();
        assertEquals(new DeltaTableWriter.WriteResult(500, false),
            writer.writeArchive(conf, rows(500), ARCHIVE, 0, ARCHIVE.key(0)), "a retry once space is back commits");
    }

    @Test
    void theReasonUsesAFixedVocabulary(@TempDir java.nio.file.Path tmp) {
        var conf = faultyConf();
        String table = tmp.resolve("table").toString();
        for (var errno : List.of(List.of("File too large", "file too large"), List.of("Disk quota exceeded", "quota exceeded"),
                List.of("Input/output error", "I/O error"))) {
            FaultyFileSystem.failWrites(name -> name.endsWith(".parquet"), () -> FaultInjectingLogStore.fsError(errno.get(0)));
            var failure = assertThrows(TargetStorageException.class,
                () -> new DeltaTableWriter(table).writeArchive(conf, rows(200), ARCHIVE, 0, ARCHIVE.key(0)));
            assertIsStorageFailure(failure, errno.get(1) + ", FSError");
        }
    }

    /** S3A allocates its upload blocks from fs.s3a.buffer.dir through LocalDirAllocator. */
    @Test
    void aDiskErrorOfUploadBuffersIsATargetStorageFailure(@TempDir java.nio.file.Path tmp) {
        var conf = faultyConf();
        FaultyFileSystem.failCreates(name -> name.endsWith(".parquet"),
            () -> new DiskChecker.DiskErrorException("No space available in any of the local directories."));
        String table = tmp.resolve("table").toString();
        var failure = assertThrows(TargetStorageException.class,
            () -> new DeltaTableWriter(table).writeArchive(conf, rows(10), ARCHIVE, 0, ARCHIVE.key(0)));
        assertIsStorageFailure(failure, "no space left, DiskErrorException");
    }

    /**
     * While the process exits, a file holds its temp run root's name so that no thread can
     * recreate the root (TempRoot), and S3A then cannot allocate a buffer there: that says nothing
     * about the disk, and a terminated run must not report a target storage failure.
     */
    @Test
    void aBufferRefusedWhileTheProcessExitsIsNoTargetStorageFailure(@TempDir java.nio.file.Path tmp) throws Exception {
        var conf = faultyConf();
        FaultyFileSystem.failCreates(name -> name.endsWith(".parquet"),
            () -> new DiskChecker.DiskErrorException("Could not find any valid local directory for s3ablock-0001-"));
        DeltaTableWriter.runRootEnded = () -> true;
        String table = tmp.resolve("table").toString();
        var failure = assertThrows(IOException.class,
            () -> new DeltaTableWriter(table).writeArchive(conf, rows(10), ARCHIVE, 0, ARCHIVE.key(0)));
        assertNull(de.sloow.gefrierschrank.core.FailureDiagnostics.find(failure, TargetStorageException.class), failure.toString());
        assertNotNull(de.sloow.gefrierschrank.core.FailureDiagnostics.find(failure, DiskChecker.DiskErrorException.class));
        assertEquals(Set.of(), ParquetFooters.parquetFilesOnDisk(tmp.resolve("table")), "staged files are gone");

        DeltaTableWriter.runRootEnded = () -> false;
        assertThrows(TargetStorageException.class,
            () -> new DeltaTableWriter(table).writeArchive(conf, rows(10), ARCHIVE, 0, ARCHIVE.key(0)));
    }

    /**
     * The memory gate has the writer close its open file between two rows, from within the row
     * iterator, which wraps the failure in an IllegalStateException: an S3A buffer-disk error there
     * is still a target storage failure.
     */
    @Test
    void aDiskErrorClosingAFileForTheMemoryGateIsATargetStorageFailure(@TempDir java.nio.file.Path tmp) throws Exception {
        var conf = faultyConf();
        conf.set(DeltaTableWriter.PARQUET_CODEC_KEY, "uncompressed");
        FaultyFileSystem.failWritesOnceArmed(name -> name.endsWith(".parquet"),
            () -> new DiskChecker.DiskErrorException("No space available in any of the local directories."));
        int twelve = 12 << 20;
        ShardReader reader = new ShardReader() {
            @Override public String runtimeName() { return "large"; }
            @Override public int docCount() { return 2; }
            @Override public void close() {}
            @Override public DocumentMetadata documentMetadata(int docId) {
                return new DocumentMetadata("id-" + docId, null, "stored", docId);
            }

            @Override public void readSource(int docId, SourceConsumer consumer) {
                if (docId == 1) FaultyFileSystem.arm();
                byte[] bytes = DocumentMemoryGateTest.source(0, docId, twelve);
                consumer.accept(bytes, bytes.length);
            }
        };
        // Budget 48 MiB: the second 12 MiB document (36 MiB) fits only once the file keeping two
        // copies of the first (24 MiB) is closed.
        var gate = new DocumentMemoryGate(64L << 20, 8L << 20, 3.0, 0);
        var rows = new ShardRanges.ArchiveRowIterator(reader, 0, -1, null, gate, tmp);
        String table = tmp.resolve("table").toString();
        var failure = assertThrows(TargetStorageException.class,
            () -> new DeltaTableWriter(table).writeArchive(conf, List.of(rows), ARCHIVE, 0, ARCHIVE.key(0)));
        assertIsStorageFailure(failure, "no space left, DiskErrorException");
        var wrapped = assertInstanceOf(IllegalStateException.class, failure.getSuppressed()[0]);
        assertEquals("reading _source of doc 1", wrapped.getMessage(), "the close for the gate failed inside the iterator");
        assertEquals(Set.of(), ParquetFooters.parquetFilesOnDisk(tmp.resolve("table")), "staged files are gone");
        rows.close();
    }

    @Test
    void anFsErrorOnTheCommitWithTheKeyAbsentDeletesTheFilesAtOnce(@TempDir java.nio.file.Path tmp) throws Exception {
        var conf = FaultInjectingLogStore.enable(new Configuration());
        String table = tmp.resolve("table").toString();
        var writer = new DeltaTableWriter(table);
        assertEquals(new DeltaTableWriter.WriteResult(10, false), writer.writeArchive(conf, rows(10), ARCHIVE, 0, ARCHIVE.key(0)));

        FaultInjectingLogStore.failNextCommits(FaultInjectingLogStore.Mode.FS_ERROR_BEFORE_WRITE, 1);
        var failure = assertThrows(TargetStorageException.class,
            () -> writer.writeArchive(conf, rows(10), ARCHIVE, 1, ARCHIVE.key(1)));
        assertIsStorageFailure(failure, "no space left, FSError");
        assertEquals(Set.of(ARCHIVE.key(0)), writer.committedShardKeys(conf));
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(tmp.resolve("table")),
            "no orphan once the log shows the key absent, without waiting for the end of the run");
    }

    @Test
    void anFsErrorAfterTheCommitLandedKeepsTheShard(@TempDir java.nio.file.Path tmp) throws Exception {
        var conf = FaultInjectingLogStore.enable(new Configuration());
        String table = tmp.resolve("table").toString();
        var writer = new DeltaTableWriter(table);
        FaultInjectingLogStore.failNextCommits(FaultInjectingLogStore.Mode.FS_ERROR_AFTER_WRITE, 1);
        assertEquals(new DeltaTableWriter.WriteResult(10, false), writer.writeArchive(conf, rows(10), ARCHIVE, 0, ARCHIVE.key(0)));
        assertEquals(Set.of(ARCHIVE.key(0)), writer.committedShardKeys(conf));
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(tmp.resolve("table")));
        assertFalse(ParquetFooters.addFilePaths(conf, table).isEmpty());
    }

    @Test
    void failuresReadingTheLogAreTranslatedEverywhere(@TempDir java.nio.file.Path tmp) throws Exception {
        String table = tmp.resolve("table").toString();
        var plain = new Configuration();
        new DeltaTableWriter(table).writeArchive(plain, rows(3), ARCHIVE, 0, ARCHIVE.key(0));
        var conf = faultyConf();
        FaultyFileSystem.failOpens(name -> name.endsWith(".json"), () -> FaultInjectingLogStore.fsError("Input/output error"));

        var keys = assertThrows(java.io.UncheckedIOException.class, () -> new DeltaTableWriter(table).committedShardKeys(conf));
        assertIsStorageFailure(keys.getCause(), "I/O error, FSError");
        assertIsStorageFailure(assertThrows(TargetStorageException.class,
            () -> new DeltaTableWriter(table).requireArchiveSnapshot(conf, ARCHIVE, 2)), "I/O error, FSError");
        assertIsStorageFailure(assertThrows(TargetStorageException.class,
            () -> new DeltaTableWriter(table).writeArchive(conf, rows(3), ARCHIVE, 1, ARCHIVE.key(1))), "I/O error, FSError");
        assertDoesNotThrow(() -> new DeltaTableWriter(table).reconcilePendingCleanup(conf));
        assertEquals(ParquetFooters.addFilePaths(plain, table), ParquetFooters.parquetFilesOnDisk(tmp.resolve("table")));
    }

    @Test
    void otherErrorsAreNeverTranslated(@TempDir java.nio.file.Path tmp) {
        var conf = faultyConf();
        String table = tmp.resolve("table").toString();
        FaultyFileSystem.failWrites(name -> name.endsWith(".parquet"), () -> new OutOfMemoryError("simulated"));
        assertThrows(OutOfMemoryError.class, () -> new DeltaTableWriter(table).writeArchive(conf, rows(200), ARCHIVE, 0, ARCHIVE.key(0)));
        FaultyFileSystem.failWrites(name -> name.endsWith(".parquet"), () -> new StackOverflowError());
        assertThrows(StackOverflowError.class, () -> new DeltaTableWriter(table).writeArchive(conf, rows(200), ARCHIVE, 0, ARCHIVE.key(0)));
        FaultyFileSystem.failWrites(name -> name.endsWith(".parquet"), () -> new IOException("plain I/O failure"));
        var plain = assertThrows(IOException.class,
            () -> new DeltaTableWriter(table).writeArchive(conf, rows(200), ARCHIVE, 0, ARCHIVE.key(0)));
        assertFalse(plain instanceof TargetStorageException, "only FSError and DiskErrorException are storage failures");
    }

    /** A target storage failure ends the index; unlike a quarantine, no further shard is started. */
    @Test
    void aTargetStorageFailureEndsTheIndexInsteadOfQuarantining(@TempDir java.nio.file.Path tmp) throws Exception {
        String table = tmp.resolve("table").toString();
        var conf = FaultInjectingLogStore.enable(new Configuration());
        var opened = new ArrayList<Integer>();
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> {
            int shardId = shardIdOf(shard);
            opened.add(shardId);
            if (shardId == 1) FaultInjectingLogStore.failNextCommits(FaultInjectingLogStore.Mode.FS_ERROR_BEFORE_WRITE, 1);
            return new RowsReader(3);
        };
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var pipeline = new DumpToDeltaPipeline(new RepositoryInspector(store), store, new DeltaTableWriter(table), conf, opener);
        assertThrows(TargetStorageException.class,
            () -> pipeline.runSnapshot(ARCHIVE, null, catalog(4), RunReportListener.NO_OP));
        assertEquals(List.of(0, 1), opened);
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(tmp.resolve("table")));
    }

    /**
     * Several ranges of a shard can fail together, the first failure carrying the others as
     * suppressed: a full target found by any of them ends the index, never a quarantine.
     */
    @Test
    void aTargetStorageFailureSuppressedByAnotherFailureStillEndsTheIndex(@TempDir java.nio.file.Path tmp) throws Exception {
        String table = tmp.resolve("table").toString();
        var opened = new ArrayList<Integer>();
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> {
            int shardId = shardIdOf(shard);
            opened.add(shardId);
            if (shardId != 1) return new RowsReader(3);
            return new ShardReader() {
                @Override public String runtimeName() { return "fake"; }
                @Override public int docCount() { return 3; }
                @Override public void close() {}
                @Override public void readSource(int docId, SourceConsumer consumer) {
                    var first = new java.io.UncheckedIOException(new IOException("another range's failure"));
                    first.addSuppressed(new TargetStorageException("no space left, FSError", null));
                    throw first;
                }
            };
        };
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var pipeline = new DumpToDeltaPipeline(new RepositoryInspector(store), store, new DeltaTableWriter(table),
            new Configuration(), opener);
        assertThrows(TargetStorageException.class,
            () -> pipeline.runSnapshot(ARCHIVE, null, catalog(4), RunReportListener.NO_OP));
        assertEquals(List.of(0, 1), opened, "no shard starts after the target filled up");
    }

    /** At parallelism 2 the shard in flight next to the failing one drains, and its temp dir goes. */
    @Test
    @Tag("concurrency")
    @Timeout(120)
    void atParallelismTwoTheOtherShardDrainsAndCleansUp(@TempDir java.nio.file.Path tmp) throws Exception {
        String table = tmp.resolve("table").toString();
        var conf = FaultInjectingLogStore.enable(new Configuration());
        FaultInjectingLogStore.failNextCommits(FaultInjectingLogStore.Mode.FS_ERROR_BEFORE_WRITE, 1);
        var bothOpen = new CyclicBarrier(2);
        var dirs = java.util.Collections.synchronizedList(new ArrayList<java.nio.file.Path>());
        var opened = java.util.Collections.synchronizedList(new ArrayList<Integer>());
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> {
            dirs.add(dir);
            opened.add(shardIdOf(shard));
            bothOpen.await(60, TimeUnit.SECONDS);
            return new RowsReader(50);
        };
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var pool = new ShardWorkPool(2, 0);
        var last = new java.util.concurrent.atomic.AtomicReference<RunReport>();
        var pipeline = new DumpToDeltaPipeline(new RepositoryInspector(store), store, new DeltaTableWriter(table), conf, opener)
            .withWorkPool(pool);

        assertThrows(TargetStorageException.class, () -> pipeline.runSnapshot(ARCHIVE, null, catalog(6), last::set));

        assertTrue(pool.awaitIdle(60, TimeUnit.SECONDS));
        assertEquals(Set.of(0, 1), Set.copyOf(opened), "admission stopped at the failure");
        assertEquals(2, dirs.size());
        for (var dir : dirs) assertFalse(Files.exists(dir), dir + " must be removed");
        var committed = new DeltaTableWriter(table).committedShardKeys(new Configuration());
        assertEquals(1, committed.size(), "the other shard finished and was recorded");
        assertNotNull(last.get());
        assertEquals(1, last.get().committedCount());
        assertEquals(0, last.get().quarantinedCount(), "a storage failure is never a quarantine");
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(tmp.resolve("table")));
    }

    /** The exception's source is pinned to this exact text. */
    @Test
    void theExceptionSourceIsExactlyTheSharedText() throws IOException {
        String expected = """
            package de.sloow.gefrierschrank.delta;

            import java.io.IOException;

            /**
             * The Delta target could not store data: disk full, file too large, quota or I/O error. Fatal for
             * the whole run, because every later shard would be downloaded in full only to fail the same way.
             */
            public final class TargetStorageException extends IOException {
                public TargetStorageException(String reason, Throwable cause) {
                    super("target storage failure (" + reason + ")", cause);
                }
            }
            """;
        assertEquals(expected, Files.readString(
            java.nio.file.Path.of("src/main/java/de/sloow/gefrierschrank/delta/TargetStorageException.java")));
    }

    private static int shardIdOf(ShardSnapshot shard) {
        return Integer.parseInt(shard.snapshotName().substring("virtual-shard-".length()));
    }

    private static DumpToDeltaPipeline.ShardCatalog catalog(int shards) throws IOException {
        ShardSnapshot template = new RepositoryInspector(new FileSystemBlobStore(FIXTURE_REPO)).shardSnapshot(INDEX_NAME, 0);
        return new DumpToDeltaPipeline.ShardCatalog() {
            @Override public int shardCount() { return shards; }

            @Override public ShardSnapshot shardSnapshot(int shardId) {
                return new ShardSnapshot(template.repoUuid(), template.shardPrefix(), "virtual-shard-" + shardId,
                    template.indexVersion(), template.totalSize(), template.files());
            }
        };
    }

    static final class RowsReader implements ShardReader {
        private final int rows;

        RowsReader(int rows) {
            this.rows = rows;
        }

        @Override public String runtimeName() { return "fake"; }
        @Override public int docCount() { return rows; }
        @Override public void close() {}

        @Override public void readSource(int docId, SourceConsumer consumer) {
            byte[] bytes = ("{\"row\":" + docId + "}").getBytes(StandardCharsets.UTF_8);
            consumer.accept(bytes, bytes.length);
        }
    }

    /** Local filesystem whose streams fail on demand, the way a full or broken disk does. */
    public static class FaultyFileSystem extends LocalFileSystem {
        private static volatile Predicate<String> failingWrites = name -> false;
        private static volatile Predicate<String> failingCreates = name -> false;
        private static volatile Predicate<String> failingOpens = name -> false;
        private static volatile Predicate<String> armableWrites = name -> false;
        private static volatile boolean armed;
        private static volatile Supplier<Throwable> failure = () -> null;

        public FaultyFileSystem() {
            super(new FaultyRaw());
        }

        static void failWrites(Predicate<String> names, Supplier<Throwable> thrown) {
            failure = thrown;
            failingWrites = names;
        }

        static void failCreates(Predicate<String> names, Supplier<Throwable> thrown) {
            failure = thrown;
            failingCreates = names;
        }

        static void failOpens(Predicate<String> names, Supplier<Throwable> thrown) {
            failure = thrown;
            failingOpens = names;
        }

        /** Streams of {@code names} created from now on fail their writes once {@link #arm} is called. */
        static void failWritesOnceArmed(Predicate<String> names, Supplier<Throwable> thrown) {
            failure = thrown;
            armed = false;
            armableWrites = names;
        }

        static void arm() {
            armed = true;
        }

        static void reset() {
            failingWrites = name -> false;
            failingCreates = name -> false;
            failingOpens = name -> false;
            armableWrites = name -> false;
            armed = false;
        }

        static void fail() throws IOException {
            Throwable thrown = failure.get();
            if (thrown instanceof IOException io) throw io;
            if (thrown instanceof Error error) throw error;
            throw new AssertionError(thrown);
        }

        static final class FaultyRaw extends RawLocalFileSystem {
            @Override
            public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                             short replication, long blockSize, Progressable progress) throws IOException {
                if (failingCreates.test(f.getName())) fail();
                FSDataOutputStream out = super.create(f, permission, overwrite, bufferSize, replication, blockSize, progress);
                return wrapped(f, out);
            }

            @Override
            public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize,
                                             Progressable progress) throws IOException {
                if (failingCreates.test(f.getName())) fail();
                FSDataOutputStream out = super.create(f, overwrite, bufferSize, replication, blockSize, progress);
                return wrapped(f, out);
            }

            @Override
            public FSDataInputStream open(Path f, int bufferSize) throws IOException {
                if (failingOpens.test(f.getName())) fail();
                return super.open(f, bufferSize);
            }

            private static FSDataOutputStream wrapped(Path f, FSDataOutputStream out) {
                if (failingWrites.test(f.getName())) return failing(out);
                if (armableWrites.test(f.getName())) return failingOnceArmed(out);
                return out;
            }

            private static FSDataOutputStream failingOnceArmed(FSDataOutputStream out) {
                return new FSDataOutputStream(new OutputStream() {
                    @Override public void write(int b) throws IOException {
                        if (armed) fail();
                        out.write(b);
                    }

                    @Override public void write(byte[] b, int off, int len) throws IOException {
                        if (armed) fail();
                        out.write(b, off, len);
                    }

                    @Override public void close() throws IOException {
                        out.close();
                    }
                }, null);
            }

            private static FSDataOutputStream failing(FSDataOutputStream out) {
                return new FSDataOutputStream(new OutputStream() {
                    @Override public void write(int b) throws IOException {
                        fail();
                    }

                    @Override public void write(byte[] b, int off, int len) throws IOException {
                        fail();
                    }

                    @Override public void close() throws IOException {
                        out.close();
                    }
                }, null);
            }
        }
    }
}
