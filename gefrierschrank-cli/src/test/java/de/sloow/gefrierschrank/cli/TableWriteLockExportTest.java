package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.delta.FullIndexExporter;
import de.sloow.gefrierschrank.delta.RunReportListener;
import de.sloow.gefrierschrank.delta.TableWriteLock;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * One writer per local table: a second export of the same table, from another JVM or from
 * this one, fails before it reads the repository, and the first export completes undisturbed.
 */
@Tag("concurrency")
@Timeout(180)
class TableWriteLockExportTest {

    static final Path FROZEN = Path.of("../gefrierschrank-core/src/test/resources/fixtures/frozen-repo");
    static final String INDEX = "frozen-docs-f5ffca09";
    static final String OTHER_INDEX = "partial-frozen-docs-f5ffca09";
    static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void aSecondProcessOnTheSameTableFailsFastAndTheFirstCompletes(@TempDir Path tmp) throws Exception {
        Path root = tmp.resolve("root");
        Path table = root.resolve(INDEX);
        Path ready = tmp.resolve("ready");
        Path release = tmp.resolve("release");
        Process holder = java(tmp.resolve("holder.log"), LockHoldingExport.class.getName(), FROZEN.toString(), INDEX,
            table.toString(), ready.toString(), release.toString());
        try {
            awaitFile(ready, holder);
            long versionWhileHeld = logVersion(table);

            Path single = tmp.resolve("single.log");
            int singleExit = java(single, Main.class.getName(), "dump-to-delta", FROZEN.toString(), "--index", INDEX,
                "-o", table.toString()).waitFor();
            assertEquals(1, singleExit, Files.readString(single));
            String singleOutput = Files.readString(single);
            assertTrue(singleOutput.contains("error: TableLockedException: another process is writing this table"),
                singleOutput);
            assertFalse(singleOutput.contains("Exporting "), "refused before snapshot selection");

            Path reports = tmp.resolve("reports");
            Path all = tmp.resolve("all.log");
            int allExit = java(all, Main.class.getName(), "dump-to-delta", FROZEN.toString(), "--all-indices",
                "-o", root.toString(), "--report", reports.toString()).waitFor();
            assertEquals(4, allExit, Files.readString(all));
            var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
            for (var entry : manifest.path("indices")) {
                boolean locked = entry.path("indexName").asText().equals(INDEX);
                assertEquals(locked ? "FAILED" : "COMPLETED", entry.path("outcome").asText(), entry.toString());
                if (locked) assertEquals("TableLockedException", entry.path("errorClass").asText());
            }

            var store = new CountingStore(new FileSystemBlobStore(FROZEN));
            var exporter = new FullIndexExporter(new RepositorySources(new RepositorySources.Source("primary", store)),
                new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
            assertThrows(TableWriteLock.TableLockedException.class,
                () -> exporter.run(INDEX, table.toString(), RunReportListener.NO_OP));
            assertEquals(0, store.reads.get(), "not a single repository read, let alone a shard download");
            assertEquals(versionWhileHeld, logVersion(table), "the refused writers left the log alone");
        } finally {
            Files.createFile(release);
            if (!holder.waitFor(60, TimeUnit.SECONDS)) holder.destroyForcibly();
        }
        assertEquals(0, holder.exitValue(), Files.readString(tmp.resolve("holder.log")));

        String reference = tmp.resolve("reference").toString();
        exporter().run(INDEX, reference, RunReportListener.NO_OP);
        String[] columns = FullExportTest.columns(reference);
        assertEquals(FullExportTest.rows(reference, columns), FullExportTest.rows(table.toString(), columns),
            "every row exactly once");
        var rerun = exporter().run(INDEX, table.toString(), RunReportListener.NO_OP);
        assertEquals(rerun.totalShards(), rerun.skippedCount());
    }

    @Test
    void aSecondExportOfTheSameTableInThisJvmIsRefused(@TempDir Path tmp) throws Exception {
        String table = tmp.resolve("table").toString();
        var inside = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var first = new FullIndexExporter(sources(), new Configuration(), (shard, dir) -> {
            inside.countDown();
            assertTrue(release.await(60, TimeUnit.SECONDS));
            return new Lucene10ShardReader(dir);
        });
        var failure = new Throwable[1];
        Thread running = new Thread(() -> {
            try {
                first.run(INDEX, table, RunReportListener.NO_OP);
            } catch (Throwable e) {
                failure[0] = e;
            }
        });
        running.start();
        try {
            assertTrue(inside.await(60, TimeUnit.SECONDS));
            assertThrows(TableWriteLock.TableLockedException.class,
                () -> exporter().run(INDEX, table, RunReportListener.NO_OP));
            assertThrows(TableWriteLock.TableLockedException.class,
                () -> exporter().run(INDEX, tmp.resolve("x/../table").toString(), RunReportListener.NO_OP));
        } finally {
            release.countDown();
        }
        running.join(60_000);
        assertNull(failure[0]);
        assertEquals(2, exporter().run(INDEX, table, RunReportListener.NO_OP).skippedCount());
    }

    /**
     * Child-JVM export that holds its table lock inside the first shard until told to finish. It
     * gives up after a while on its own, so a test JVM killed mid-test leaves no child behind.
     */
    public static final class LockHoldingExport {
        public static void main(String[] args) throws Exception {
            Path ready = Path.of(args[3]);
            Path release = Path.of(args[4]);
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
            var exporter = new FullIndexExporter(
                new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(Path.of(args[0])))),
                new Configuration(), (shard, dir) -> {
                    if (!Files.exists(ready)) Files.createFile(ready);
                    while (!Files.exists(release)) {
                        if (System.nanoTime() > deadline) System.exit(5);
                        Thread.sleep(20);
                    }
                    return new Lucene10ShardReader(dir);
                });
            var report = exporter.run(args[1], args[2], RunReportListener.NO_OP);
            System.exit(report.committedCount() == report.totalShards() ? 0 : 3);
        }
    }

    private static FullIndexExporter exporter() {
        return new FullIndexExporter(sources(), new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
    }

    private static RepositorySources sources() {
        return new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(FROZEN)));
    }

    private static long logVersion(Path table) {
        return io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).update().getVersion();
    }

    private static Process java(Path log, String mainClass, String... args) throws IOException {
        var command = new ArrayList<String>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        command.add("-cp");
        command.add(System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")));
        command.add(mainClass);
        command.addAll(List.of(args));
        return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
    }

    private static void awaitFile(Path file, Process process) throws Exception {
        for (int i = 0; i < 6000 && !Files.exists(file); i++) {
            if (!process.isAlive()) fail("child exited early with " + process.exitValue());
            Thread.sleep(20);
        }
        assertTrue(Files.exists(file), "child never reached its first shard");
    }

    /** Counts every read of the repository. */
    static final class CountingStore implements BlobStore {
        final BlobStore delegate;
        final AtomicInteger reads = new AtomicInteger();

        CountingStore(BlobStore delegate) { this.delegate = delegate; }

        @Override public Optional<BlobMetadata> stat(String key) { reads.incrementAndGet(); return delegate.stat(key); }
        @Override public InputStream read(String key) throws IOException { reads.incrementAndGet(); return delegate.read(key); }
        @Override public List<String> list(String prefix) { reads.incrementAndGet(); return delegate.list(prefix); }
        @Override public List<BlobMetadata> listWithSizes(String prefix) { reads.incrementAndGet(); return delegate.listWithSizes(prefix); }
        @Override public void put(String key, byte[] content) { delegate.put(key, content); }
    }
}
