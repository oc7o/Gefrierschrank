package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static de.sloow.gefrierschrank.delta.SelectiveShardFixture.STORED_METADATA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** dump-to-delta's pipeline with selective fetch: what is downloaded, what is exported, and the in-run retry. */
class SelectiveFetchPipelineTest {

    private String savedSelective;
    private String savedMinRange;

    @BeforeEach
    void save() {
        savedSelective = System.getProperty(SelectiveFetch.PROPERTY);
        savedMinRange = System.getProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY);
    }

    @AfterEach
    void restore() {
        restore(SelectiveFetch.PROPERTY, savedSelective);
        restore(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, savedMinRange);
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }

    static Configuration threads(int threads) {
        var conf = new Configuration();
        conf.set(ShardRanges.SHARD_THREADS_KEY, Integer.toString(threads));
        return conf;
    }

    record Run(RunReport report, String table, String stderr) {
        RunReport.ShardReport shard() {
            return report.shards().get(0);
        }
    }

    static Run export(SelectiveShardFixture.Shard shard, SelectiveShardFixture.CountingStore store, Path table,
                      Configuration conf, DumpToDeltaPipeline.ShardReaderOpener opener) throws Exception {
        PrintStream stderr = System.err;
        var captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        RunReport report;
        try {
            report = SelectiveShardFixture.export(shard, store, table.toString(), conf, opener, STORED_METADATA);
        } finally {
            System.setErr(stderr);
        }
        // The export's own lines only: library loggers may print their first-use notices to stderr too.
        String exportLines = captured.toString(StandardCharsets.UTF_8).lines()
            .filter(line -> line.startsWith("warning:") || line.startsWith("note:"))
            .collect(java.util.stream.Collectors.joining("\n"));
        return new Run(report, table.toString(), exportLines);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void selectiveShardsSkipThePrunedFilesAndExportTheSameTable(int threads, @TempDir Path tmp) throws Exception {
        System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "5");
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("shard"));
        assertFalse(shard.prunableBlobs().isEmpty());

        var fullStore = new SelectiveShardFixture.CountingStore(shard.repo());
        Run full = export(shard, fullStore, tmp.resolve("full"), threads(threads), new SelectiveShardFixture.Opener());
        assertEquals("full", full.shard().fetch());
        assertEquals(shard.downloadBytes(), full.shard().bytesFetched());

        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var store = new SelectiveShardFixture.CountingStore(shard.repo());
        var opener = new SelectiveShardFixture.Opener();
        Run selective = export(shard, store, tmp.resolve("selective"), threads(threads), opener);

        assertEquals(RunReport.Outcome.COMMITTED, selective.shard().outcome());
        assertEquals("selective", selective.shard().fetch());
        for (String blob : shard.prunableBlobs()) assertEquals(0, store.reads(blob), blob);
        for (String blob : store.blobsRead()) assertEquals(1, store.reads(blob), blob);
        long prunable = shard.snapshot().files().stream().filter(f -> shard.prunableBlobs().contains(f.blobName()))
            .mapToLong(de.sloow.gefrierschrank.core.repo.RepoFileInfo::length).sum();
        assertEquals(shard.downloadBytes() - prunable, selective.shard().bytesFetched());
        assertEquals(1, opener.prunedOpens.get());
        assertEquals(0, opener.opens.get());
        assertEquals(full.shard().rowsWritten(), selective.shard().rowsWritten());
        assertEquals(full.shard().bytesProcessed(), selective.shard().bytesProcessed());
        assertEquals(SelectiveShardFixture.rows(full.table()), SelectiveShardFixture.rows(selective.table()));
        assertEquals(SelectiveShardFixture.commitShape(full.table()), SelectiveShardFixture.commitShape(selective.table()));
        assertEquals(full.stderr(), selective.stderr());
    }

    /** An opener whose first {@code failingReaders} readers fail on one document. */
    static final class FlakyOpener extends SelectiveShardFixture.Opener {
        final AtomicInteger readers = new AtomicInteger();
        final int failingReaders;
        final Runnable beforeFailure;

        FlakyOpener(int failingReaders, Runnable beforeFailure) {
            this.failingReaders = failingReaders;
            this.beforeFailure = beforeFailure;
        }

        private ShardReader flaky(ShardReader reader) {
            if (readers.incrementAndGet() > failingReaders) return reader;
            return new SelectiveShardFixture.FailingReader(reader, doc -> {
                if (doc != 41) return false;
                beforeFailure.run();
                return true;
            });
        }

        @Override
        public ShardReader open(ShardSnapshot shard, Path shardDir) throws Exception {
            return flaky(super.open(shard, shardDir));
        }

        @Override
        public ShardReader openPruned(ShardSnapshot shard, Path shardDir, com.fasterxml.jackson.databind.JsonNode metadata)
                throws Exception {
            return flaky(super.openPruned(shard, shardDir, metadata));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void aFailedSelectiveShardIsCompletedAndCommittedOnceInTheSameRun(int threads, @TempDir Path tmp) throws Exception {
        System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "5");
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("shard"));
        Run reference = export(shard, new SelectiveShardFixture.CountingStore(shard.repo()), tmp.resolve("reference"),
            threads(threads), new SelectiveShardFixture.Opener());

        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var store = new SelectiveShardFixture.CountingStore(shard.repo());
        var opener = new FlakyOpener(1, () -> {});
        Run retried = export(shard, store, tmp.resolve("retried"), threads(threads), opener);

        assertEquals(RunReport.Outcome.COMMITTED, retried.shard().outcome(), retried.report().toString());
        assertEquals("completed", retried.shard().fetch());
        assertEquals(shard.downloadBytes(), retried.shard().bytesFetched());
        assertEquals(1, opener.prunedOpens.get());
        assertEquals(1, opener.opens.get(), "the retry opens the completed directory normally");
        for (String blob : store.blobsRead()) assertEquals(1, store.reads(blob), "downloaded once: " + blob);
        assertEquals(SelectiveShardFixture.rows(reference.table()), SelectiveShardFixture.rows(retried.table()));
        assertEquals(SelectiveShardFixture.commitShape(reference.table()), SelectiveShardFixture.commitShape(retried.table()));
        assertEquals(List.of(), List.copyOf(SelectiveShardFixture.unreferencedDataFiles(retried.table())),
            "the failed attempt's staged files are gone");
        assertEquals(reference.stderr(), retried.stderr(), "warnings of the retry only");
        assertTrue(retried.stderr().contains(" 1 documents are not strict UTF-8 JSON"), retried.stderr());
    }

    @Test
    void aSecondFailureQuarantinesTheShard(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("shard"));
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var opener = new FlakyOpener(2, () -> {});
        Run failed = export(shard, new SelectiveShardFixture.CountingStore(shard.repo()), tmp.resolve("t"), threads(1), opener);
        assertEquals(RunReport.Outcome.QUARANTINED, failed.shard().outcome());
        assertEquals("InjectedReadFailure", failed.shard().errorClass());
        assertEquals("completed", failed.shard().fetch());
        assertEquals(shard.downloadBytes(), failed.shard().bytesFetched());
        assertEquals(1, opener.opens.get());
    }

    @Test
    void aFullShardIsNeverReadTwice(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("shard"));
        var opener = new FlakyOpener(1, () -> {});
        Run failed = export(shard, new SelectiveShardFixture.CountingStore(shard.repo()), tmp.resolve("off"), threads(1), opener);
        assertEquals(RunReport.Outcome.QUARANTINED, failed.shard().outcome());
        assertEquals("full", failed.shard().fetch());
        assertEquals(1, opener.opens.get());
        assertEquals(0, opener.commitReads.get(), "no commit-first round without the flag");

        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var notEligible = new FlakyOpener(1, () -> {});
        Run synthetic;
        PrintStream stderr = System.err;
        System.setErr(new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8));
        try {
            synthetic = new Run(SelectiveShardFixture.export(shard, new SelectiveShardFixture.CountingStore(shard.repo()),
                tmp.resolve("synthetic").toString(), threads(1), notEligible,
                SelectiveShardFixture.parse("{\"settings\":{\"index.mode\":\"logsdb\"},\"mappings\":{}}")), null, null);
        } finally {
            System.setErr(stderr);
        }
        assertEquals(RunReport.Outcome.QUARANTINED, synthetic.shard().outcome());
        assertEquals("full", synthetic.shard().fetch());
        assertEquals(1, notEligible.opens.get());
        assertEquals(0, notEligible.prunedOpens.get());
    }

    @Test
    void aFailureAfterTheKeyWasCommittedIsNotRetried(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("shard"));
        Path table = tmp.resolve("table");
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var committedElsewhere = new java.util.concurrent.atomic.AtomicBoolean();
        var opener = new FlakyOpener(1, () -> {
            if (!committedElsewhere.compareAndSet(false, true)) return;
            // Another writer commits the same shard key while this attempt is still reading.
            try {
                SelectiveShardFixture.export(shard, new SelectiveShardFixture.CountingStore(shard.repo()), table.toString(),
                    threads(1), new SelectiveShardFixture.Opener(), STORED_METADATA);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        Run failed = export(shard, new SelectiveShardFixture.CountingStore(shard.repo()), table, threads(1), opener);
        assertTrue(committedElsewhere.get());
        assertEquals(RunReport.Outcome.QUARANTINED, failed.shard().outcome());
        assertEquals("InjectedReadFailure", failed.shard().errorClass());
        assertEquals("selective", failed.shard().fetch());
        assertEquals(0, opener.opens.get(), "not read again");
        assertEquals(1, SelectiveShardFixture.commitShape(table.toString()).stream().filter(s -> s.startsWith("txn=")).count());
    }

    @Test
    void aFailingPrunedOpenCompletesTheDownloadAndOpensNormally(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("shard"));
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var store = new SelectiveShardFixture.CountingStore(shard.repo());
        var opener = new SelectiveShardFixture.Opener() {
            @Override
            public ShardReader openPruned(ShardSnapshot s, Path dir, com.fasterxml.jackson.databind.JsonNode metadata)
                    throws Exception {
                prunedOpens.incrementAndGet();
                throw new de.sloow.gefrierschrank.core.repo.PrunedFormatAccessException();
            }
        };
        Run run = export(shard, store, tmp.resolve("t"), threads(1), opener);
        assertEquals(RunReport.Outcome.COMMITTED, run.shard().outcome());
        assertEquals("completed", run.shard().fetch());
        assertEquals(shard.downloadBytes(), run.shard().bytesFetched());
        assertEquals(1, opener.opens.get());
        for (String blob : store.blobsRead()) assertEquals(1, store.reads(blob), blob);
        assertEquals(shard.blobsOf(name -> true), store.blobsRead());
    }

    /**
     * A shard whose pruned open already fell back to the normal open was read from the complete
     * directory: no second read.
     */
    @Test
    void aWriteFailureAfterThePrunedOpenFellBackIsNotRetried(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("shard"));
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var opener = new SelectiveShardFixture.Opener() {
            @Override
            public ShardReader open(ShardSnapshot s, Path dir) throws Exception {
                return new SelectiveShardFixture.FailingReader(super.open(s, dir), doc -> doc == 41);
            }

            @Override
            public ShardReader openPruned(ShardSnapshot s, Path dir, com.fasterxml.jackson.databind.JsonNode metadata)
                    throws Exception {
                prunedOpens.incrementAndGet();
                throw new de.sloow.gefrierschrank.core.repo.PrunedFormatAccessException();
            }
        };
        Run failed = export(shard, new SelectiveShardFixture.CountingStore(shard.repo()), tmp.resolve("t"), threads(1), opener);
        assertEquals(RunReport.Outcome.QUARANTINED, failed.shard().outcome());
        assertEquals("InjectedReadFailure", failed.shard().errorClass());
        assertEquals("completed", failed.shard().fetch());
        assertEquals(1, opener.prunedOpens.get());
        assertEquals(1, opener.opens.get(), "not read again");
    }

    /** The retry's reopen fails: the shard is quarantined with that failure, which carries the first one. */
    @Test
    void aFailingReopenKeepsTheFirstFailure(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("shard"));
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var reopenFailure = new java.io.IOException("reopen");
        var opener = new SelectiveShardFixture.Opener() {
            @Override
            public ShardReader open(ShardSnapshot s, Path dir) throws Exception {
                opens.incrementAndGet();
                throw reopenFailure;
            }

            @Override
            public ShardReader openPruned(ShardSnapshot s, Path dir, com.fasterxml.jackson.databind.JsonNode metadata)
                    throws Exception {
                return new SelectiveShardFixture.FailingReader(super.openPruned(s, dir, metadata), doc -> doc == 41);
            }
        };
        Run failed = export(shard, new SelectiveShardFixture.CountingStore(shard.repo()), tmp.resolve("t"), threads(1), opener);
        assertEquals(RunReport.Outcome.QUARANTINED, failed.shard().outcome());
        assertEquals("IOException", failed.shard().errorClass());
        assertEquals(1, opener.opens.get());
        assertEquals(List.of(SelectiveShardFixture.InjectedReadFailure.class),
            java.util.Arrays.stream(reopenFailure.getSuppressed())
                .map(first -> de.sloow.gefrierschrank.core.FailureDiagnostics.rootCause(first).getClass()).toList());
    }

    /** The retry's completion fails (here with an Error, which ends the run): that failure carries the first one. */
    @Test
    void aFailingCompletionKeepsTheFirstFailure(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("shard"));
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var store = new SelectiveShardFixture.CountingStore(shard.repo());
        var completionFailure = new AssertionError("completion");
        var opener = new SelectiveShardFixture.Opener() {
            @Override
            public ShardReader openPruned(ShardSnapshot s, Path dir, com.fasterxml.jackson.databind.JsonNode metadata)
                    throws Exception {
                return new SelectiveShardFixture.FailingReader(super.openPruned(s, dir, metadata), doc -> {
                    if (doc != 41) return false;
                    store.failReads = completionFailure;
                    return true;
                });
            }
        };
        Throwable thrown = org.junit.jupiter.api.Assertions.assertThrows(Throwable.class,
            () -> export(shard, store, tmp.resolve("t"), threads(1), opener));
        while (thrown != completionFailure && thrown.getCause() != null) thrown = thrown.getCause();
        org.junit.jupiter.api.Assertions.assertSame(completionFailure, thrown);
        assertEquals(0, opener.opens.get());
        assertEquals(List.of(SelectiveShardFixture.InjectedReadFailure.class),
            java.util.Arrays.stream(completionFailure.getSuppressed())
                .map(first -> de.sloow.gefrierschrank.core.FailureDiagnostics.rootCause(first).getClass()).toList());
    }
}
