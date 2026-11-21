package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exactly-once and cleanup guarantees of the writer's staging and commit protocol, observed
 * through the real pipeline: shards are simulated over the demo fixture, as in
 * DumpToDeltaPipelineTest, and every shard rolls one data file per document. Every scenario also
 * runs with each shard read as three ranges: the protocol is per shard, not per range.
 */
class CommitProtocolPipelineTest {

    private static final Path FIXTURE_REPO =
        Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");
    private static final String INDEX_NAME = "test_data_7_17_9";
    private static final int ROWS_PER_SHARD = 5;

    private static final ArchiveContext CONTEXT =
        new ArchiveContext(INDEX_NAME, "repo-index-id", "snap-uuid", "snap", INDEX_NAME, "snap-uuid");

    /** The JSON row path, the archive row path, and the archive path with every shard in three ranges. */
    enum Mode {
        JSON_ROWS(1), ARCHIVE_ROWS(1), ARCHIVE_THREE_RANGES(3);

        final int threads;

        Mode(int threads) {
            this.threads = threads;
        }

        RunReport run(DumpToDeltaPipeline pipeline, DumpToDeltaPipeline.ShardCatalog catalog) throws Exception {
            return this == JSON_ROWS ? pipeline.run(INDEX_NAME, null, catalog)
                : pipeline.runSnapshot(CONTEXT, null, catalog, RunReportListener.NO_OP);
        }
    }

    private String previousMinimum;

    @BeforeEach
    void smallRanges() {
        previousMinimum = System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "1");
    }

    @AfterEach
    void restore() {
        if (previousMinimum == null) System.clearProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY);
        else System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, previousMinimum);
    }

    private static Configuration withThreads(Configuration conf, int threads) {
        conf.set(ShardRanges.SHARD_THREADS_KEY, Integer.toString(threads));
        return conf;
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void multiFileCrashThenResumeIsExactlyOnceWithoutOrphans(Mode mode, @TempDir Path tmp) throws Exception {
        Path table = tmp.resolve("table");
        var conf = withThreads(new Configuration(), mode.threads);
        conf.set(DeltaTableWriter.TARGET_FILE_BYTES_KEY, "1");
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        var catalog = virtualCatalog(inspector.shardSnapshot(INDEX_NAME, 0), 4);

        var crashing = new Opener(2, 3);
        var first = new DumpToDeltaPipeline(inspector, store, table.toString(), conf, crashing);
        assertThrows(SimulatedCrash.class, () -> mode.run(first, catalog));
        assertEquals(List.of(0, 1, 2), crashing.opened);
        assertEquals(ParquetFooters.addFilePaths(conf, table.toString()), ParquetFooters.parquetFilesOnDisk(table),
            "the crashed shard's rolled files must not outlive the attempt");
        assertEquals(2L * ROWS_PER_SHARD, ParquetFooters.addFilePaths(conf, table.toString()).size());

        var resuming = new Opener(null, 0);
        var report = mode.run(new DumpToDeltaPipeline(inspector, store, table.toString(), conf, resuming), catalog);
        assertEquals(List.of(2, 3), resuming.opened);
        assertEquals(2, report.committedCount());
        assertEquals(2, report.skippedCount());
        assertEquals(Map.of(0L, 5L, 1L, 5L, 2L, 5L, 3L, 5L), rowsPerShard(conf, table.toString()));
        assertEquals(ParquetFooters.addFilePaths(conf, table.toString()), ParquetFooters.parquetFilesOnDisk(table));
        assertEquals(4L * ROWS_PER_SHARD, ParquetFooters.addFilePaths(conf, table.toString()).size());
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void resumeWithAnotherCodecKeepsEveryShardExactlyOnce(Mode mode, @TempDir Path tmp) throws Exception {
        Path table = tmp.resolve("table");
        var uncompressed = withThreads(new Configuration(), mode.threads);
        uncompressed.set(DeltaTableWriter.PARQUET_CODEC_KEY, "uncompressed");
        var zstd = withThreads(new Configuration(), mode.threads);
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        var catalog = virtualCatalog(inspector.shardSnapshot(INDEX_NAME, 0), 5);

        assertThrows(SimulatedCrash.class, () -> mode.run(new DumpToDeltaPipeline(inspector, store, table.toString(), uncompressed,
            new Opener(2, 0)), catalog));
        var resumed = mode.run(new DumpToDeltaPipeline(inspector, store, table.toString(), zstd, new Opener(null, 0)), catalog);
        assertEquals(3, resumed.committedCount());
        assertEquals(2, resumed.skippedCount());

        var codecs = new java.util.HashMap<org.apache.parquet.hadoop.metadata.CompressionCodecName, Integer>();
        for (var footer : ParquetFooters.read(zstd, table.toString()))
            for (var codec : footer.codecs()) codecs.merge(codec, 1, Integer::sum);
        assertEquals(Map.of(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED, 2 * mode.threads,
            org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD, 3 * mode.threads), codecs, "one file per range");
        assertEquals(Map.of(0L, 5L, 1L, 5L, 2L, 5L, 3L, 5L, 4L, 5L), rowsPerShard(zstd, table.toString()));
        assertEquals(ParquetFooters.addFilePaths(zstd, table.toString()), ParquetFooters.parquetFilesOnDisk(table));
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void ambiguousCommitFailureIsQuarantinedThenCommittedExactlyOnce(Mode mode, @TempDir Path tmp) throws Exception {
        Path table = tmp.resolve("table");
        var conf = withThreads(FaultInjectingLogStore.enable(new Configuration()), mode.threads);
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        var catalog = virtualCatalog(inspector.shardSnapshot(INDEX_NAME, 0), 3);

        var failingCommit = new Opener(null, 0) {
            @Override public ShardReader open(ShardSnapshot shard, Path shardDir) {
                if (shard.snapshotName().endsWith("-1"))
                    FaultInjectingLogStore.failNextCommits(FaultInjectingLogStore.Mode.THROW_BEFORE_WRITE, 1);
                return super.open(shard, shardDir);
            }
        };
        RunReport first;
        try {
            first = mode.run(new DumpToDeltaPipeline(inspector, store, table.toString(), conf, failingCommit), catalog);
        } finally {
            FaultInjectingLogStore.reset();
        }
        assertEquals(2, first.committedCount());
        assertEquals(1, first.quarantinedCount());
        assertEquals(RunReport.Outcome.QUARANTINED, first.shards().get(1).outcome());
        assertEquals("IOException", first.shards().get(1).errorClass());
        assertEquals(ParquetFooters.addFilePaths(conf, table.toString()), ParquetFooters.parquetFilesOnDisk(table),
            "the failed commit's files are removed at the end of the run");

        var second = mode.run(new DumpToDeltaPipeline(inspector, store, table.toString(), conf, new Opener(null, 0)), catalog);
        assertEquals(1, second.committedCount());
        assertEquals(2, second.skippedCount());
        assertEquals(Map.of(0L, 5L, 1L, 5L, 2L, 5L), rowsPerShard(conf, table.toString()));
        assertEquals(ParquetFooters.addFilePaths(conf, table.toString()), ParquetFooters.parquetFilesOnDisk(table));
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void commitThatLandedDespiteAnErrorIsReportedCommitted(Mode mode, @TempDir Path tmp) throws Exception {
        Path table = tmp.resolve("table");
        var conf = withThreads(FaultInjectingLogStore.enable(new Configuration()), mode.threads);
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        var catalog = virtualCatalog(inspector.shardSnapshot(INDEX_NAME, 0), 3);

        var failingAfterWrite = new Opener(null, 0) {
            @Override public ShardReader open(ShardSnapshot shard, Path shardDir) {
                if (shard.snapshotName().endsWith("-1"))
                    FaultInjectingLogStore.failNextCommits(FaultInjectingLogStore.Mode.THROW_AFTER_WRITE, 1);
                return super.open(shard, shardDir);
            }
        };
        RunReport report;
        try {
            report = mode.run(new DumpToDeltaPipeline(inspector, store, table.toString(), conf, failingAfterWrite), catalog);
        } finally {
            FaultInjectingLogStore.reset();
        }
        assertEquals(3, report.committedCount());
        assertEquals(0, report.quarantinedCount());
        assertEquals(15, report.rowsWrittenThisRun());
        assertEquals(Map.of(0L, 5L, 1L, 5L, 2L, 5L), rowsPerShard(conf, table.toString()));
        assertEquals(ParquetFooters.addFilePaths(conf, table.toString()), ParquetFooters.parquetFilesOnDisk(table));
    }

    /** JSON rows (shardId, row) or archive rows (_shard, _doc) per shard; fails on a duplicate. */
    static Map<Long, Long> rowsPerShard(Configuration conf, String table) throws IOException {
        Map<Long, Long> counts = new HashMap<>();
        var seen = new java.util.HashSet<String>();
        var log = io.delta.standalone.DeltaLog.forTable(conf, table).update();
        boolean archive = java.util.Arrays.asList(log.getMetadata().getSchema().getFieldNames()).contains("_shard");
        String shardColumn = archive ? "_shard" : "shardId";
        String rowColumn = archive ? "_doc" : "row";
        try (var rows = log.open()) {
            while (rows.hasNext()) {
                var row = rows.next();
                assertTrue(seen.add(row.getLong(shardColumn) + "/" + row.getLong(rowColumn)), "duplicate row");
                counts.merge(row.getLong(shardColumn), 1L, Long::sum);
            }
        }
        return counts;
    }

    static DumpToDeltaPipeline.ShardCatalog virtualCatalog(ShardSnapshot template, int shardCount) {
        return new DumpToDeltaPipeline.ShardCatalog() {
            @Override public int shardCount() {
                return shardCount;
            }

            @Override public ShardSnapshot shardSnapshot(int shardId) {
                return new ShardSnapshot(template.repoUuid(), template.shardPrefix(),
                    "virtual-shard-" + shardId, template.indexVersion(), template.totalSize(), template.files());
            }
        };
    }

    /** Simulates process death (kill -9): not caught by the pipeline's per-shard quarantine. */
    static final class SimulatedCrash extends Error {}

    /** Opens deterministic shard readers; optionally dies while reading one shard's documents. */
    static class Opener implements DumpToDeltaPipeline.ShardReaderOpener {
        final List<Integer> opened = new ArrayList<>();
        private final Integer crashShard;
        private final int crashAtDoc;

        Opener(Integer crashShard, int crashAtDoc) {
            this.crashShard = crashShard;
            this.crashAtDoc = crashAtDoc;
        }

        @Override
        public ShardReader open(ShardSnapshot shard, Path shardDir) {
            int shardId = Integer.parseInt(shard.snapshotName().substring("virtual-shard-".length()));
            opened.add(shardId);
            return new Reader(shardId, crashShard != null && crashShard == shardId, crashAtDoc);
        }

        /** Forks into readers of the same documents, so the shard can be read in ranges. */
        private record Reader(int shardId, boolean crashes, int crashAtDoc) implements ShardReader {
            @Override public String runtimeName() {
                return "fake";
            }

            @Override public int docCount() {
                return ROWS_PER_SHARD;
            }

            @Override public void readSource(int docId, SourceConsumer consumer) {
                if (crashes && docId == crashAtDoc) throw new SimulatedCrash();
                byte[] bytes = ("{\"shardId\":" + shardId + ",\"row\":" + docId + "}").getBytes(StandardCharsets.UTF_8);
                consumer.accept(bytes, bytes.length);
            }

            @Override public ShardReader fork() {
                return new Reader(shardId, crashes, crashAtDoc);
            }

            @Override public void close() {
            }
        }
    }
}
