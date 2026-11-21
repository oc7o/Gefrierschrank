package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FullIndexExporter arms its document-table writer with requireArchiveSnapshot and hands that same
 * instance to the pipeline, so the snapshot guard covers every shard commit of the export,
 * including concurrent ones. Here another export of a different snapshot commits into the table
 * after this export's checks passed and while its shards are open: no shard of this export may
 * land on top.
 */
class ExportSnapshotGuardTest {

    private static final Path FROZEN_REPO = Path.of("../gefrierschrank-core/src/test/resources/fixtures/frozen-repo");
    private static final String INDEX = "frozen-docs-f5ffca09";

    @ParameterizedTest(name = "parallelism {0}")
    @ValueSource(ints = {1, 2})
    void aSnapshotCommittedAfterTheChecksFailsEveryShardCommitOfTheExport(int parallelism, @TempDir Path tmp) throws Exception {
        var conf = new Configuration();
        Path tableDir = tmp.resolve("table");
        String table = tableDir.toString();
        var otherSnapshot = new ArchiveContext(INDEX, "rid", "another-snapshot", "other", INDEX, "another-snapshot");
        var raced = new boolean[1];
        DumpToDeltaPipeline.ShardReaderOpener opener = (shard, dir) -> {
            synchronized (raced) {
                if (!raced[0]) ArchiveGuardTest.writeShard(new DeltaTableWriter(table), conf, otherSnapshot, 0);
                raced[0] = true;
            }
            return new Lucene10ShardReader(dir);
        };
        var sources = new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(FROZEN_REPO)));
        var exporter = new FullIndexExporter(sources, conf, opener);
        exporter.configure(new ExportSettings(SnapshotSelection.DEFAULT, parallelism, 0));

        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class,
            () -> exporter.run(INDEX, table, RunReportListener.NO_OP));

        assertTrue(raced[0]);
        assertEquals(Set.of(otherSnapshot.key(0)), new DeltaTableWriter(table).committedShardKeys(conf),
            "only the other snapshot's commit is in the table");
        assertEquals(Set.of("another-snapshot"), ArchiveGuardTest.snapshotUuids(conf, table));
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(tableDir),
            "the refused shards left no Parquet files behind");
    }
}
