package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Synthetic-source shards at --parallelism > 1: every shard task opens its own reconstructor, all
 * of them on the one shared isolated loader, while their writes run concurrently. The rows must
 * equal a serial run's, and the concurrent run must not load Elasticsearch's classes again per
 * shard (a loader per shard used to add about 2,800 classes each).
 */
class ParallelSyntheticReconstructionTest {

    private static final Path SYNTH_REPO = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures/synth-repo");
    private static final String INDEX = "test_tsdb_synth";
    private static final int SHARDS = 6;
    private static final int WORKERS = 3;

    @Test
    @Tag("concurrency")
    @Timeout(180)
    void concurrentSyntheticShardsReconstructLikeASerialRunOnOneLoader(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(SYNTH_REPO);
        var inspector = new RepositoryInspector(store);
        JsonNode metadata = inspector.indexMetadata(INDEX);
        ShardSnapshot template = inspector.shardSnapshot(INDEX, 0);
        var context = new ArchiveContext(INDEX, "rid", "snap", "snap", INDEX, "snap");

        String serialTable = tmp.resolve("serial").toString();
        RunReport serial = new DumpToDeltaPipeline(inspector, store, serialTable, new Configuration(),
                (shard, dir) -> new Lucene10ShardReader(dir))
            .runSnapshot(context, metadata, CommitProtocolPipelineTest.virtualCatalog(template, SHARDS), RunReportListener.NO_OP);
        assertEquals(SHARDS, serial.committedCount());

        var classLoading = java.lang.management.ManagementFactory.getClassLoadingMXBean();
        long classesBefore = classLoading.getLoadedClassCount();
        var allOpen = new CyclicBarrier(WORKERS);
        String parallelTable = tmp.resolve("parallel").toString();
        RunReport parallel = new DumpToDeltaPipeline(inspector, store, new DeltaTableWriter(parallelTable), new Configuration(),
                (shard, dir) -> {
                    allOpen.await(60, TimeUnit.SECONDS);
                    return new Lucene10ShardReader(dir);
                })
            .withWorkPool(new ShardWorkPool(WORKERS, 0))
            .runSnapshot(context, metadata, CommitProtocolPipelineTest.virtualCatalog(template, SHARDS), RunReportListener.NO_OP);
        long classesLoaded = classLoading.getLoadedClassCount() - classesBefore;

        assertEquals(0, parallel.quarantinedCount());
        assertEquals(SHARDS, parallel.committedCount());
        assertEquals(serial.rowsWrittenThisRun(), parallel.rowsWrittenThisRun());
        List<String> rows = rows(serialTable);
        assertTrue(rows.stream().allMatch(row -> row.contains("|synthetic|")), "every row was reconstructed");
        assertEquals(rows, rows(parallelTable), "every column of every row");
        assertTrue(classesLoaded < 500, SHARDS + " concurrent synthetic shards loaded " + classesLoaded + " classes");
    }

    private static List<String> rows(String table) throws Exception {
        var rows = new ArrayList<String>();
        try (var it = io.delta.standalone.DeltaLog.forTable(new Configuration(), table).update().open()) {
            while (it.hasNext()) {
                var row = it.next();
                rows.add(row.getLong("_shard") + "|" + row.getLong("_doc") + "|" + row.getString("_id") + "|"
                    + row.getString("_source_kind") + "|" + row.getString("_source"));
            }
        }
        rows.sort(null);
        return rows;
    }
}
