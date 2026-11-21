package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The pipeline checks the vector plan itself, before any shard, so callers that skip the export
 * preflight (run(String), test seams) never export a document without vectors it cannot put back.
 */
class SourceVectorsPipelineGuardTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path REPO = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures/es93-repo");
    private static final ArchiveContext ARCHIVE = new ArchiveContext("idx", "repo-index-id", "snap-uuid", "snap", "idx", "snap-uuid");

    @Test
    void anUnrehydratableIndexFailsBeforeAnyShard(@TempDir Path tmp) throws Exception {
        JsonNode rankVectors = JSON.readTree("{\"settings\":{\"index.version.created\":\"9060000\",\"index.number_of_shards\":\"1\","
            + "\"index.number_of_replicas\":\"0\"},\"mappings\":{\"properties\":{\"v\":{\"type\":\"rank_vectors\"}}}}");
        List<Integer> asked = new ArrayList<>();
        var store = new FileSystemBlobStore(REPO);
        Path table = tmp.resolve("table");
        var pipeline = new DumpToDeltaPipeline(new RepositoryInspector(store), store, new DeltaTableWriter(table.toString()),
            new Configuration(), (shard, dir) -> { throw new AssertionError("no shard may be opened"); });
        var catalog = new DumpToDeltaPipeline.ShardCatalog() {
            @Override public int shardCount() { return 2; }
            @Override public ShardSnapshot shardSnapshot(int shardId) {
                asked.add(shardId);
                throw new AssertionError("no shard may be resolved");
            }
        };
        assertThrows(SourceVectorsPlan.ExcludedSourceVectorsException.class,
            () -> pipeline.runSnapshot(ARCHIVE, rankVectors, catalog, RunReportListener.NO_OP));
        assertThrows(SourceVectorsPlan.ExcludedSourceVectorsException.class,
            () -> pipeline.run("idx", null, catalog, RunReportListener.NO_OP, rankVectors));
        assertEquals(List.of(), asked);
        assertFalse(Files.exists(table));
    }
}
