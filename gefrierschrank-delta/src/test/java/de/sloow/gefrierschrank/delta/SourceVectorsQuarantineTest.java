package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * A rehydration failure in the middle of a shard's ranges quarantines that shard like any other
 * read failure: no staged Parquet file of it survives, and the next run commits it once.
 */
class SourceVectorsQuarantineTest {

    private static final String INDEX = "vec_root";

    @Test
    void aCursorFailureQuarantinesTheShardAndTheRerunCommitsItOnce(@TempDir Path tmp) throws Exception {
        var result = VectorExportJvm.export(VectorExportJvm.FIXTURES.resolve("vec-repo"), tmp, List.of(INDEX), 1, 2, Map.of(
            VectorExportJvm.FAIL_AFTER_LOADS, "12",
            "gefrierschrank.shard.threads", "3", ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "5"));

        JsonNode failed = result.reports().get(INDEX + "#1");
        assertEquals(1, failed.path("quarantinedCount").asInt(), failed.toString());
        assertEquals(0, failed.path("committedCount").asInt(), failed.toString());
        JsonNode shard = failed.path("shards").path(0);
        assertEquals("QUARANTINED", shard.path("outcome").asText());
        assertEquals("VectorRehydrationException", shard.path("errorClass").asText());
        assertFalse(shard.path("errorMessage").asText().contains("injected"), "no raw exception message in a report");
        assertFalse(result.stdout().contains("injected") || result.stderr().contains("injected"));

        JsonNode rerun = result.reports().get(INDEX + "#2");
        assertEquals(1, rerun.path("committedCount").asInt(), rerun.toString());
        Map<String, String> expected = VectorExportJvm.rawExpected("vec-" + INDEX + "-expected.json");
        Map<String, VectorExportJvm.Row> rows = VectorExportJvm.rows(tmp.resolve(INDEX));
        assertEquals(expected.keySet(), rows.keySet(), "every document once");
        assertEquals(expected.size(), rerun.path("rowsWrittenThisRun").asLong());

        Set<String> committed = io.delta.standalone.DeltaLog.forTable(new org.apache.hadoop.conf.Configuration(),
                tmp.resolve(INDEX).toString()).update().getAllFiles().stream()
            .map(file -> file.getPath()).collect(Collectors.toCollection(TreeSet::new));
        try (Stream<Path> files = Files.list(tmp.resolve(INDEX))) {
            Set<String> parquet = files.map(path -> path.getFileName().toString())
                .filter(name -> name.endsWith(".parquet")).collect(Collectors.toCollection(TreeSet::new));
            assertEquals(committed, parquet, "the quarantined attempt left no staged file behind");
        }
        for (Map<String, String> tags : VectorExportJvm.fileTags(tmp.resolve(INDEX))) {
            assertEquals(SourceVectorsPlan.FILE_TAG_VALUE, tags.get(SourceVectorsPlan.FILE_TAG), "the retry tags its files too");
        }
    }
}
