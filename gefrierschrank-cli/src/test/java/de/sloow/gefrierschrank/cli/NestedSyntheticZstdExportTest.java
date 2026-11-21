package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.delta.FullIndexExporter;
import de.sloow.gefrierschrank.delta.RunReport;
import de.sloow.gefrierschrank.delta.RunReportListener;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Nested objects under synthetic source in a logsdb index (ZSTD stored fields, so on the CLI
 * classpath like SyntheticZstdExportTest): every live root commits with its nested objects exactly
 * as Elasticsearch's search rendered them (logsdb keeps arrays in {@code _ignored_source}, which
 * Elasticsearch's loader prefers), the same rows at one to four shard threads and through either
 * reconstruction directory.
 */
class NestedSyntheticZstdExportTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final String INDEX = "cov_nested_logsdb";
    private String previousMinimum;
    private String previousDirectory;

    @BeforeEach
    void smallRanges() {
        previousMinimum = System.setProperty("gefrierschrank.shard.threads.minRangeLiveDocs", "1");
        previousDirectory = System.getProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY);
    }

    @AfterEach
    void restore() {
        if (previousMinimum == null) System.clearProperty("gefrierschrank.shard.threads.minRangeLiveDocs");
        else System.setProperty("gefrierschrank.shard.threads.minRangeLiveDocs", previousMinimum);
        if (previousDirectory == null) System.clearProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY);
        else System.setProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY, previousDirectory);
    }

    @Test
    void logsdbRootsExportWithTheirNestedObjects(@TempDir Path tmp) throws Exception {
        JsonNode expected = JSON.readTree(FIXTURES.resolve("synthcov-" + INDEX + "-expected.json").toFile());
        Map<String, String> reference = null;
        int run = 0;
        for (String directory : new String[]{"mmap", "niofs"}) {
            System.setProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY, directory);
            for (int threads : directory.equals("mmap") ? new int[]{1, 2, 3, 4} : new int[]{1, 4}) {
                var conf = new Configuration();
                conf.set("gefrierschrank.shard.threads", Integer.toString(threads));
                Path table = tmp.resolve("table" + run++);
                var sources = new RepositorySources(new RepositorySources.Source("primary",
                    new FileSystemBlobStore(FIXTURES.resolve("synthcov-repo"))));
                RunReport report = new FullIndexExporter(sources, conf, (shard, dir) -> new Lucene10ShardReader(dir))
                    .run(INDEX, table.toString(), RunReportListener.NO_OP);
                String what = directory + " K=" + threads;
                assertEquals(2, report.committedCount(), what + " " + report.shards());
                Map<String, String> rows = new TreeMap<>();
                try (var it = io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).update().open()) {
                    while (it.hasNext()) {
                        var row = it.next();
                        assertEquals("synthetic", row.getString("_source_kind"));
                        assertNull(rows.put(row.getString("_id"), row.getLong("_shard") + " " + row.getLong("_doc") + " "
                            + row.getString("_source")));
                    }
                }
                assertEquals(expected.path("sources").size(), rows.size(), what);
                for (var entry : rows.entrySet()) {
                    JsonNode actual = JSON.readTree(entry.getValue().substring(entry.getValue().indexOf('{')));
                    JsonNode source = expected.path("sources").path(entry.getKey());
                    assertEquals(source, actual, what + " " + entry.getKey());
                    assertEquals(order(source), order(actual), what + " " + entry.getKey() + " field order");
                }
                for (JsonNode deleted : expected.path("deleted_ids")) assertFalse(rows.containsKey(deleted.asText()));
                if (reference == null) reference = rows;
                else assertEquals(reference, rows, what + ": identical rows and slots");
            }
        }
    }

    private static List<String> order(JsonNode node) {
        List<String> names = new ArrayList<>();
        collect(node, "", names);
        return names;
    }

    private static void collect(JsonNode node, String path, List<String> into) {
        if (node.isObject()) {
            node.properties().forEach(e -> {
                into.add(path + "/" + e.getKey());
                collect(e.getValue(), path + "/" + e.getKey(), into);
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) collect(node.get(i), path + "[" + i + "]", into);
        }
    }
}
