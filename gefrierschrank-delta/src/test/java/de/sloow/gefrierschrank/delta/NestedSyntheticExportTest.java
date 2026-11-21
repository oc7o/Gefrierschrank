package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker;
import de.sloow.gefrierschrank.syntheticsource.SyntheticSourceReconstructor;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Nested objects under synthetic source export as Elasticsearch renders them:
 * every live root commits with its nested objects, ordered value equality with ES's own search
 * output of the same documents, no row for a nested child, deleted roots absent, updated roots in
 * their latest version; the same rows at one to four shard threads (ranges cut inside the shard)
 * and through either reconstruction directory. A child id asked of the worker fails loudly.
 * cov_nested_logsdb (ZSTD stored fields) runs in the CLI module, NestedSyntheticZstdExportTest.
 */
class NestedSyntheticExportTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private String previousMinimum;
    private String previousDirectory;

    @BeforeEach
    void smallRanges() {
        previousMinimum = System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "1");
        previousDirectory = System.getProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY);
    }

    @AfterEach
    void restore() {
        if (previousMinimum == null) System.clearProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY);
        else System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, previousMinimum);
        if (previousDirectory == null) System.clearProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY);
        else System.setProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY, previousDirectory);
    }

    @ParameterizedTest
    @ValueSource(strings = {"test_deletes_nested", "cov_nested_synth", "cov8_nested_synth"})
    void rootsExportWithTheirNestedObjectsAtEveryThreadCountAndDirectory(String index, @TempDir Path tmp) throws Exception {
        JsonNode expected = SynthCovFixtures.expected(index);
        Map<String, JsonNode> sources = new TreeMap<>();
        expected.path("sources").properties().forEach(e -> sources.put(e.getKey(), e.getValue()));
        Map<String, SynthCovFixtures.Row> reference = null;
        int run = 0;
        for (String directory : new String[]{"mmap", "niofs"}) {
            System.setProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY, directory);
            for (int threads = 1; threads <= 4; threads++) {
                if (directory.equals("niofs") && threads > 1 && threads < 4) continue;
                var conf = new Configuration();
                conf.set(ShardRanges.SHARD_THREADS_KEY, Integer.toString(threads));
                var export = SynthCovFixtures.export(index, tmp.resolve("table" + run++), conf);
                String what = index + " " + directory + " K=" + threads;
                assertEquals(0, export.report().quarantinedCount(), what + " " + export.report().shards());
                assertEquals(export.report().shards().size(), export.report().committedCount(), what);
                Map<String, JsonNode> actual = new HashMap<>();
                for (var row : export.rows().entrySet()) {
                    assertEquals("synthetic", row.getValue().kind(), what);
                    actual.put(row.getKey(), JSON.readTree(row.getValue().source()));
                }
                OrderedJsonAssert.assertOrderedEquals(sources, actual);
                for (JsonNode deleted : expected.path("deleted_ids")) {
                    assertFalse(actual.containsKey(deleted.asText()), what + " deleted " + deleted.asText());
                }
                if (reference == null) reference = export.rows();
                else assertEquals(reference, export.rows(), what + ": identical rows, slots and kinds");
            }
        }
        assertTrue(expected.path("updated_ids").size() > 0 && expected.path("deleted_ids").size() > 0, "the fixture updates and deletes roots");
    }

    @ParameterizedTest
    @ValueSource(strings = {"cov_nested_synth", "cov8_nested_synth"})
    void aChildIdIsRefused(String index, @TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(SynthCovFixtures.repository(index));
        var inspector = new RepositoryInspector(store);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot(index, 0), shardDir);
        int children = 0;
        try (var reconstructor = SyntheticSourceReconstructor.open(shardDir, index, inspector.indexMetadata(index))) {
            for (int doc = 0; doc < reconstructor.docCount(); doc++) {
                try {
                    reconstructor.readSourceBytes(doc);
                } catch (IllegalArgumentException child) {
                    assertEquals("doc id is not a root document", child.getMessage());
                    children++;
                    int id = doc;
                    assertThrows(IllegalArgumentException.class, () -> reconstructor.readSourcesBytes(new int[]{id}));
                }
            }
        }
        assertTrue(children > 100, "children " + children);
        assertTrue(Files.isDirectory(shardDir));
    }
}
