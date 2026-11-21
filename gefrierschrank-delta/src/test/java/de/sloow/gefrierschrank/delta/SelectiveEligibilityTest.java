package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static de.sloow.gefrierschrank.delta.SelectiveShardFixture.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SelectiveEligibilityTest {

    static final Path SYNTH = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");

    private String saved;

    @BeforeEach
    void save() {
        saved = System.getProperty(SelectiveFetch.PROPERTY);
    }

    @AfterEach
    void restore() {
        if (saved == null) System.clearProperty(SelectiveFetch.PROPERTY);
        else System.setProperty(SelectiveFetch.PROPERTY, saved);
    }

    static JsonNode metadata(String repo, String index) throws Exception {
        return new RepositoryInspector(new FileSystemBlobStore(SYNTH.resolve(repo))).indexMetadata(index);
    }

    @Test
    void onlyIndicesServedFromTheirStoredSourceAreEligible() throws Exception {
        assertTrue(SelectiveEligibility.of("idx", SelectiveShardFixture.STORED_METADATA));
        assertFalse(SelectiveEligibility.of("idx", null), "no metadata");
        assertFalse(SelectiveEligibility.of("idx", parse("[]")));
        assertFalse(SelectiveEligibility.of("idx", parse("{\"settings\":{\"index.mode\":\"logsdb\"},\"mappings\":{}}")));
        assertFalse(SelectiveEligibility.of("idx", parse("{\"settings\":{\"index\":{\"mapping\":{\"source\":{\"mode\":\"synthetic\"}}}},\"mappings\":{}}")));
        assertFalse(SelectiveEligibility.of("idx", parse("{\"settings\":{},\"mappings\":{\"_doc\":{\"_source\":{\"mode\":\"synthetic\"}}}}")));
        assertFalse(SelectiveEligibility.of("idx", parse("{\"settings\":{\"index.version.created\":\"8505000\"},"
            + "\"mappings\":{\"_doc\":{\"_source\":{\"enabled\":false}}}}")), "_source disabled after creation (ES 8+)");
        assertFalse(SelectiveEligibility.of("idx", parse("{\"settings\":{\"index.mapping.source.mode\":\"disabled\"},\"mappings\":{}}")),
            "_source disabled from creation");
        assertFalse(SelectiveEligibility.of("idx", parse("{\"settings\":{},\"mappings\":{\"_doc\":{\"_source\":{\"mode\":\"disabled\"}}}}")));
    }

    @Test
    void vectorsReadFromTheVectorFilesTakeTheFullPlan() throws Exception {
        assertFalse(SelectiveEligibility.of("test_es93_vectors_default", metadata("es93-repo", "test_es93_vectors_default")));
        assertTrue(SelectiveEligibility.of("test_es93_vectors_included", metadata("es93-repo", "test_es93_vectors_included")));
    }

    /** The metadata MixedSourceShardTest exports its mixed shard under, both variants. */
    @Test
    void theMixedSourceShardsIndexTakesTheFullPlan() throws Exception {
        JsonNode synthetic = metadata("synth-repo", "test_plain_synth");
        assertFalse(SelectiveEligibility.of("test_plain_synth", synthetic));
        ObjectNode disabled = synthetic.deepCopy();
        ((ObjectNode) disabled.path("settings")).remove("index.mapping.source.mode");
        JsonNode mappings = disabled.path("mappings");
        if (mappings.isArray()) mappings = mappings.path(0);
        ObjectNode mapping = (ObjectNode) (mappings.has("_doc") ? mappings.path("_doc") : mappings);
        mapping.putObject("_source").put("enabled", false);
        assertFalse(SelectiveEligibility.of("test_plain_synth", disabled));
    }

    @Test
    void theLeaseOfAnEligibleShardIsItsNameBasedEstimateWithTheFlagOnly(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.storedShard(tmp).snapshot();
        System.clearProperty(SelectiveFetch.PROPERTY);
        assertEquals(shard.totalSize(), DumpToDeltaPipeline.tempEstimateBytes(shard, "idx", SelectiveShardFixture.STORED_METADATA));
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        long estimate = DumpToDeltaPipeline.tempEstimateBytes(shard, "idx", SelectiveShardFixture.STORED_METADATA);
        assertEquals(SelectiveFetch.estimateBytes(shard), estimate);
        assertTrue(estimate < shard.totalSize());
        assertEquals(shard.totalSize(), DumpToDeltaPipeline.tempEstimateBytes(shard, "idx",
            parse("{\"settings\":{\"index.mode\":\"logsdb\"},\"mappings\":{}}")));
        assertEquals(shard.totalSize(), DumpToDeltaPipeline.tempEstimateBytes(shard));
    }
}
