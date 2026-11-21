package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.syntheticsource.SourceVectors;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SourceVectorsPlanTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final SourceVectorsPlan.Prober NEVER = (index, metadata) -> fail("the isolated probe must not run");

    private static final String ALL_SHAPES = """
        {"properties":{
          "embedding":{"type":"dense_vector","dims":3},
          "title":{"type":"text","fields":{"vec":{"type":"sparse_vector"},"raw":{"type":"keyword"}}},
          "meta":{"properties":{"inner":{"properties":{"deep":{"type":"dense_vector","dims":3}}},"tag":{"type":"keyword"}}},
          "chunks":{"type":"nested","properties":{"vector":{"type":"rank_vectors"},"text":{"type":"text"}}},
          "summary":{"type":"semantic_text"},
          "count":{"type":"long"}}}""";

    @Test
    void theMappingWalkFindsVectorsEverywhereInEveryWrapperShape() throws Exception {
        Map<String, String> expected = Map.of(
            "embedding", "dense_vector", "title.vec", "sparse_vector", "meta.inner.deep", "dense_vector",
            "chunks.vector", "rank_vectors", "summary", "semantic_text");
        JsonNode mapping = JSON.readTree(ALL_SHAPES);
        for (JsonNode wrapped : List.of(
                metadata("{}", mapping),
                metadata("{}", JSON.createObjectNode().set("_doc", mapping)),
                metadata("{}", JSON.createArrayNode().add(JSON.createObjectNode().set("_doc", mapping))),
                metadata("{}", JSON.createArrayNode().add(mapping)))) {
            List<String> probed = new ArrayList<>();
            SourceVectorsPlan plan = SourceVectorsPlan.evaluate("idx", withCreated(wrapped, "9060000"), (index, metadata) -> {
                probed.add(index);
                return new SourceVectors.Result(SourceVectors.Verdict.EXCLUDED, Set.of("embedding"));
            });
            assertEquals(expected, plan.candidates());
            assertEquals(List.of("idx"), probed);
            assertEquals(SourceVectors.Verdict.EXCLUDED, plan.verdict());
            assertEquals(Set.of("embedding"), plan.excludedFields());
        }
    }

    @Test
    void metadataWithoutVectorFieldsNeverReachesTheIsolatedWorld() throws Exception {
        JsonNode metadata = metadata("{\"index.version.created\":\"9060000\"}",
            JSON.readTree("{\"properties\":{\"title\":{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}},"
                + "\"vectorish\":{\"properties\":{\"type\":{\"type\":\"keyword\"}}}}}"));
        SourceVectorsPlan plan = SourceVectorsPlan.evaluate("idx", metadata, NEVER);
        assertEquals(SourceVectors.Verdict.NONE, plan.verdict());
        assertTrue(plan.candidates().isEmpty());
        assertEquals(SourceVectors.Verdict.NONE, SourceVectorsPlan.evaluate("idx", null, NEVER).verdict());
        assertEquals(SourceVectors.Verdict.NONE, assertDoesNotThrow(() -> SourceVectorsPlan.require("idx", metadata)).verdict());
    }

    @Test
    void anExplicitFalseSettingIsNoneWithoutTheProbe() throws Exception {
        JsonNode mapping = JSON.readTree(ALL_SHAPES);
        assertEquals(SourceVectors.Verdict.NONE, SourceVectorsPlan.evaluate("idx", metadata(
            "{\"index.version.created\":\"9060000\",\"index.mapping.exclude_source_vectors\":\"false\"}", mapping), NEVER).verdict());
        assertEquals(SourceVectors.Verdict.NONE, SourceVectorsPlan.evaluate("idx", metadata(
            "{\"index\":{\"version\":{\"created\":\"9060000\"},\"mapping\":{\"exclude_source_vectors\":false}}}", mapping), NEVER).verdict());
    }

    @Test
    void indicesCreatedBeforeTheDefaultAreNoneWithoutTheProbeUnlessExplicitlyExcluding() throws Exception {
        JsonNode mapping = JSON.readTree(ALL_SHAPES);
        for (String created : List.of("9034999", "9033000", "8521000", "7170099")) {
            assertEquals(SourceVectors.Verdict.NONE, SourceVectorsPlan.evaluate("idx",
                metadata("{\"index.version.created\":\"" + created + "\"}", mapping), NEVER).verdict(), created);
        }
        List<String> probed = new ArrayList<>();
        SourceVectorsPlan.evaluate("idx", metadata(
            "{\"index.version.created\":\"9033000\",\"index.mapping.exclude_source_vectors\":\"true\"}", mapping),
            (index, metadata) -> {
                probed.add(index);
                return new SourceVectors.Result(SourceVectors.Verdict.NONE, Set.of());
            });
        assertEquals(List.of("idx"), probed, "an explicit true on an older index must still be checked");
        List<String> atDefault = new ArrayList<>();
        SourceVectorsPlan.evaluate("idx", metadata("{\"index.version.created\":\"9035000\"}", mapping), (index, metadata) -> {
            atDefault.add(index);
            return new SourceVectors.Result(SourceVectors.Verdict.NONE, Set.of());
        });
        assertEquals(List.of("idx"), atDefault);
    }

    @Test
    void aFailingProbeIsUnsupportedAndOnlyUnrehydratableVerdictsFailTheIndex() throws Exception {
        JsonNode metadata = metadata("{\"index.version.created\":\"9060000\"}", JSON.readTree(ALL_SHAPES));
        assertEquals(SourceVectors.Verdict.UNSUPPORTED, SourceVectorsPlan.evaluate("idx", metadata, (index, m) -> {
            throw new IOException("probe failed");
        }).verdict());

        SourceVectorsPlan plan = SourceVectorsPlan.require("idx", excludedDense());
        assertEquals(SourceVectors.Verdict.EXCLUDED, plan.verdict());
        assertEquals(Set.of("embedding"), plan.excludedFields());
        assertTrue(plan.rehydrates());

        JsonNode rankVectors = metadata("{\"index.number_of_shards\":\"1\",\"index.number_of_replicas\":\"0\","
            + "\"index.version.created\":\"9060000\"}", JSON.readTree("{\"properties\":{\"v\":{\"type\":\"rank_vectors\"}}}"));
        var failure = assertThrows(SourceVectorsPlan.ExcludedSourceVectorsException.class,
            () -> SourceVectorsPlan.require("idx", rankVectors));
        assertEquals("stored _source excludes vector fields that cannot be rehydrated (rank_vectors, semantic_text, "
            + "or the exclusion could not be evaluated), so the index is not exported", failure.getMessage());
        assertFalse(SourceVectorsPlan.forIndex("idx", rankVectors).rehydrates());
    }

    @Test
    void theKillSwitchRestoresTheRefusalOfEveryExcludingIndex() throws Exception {
        JsonNode excluded = excludedDense();
        String previous = System.getProperty(SourceVectorsPlan.REHYDRATE_PROPERTY);
        System.setProperty(SourceVectorsPlan.REHYDRATE_PROPERTY, "false");
        try {
            var failure = assertThrows(SourceVectorsPlan.ExcludedSourceVectorsException.class,
                () -> SourceVectorsPlan.require("idx", excluded));
            assertEquals("stored _source excludes vector fields (index.mapping.exclude_source_vectors); "
                + "exporting it would lose their values, so the index is not exported", failure.getMessage());
            assertFalse(failure.getMessage().contains("embedding"), "field names stay out of the message");
            assertFalse(SourceVectorsPlan.forIndex("idx", excluded).rehydrates());
        } finally {
            if (previous == null) System.clearProperty(SourceVectorsPlan.REHYDRATE_PROPERTY);
            else System.setProperty(SourceVectorsPlan.REHYDRATE_PROPERTY, previous);
        }
        assertTrue(SourceVectorsPlan.require("idx", excluded).rehydrates(), "read at every decision");
    }

    @Test
    void thePlanIsEvaluatedOncePerMetadataInstance() throws Exception {
        JsonNode excluded = excludedDense();
        SourceVectorsPlan first = SourceVectorsPlan.forIndex("idx", excluded);
        assertSame(first, SourceVectorsPlan.forIndex("idx", excluded));
        assertSame(first, SourceVectorsPlan.require("idx", excluded));
        assertNotSame(first, SourceVectorsPlan.forIndex("idx", excluded.deepCopy()), "keyed by identity, not equality");
        SourceVectorsPlan none = SourceVectorsPlan.forIndex("idx", null);
        assertEquals(SourceVectors.Verdict.NONE, none.verdict());
        assertFalse(none.rehydrates());
        assertFalse(SourceVectorsPlan.none().rehydrates());
    }

    private static JsonNode excludedDense() throws Exception {
        return metadata("{\"index.number_of_shards\":\"1\",\"index.number_of_replicas\":\"0\","
            + "\"index.version.created\":\"9060000\"}",
            JSON.readTree("{\"properties\":{\"embedding\":{\"type\":\"dense_vector\",\"dims\":3}}}"));
    }

    private static JsonNode metadata(String settings, JsonNode mappings) throws Exception {
        var metadata = JSON.createObjectNode();
        metadata.set("settings", JSON.readTree(settings));
        metadata.set("mappings", mappings);
        return metadata;
    }

    private static JsonNode withCreated(JsonNode metadata, String created) {
        var copy = metadata.deepCopy();
        ((com.fasterxml.jackson.databind.node.ObjectNode) copy.path("settings")).put("index.version.created", created);
        return copy;
    }
}
