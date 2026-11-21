package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Elasticsearch's own mapper code decides which vector fields an index kept out of its stored
 * source; these cases pin the decision against index metadata only (no shard).
 */
class SourceVectorsTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String DENSE = "{\"properties\":{\"title\":{\"type\":\"keyword\"},"
        + "\"embedding\":{\"type\":\"dense_vector\",\"dims\":3,\"similarity\":\"cosine\"}}}";

    @ParameterizedTest(name = "created {0} -> {1}")
    @CsvSource({"9060000, EXCLUDED", "9035000, EXCLUDED", "9034999, NONE", "8521000, NONE"})
    void theCreationVersionBoundaryComesFromElasticsearch(String created, SourceVectors.Verdict verdict) throws Exception {
        SourceVectors.Result result = SourceVectors.probe("idx", metadata(created, Map.of(), DENSE));
        assertEquals(verdict, result.verdict(), result.toString());
        assertEquals(verdict == SourceVectors.Verdict.EXCLUDED ? Set.of("embedding") : Set.of(), result.excludedFields());
    }

    @Test
    void anExplicitSettingOverridesTheVersionDefault() throws Exception {
        assertEquals(new SourceVectors.Result(SourceVectors.Verdict.EXCLUDED, Set.of("embedding")),
            SourceVectors.probe("idx", metadata("9033000", Map.of("index.mapping.exclude_source_vectors", "true"), DENSE)));
        assertEquals(SourceVectors.Verdict.NONE, SourceVectors.probe("idx",
            metadata("9060000", Map.of("index.mapping.exclude_source_vectors", "false"), DENSE)).verdict());
    }

    @Test
    void fieldParametersDecideLikeElasticsearch() throws Exception {
        assertEquals(SourceVectors.Verdict.NONE, SourceVectors.probe("idx", metadata("9060000", Map.of(),
            "{\"properties\":{\"embedding\":{\"type\":\"dense_vector\",\"dims\":3,\"index\":false}}}")).verdict());
        assertEquals(SourceVectors.Verdict.NONE, SourceVectors.probe("idx", metadata("9060000", Map.of(),
            "{\"properties\":{\"tokens\":{\"type\":\"sparse_vector\",\"store\":false}}}")).verdict());
        assertEquals(new SourceVectors.Result(SourceVectors.Verdict.EXCLUDED, Set.of("tokens")), SourceVectors.probe("idx",
            metadata("9060000", Map.of(), "{\"properties\":{\"tokens\":{\"type\":\"sparse_vector\"}}}")));
    }

    @Test
    void syntheticSourceNeverExcludesVectors() throws Exception {
        assertEquals(SourceVectors.Verdict.NONE, SourceVectors.probe("idx",
            metadata("9060000", Map.of("index.mapping.source.mode", "synthetic"), DENSE)).verdict());
        String legacySyntheticMapping = "{\"_source\":{\"mode\":\"synthetic\"},"
            + "\"properties\":{\"embedding\":{\"type\":\"dense_vector\",\"dims\":3}}}";
        // The _source.mode mapping parameter only counts for indices created before 9.0 (ES ignores
        // it later), so this follows Elasticsearch rather than the raw mapping text.
        assertEquals(SourceVectors.Verdict.NONE, SourceVectors.probe("idx",
            metadata("8521000", Map.of("index.mapping.exclude_source_vectors", "true"), legacySyntheticMapping)).verdict());
        assertEquals(SourceVectors.Verdict.EXCLUDED, SourceVectors.probe("idx",
            metadata("9060000", Map.of(), legacySyntheticMapping)).verdict());
    }

    @Test
    void vectorsUnderObjectsAndNestedAreReportedByFullPath() throws Exception {
        String mapping = "{\"properties\":{\"meta\":{\"properties\":{\"embedding\":{\"type\":\"dense_vector\",\"dims\":3}}},"
            + "\"chunks\":{\"type\":\"nested\",\"properties\":{\"text\":{\"type\":\"text\"},"
            + "\"vector\":{\"type\":\"dense_vector\",\"dims\":3}}}}}";
        assertEquals(new SourceVectors.Result(SourceVectors.Verdict.EXCLUDED, Set.of("meta.embedding", "chunks.vector")),
            SourceVectors.probe("idx", metadata("9060000", Map.of(), mapping)));
    }

    @Test
    void unrelatedAnalyzersAndPluginTypesArePrunedBeforeTheBuild() throws Exception {
        String mapping = "{\"properties\":{"
            + "\"body\":{\"type\":\"text\",\"analyzer\":\"english\"},"
            + "\"custom\":{\"type\":\"text\",\"analyzer\":\"my_custom_analyzer\",\"similarity\":\"my_bm25\"},"
            + "\"annotated\":{\"type\":\"annotated_text\"},"
            + "\"embedding\":{\"type\":\"dense_vector\",\"dims\":3}},"
            + "\"runtime\":{\"day\":{\"type\":\"keyword\",\"script\":{\"source\":\"emit('x')\"}}},"
            + "\"dynamic_templates\":[{\"s\":{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":\"text\",\"analyzer\":\"nope\"}}}]}";
        assertEquals(new SourceVectors.Result(SourceVectors.Verdict.EXCLUDED, Set.of("embedding")),
            SourceVectors.probe("idx", metadata("9060000", Map.of(), mapping)));
    }

    @Test
    void logsdbWithStoredSourceBuilds() throws Exception {
        String mapping = "{\"properties\":{\"@timestamp\":{\"type\":\"date\"},"
            + "\"host\":{\"properties\":{\"name\":{\"type\":\"keyword\"}}},"
            + "\"message\":{\"type\":\"text\",\"analyzer\":\"english\"},"
            + "\"embedding\":{\"type\":\"dense_vector\",\"dims\":3}}}";
        assertEquals(new SourceVectors.Result(SourceVectors.Verdict.EXCLUDED, Set.of("embedding")), SourceVectors.probe("logs",
            metadata("9060000", Map.of("index.mode", "logsdb", "index.mapping.source.mode", "stored"), mapping)));
        assertEquals(SourceVectors.Verdict.NONE, SourceVectors.probe("logs",
            metadata("9060000", Map.of("index.mode", "logsdb"), mapping)).verdict(), "logsdb defaults to synthetic source");
    }

    @ParameterizedTest
    @CsvSource({"rank_vectors", "semantic_text"})
    void unregisteredVectorTypesAreUnsupportedOnlyUnderAnActiveExclusion(String type) throws Exception {
        String mapping = "{\"properties\":{\"field\":{\"type\":\"" + type + "\"},"
            + "\"embedding\":{\"type\":\"dense_vector\",\"dims\":3}}}";
        assertEquals(SourceVectors.Verdict.UNSUPPORTED,
            SourceVectors.probe("idx", metadata("9060000", Map.of(), mapping)).verdict());
        assertEquals(SourceVectors.Verdict.NONE, SourceVectors.probe("idx",
            metadata("9060000", Map.of("index.mapping.exclude_source_vectors", "false"), mapping)).verdict());
    }

    @Test
    void metadataWithoutVectorFieldsIsNone() throws Exception {
        assertEquals(SourceVectors.Verdict.NONE, SourceVectors.probe("idx",
            metadata("9060000", Map.of(), "{\"properties\":{\"title\":{\"type\":\"keyword\"}}}")).verdict());
    }

    @Test
    void aMappingElasticsearchRejectsIsUnsupportedNeverNone() throws Exception {
        assertEquals(SourceVectors.Verdict.UNSUPPORTED, SourceVectors.probe("idx", metadata("9060000", Map.of(),
            "{\"properties\":{\"embedding\":{\"type\":\"dense_vector\",\"dims\":-3}}}")).verdict());
    }

    /**
     * A fresh loader per probe loaded Elasticsearch's mapper classes again for every vector index
     * (about 1.6 s each); the probe now shares synthetic reconstruction's loader and leaves it open.
     */
    @Test
    void probesShareTheIsolatedLoaderWithReconstructionAndLeaveItOpen() throws Exception {
        JsonNode excluded = metadata("9060000", Map.of(), DENSE);
        var expected = new SourceVectors.Result(SourceVectors.Verdict.EXCLUDED, Set.of("embedding"));
        assertEquals(expected, SourceVectors.probe("idx", excluded));
        IsolatedEsClassLoader shared = IsolatedEsClassLoader.shared();

        var classLoading = java.lang.management.ManagementFactory.getClassLoadingMXBean();
        long before = classLoading.getLoadedClassCount();
        for (int i = 0; i < 3; i++) assertEquals(expected, SourceVectors.probe("idx-" + i, excluded));
        long grown = classLoading.getLoadedClassCount() - before;

        assertSame(shared, IsolatedEsClassLoader.shared(), "the probe neither closed nor replaced the shared loader");
        assertTrue(grown < 500, "3 probes loaded " + grown + " classes");
        assertNotNull(Class.forName("org.elasticsearch.index.mapper.MapperService", false, shared),
            "the shared loader still serves classes after the probes");
    }

    static JsonNode metadata(String created, Map<String, String> settings, String mapping) throws Exception {
        ObjectNode metadata = JSON.createObjectNode();
        ObjectNode flat = metadata.putObject("settings");
        flat.put("index.number_of_shards", "1");
        flat.put("index.number_of_replicas", "0");
        flat.put("index.version.created", created);
        settings.forEach(flat::put);
        metadata.putArray("mappings").addObject().set("_doc", JSON.readTree(mapping));
        return metadata;
    }
}
