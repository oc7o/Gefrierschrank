package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The commit-point preflight decides on a segment's FieldInfos alone; it must decide exactly as
 * reconstruction does on the opened segment. Pinned on every segment of every fixture shard, so an
 * Elasticsearch upgrade that changes what its loaders ask a leaf fails here, not during an export.
 */
class FieldInfosLeafReaderTest {

    @Test
    void theFieldInfosLeafRefusesExactlyAsTheRealLeafOnEveryFixtureSegment() throws Exception {
        List<String> differences = new ArrayList<>();
        var segments = FixtureSegments.all();
        for (var segment : segments) {
            if (!segment.real().equals(segment.stub())) {
                differences.add(segment.where() + ": real [" + segment.real() + "] stub [" + segment.stub() + "]");
            }
        }
        assertTrue(segments.size() > 20, "fixture segments: " + segments.size());
        assertTrue(segments.stream().anyMatch(s -> !s.real().isEmpty()), "the rule refuses somewhere, so equality is not vacuous");
        assertEquals(List.of(), differences);
    }

    /**
     * A field whose every document was deleted and merged away keeps its FieldInfo with an empty
     * dictionary, for which Elasticsearch's keyword loader is null on the real leaf. Nothing is
     * lost, so neither leaf refuses and both live documents reconstruct.
     */
    @Test
    void aFieldWithAnEmptyDictionaryRefusesOnNeitherLeaf(@TempDir Path tmp) throws Exception {
        Map<String, String> settings = Map.of("index.number_of_shards", "1", "index.number_of_replicas", "0",
            "index.version.created", "9039000");
        String mapping = "{\"_source\":{\"enabled\":false},\"properties\":{\"k\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"}}}";
        JsonNode result = new ObjectMapper().readTree(matrixProbe(mapping, settings,
            List.of("{\"k\":\"x\",\"n\":1}", "{\"n\":2}", "{\"n\":3}"), tmp, "delete 0"));
        assertEquals(1, result.path("leaves").size(), result.toString());
        assertEquals("", result.path("leaves").get(0).path("real").asText(), result.toString());
        assertEquals("", result.path("leaves").get(0).path("stub").asText(), result.toString());
        assertEquals(2, result.path("docs").size(), result.toString());
        for (JsonNode doc : result.path("docs")) assertTrue(doc.has("rewrite"), result.toString());
    }

    private static String matrixProbe(String mapping, Map<String, String> settings, List<String> documents, Path tmp,
                                      String inject) throws Exception {
        IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            return (String) Class.forName("de.sloow.gefrierschrank.syntheticsource.isolated.CoverageMatrixProbe", true, loader)
                .getMethod("run", String.class, Map.class, List.class, String.class, String.class)
                .invoke(null, mapping, settings, documents, tmp.toString(), inject);
        } catch (InvocationTargetException e) {
            throw e.getTargetException() instanceof Exception cause ? cause : e;
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
