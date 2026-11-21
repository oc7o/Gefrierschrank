package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The reference: an ES 9.3 container restored the committed es93-repo snapshot read-only and
 * fetched every document of {@code test_es93_vectors_default} with its vectors
 * ({@code scripts/generate_vector_fixture.py --restored-es93} wrote vec-es93-restored-expected.json).
 * The export of the same snapshot must equal that fetch at value level, and the appended vector
 * member must be exactly the text Elasticsearch's fetch wrote for it. Reads only the committed file.
 */
class Es93VectorsRestoredFetchTest {

    private static final String INDEX = "test_es93_vectors_default";

    @Test
    void theExportEqualsTheRestoredSnapshotsFetch(@TempDir Path tmp) throws Exception {
        var result = VectorExportJvm.export(VectorExportJvm.FIXTURES.resolve("es93-repo"), tmp, List.of(INDEX), 1, 1, Map.of());
        Map<String, String> fetched = VectorExportJvm.rawExpected("vec-es93-restored-expected.json");
        Map<String, VectorExportJvm.Row> rows = VectorExportJvm.rows(tmp.resolve(INDEX));
        assertEquals(fetched.keySet(), rows.keySet());
        assertEquals(20, rows.size());
        for (var row : rows.values()) {
            String es = fetched.get(row.id());
            assertEquals(VectorExportJvm.canonical(es), VectorExportJvm.canonical(row.source()), row.id());
            byte[] stored = result.stored().get(INDEX + "/" + row.id());
            String member = row.source().substring(new String(stored, StandardCharsets.UTF_8).length() - 1, row.source().length() - 1);
            assertTrue(member.startsWith(","), member);
            assertTrue(es.endsWith(member + "}"), es + " / " + member);
            JsonNode embedding = VectorExportJvm.EXACT.readTree(row.source()).get("embedding");
            JsonNode esEmbedding = VectorExportJvm.EXACT.readTree(es).get("embedding");
            for (int i = 0; i < esEmbedding.size(); i++) {
                assertEquals(Float.floatToIntBits(esEmbedding.get(i).floatValue()),
                    Float.floatToIntBits(embedding.get(i).floatValue()), row.id() + "[" + i + "]");
            }
        }
    }
}
