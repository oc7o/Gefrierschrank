package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Nested roots go through the same export conversion as every synthetic document:
 * where the fast path passes Elasticsearch's bytes through, they equal the char path's text, and
 * every root's export bytes equal the golden. Children are refused by the worker, so only roots
 * are read (SyntheticSourceJsonTest's fixture sweep reads every doc id and therefore leaves nested out).
 */
class NestedSyntheticFastPathTest {

    private static final String NESTED = "synth-deletes-repo/test_deletes_nested";

    @Test
    void nestedRootsTakeTheFastPathOnlyWhereItIsIdentical(@TempDir Path tmp) throws Exception {
        List<String> golden = new ArrayList<>();
        new ObjectMapper().readTree(SyntheticSourceReconstructorTest.GOLDEN.toFile()).path("reconstructed")
            .path(NESTED).forEach(doc -> golden.add(doc.asText()));
        int roots = 0;
        int hits = 0;
        try (var reconstructor = SyntheticSourceReconstructorTest.Fixture.materialize(NESTED, tmp).open()) {
            assertEquals(golden.size(), reconstructor.docCount());
            for (int doc = 0; doc < reconstructor.docCount(); doc++) {
                if (golden.get(doc).startsWith("!")) {
                    assertEquals("!IllegalArgumentException", golden.get(doc), "doc " + doc);
                    continue;
                }
                roots++;
                byte[] esJson = reconstructor.elasticsearchSource(doc);
                byte[] fast = SyntheticSourceJson.fastPath(esJson);
                if (fast != null) {
                    assertSame(esJson, fast);
                    assertArrayEquals(SyntheticSourceJson.toExportText(esJson).getBytes(StandardCharsets.UTF_8), fast);
                    hits++;
                }
                assertArrayEquals(golden.get(doc).getBytes(StandardCharsets.UTF_8), SyntheticSourceJson.toExportJson(esJson),
                    "doc " + doc);
            }
        }
        assertTrue(roots > 0 && hits > 0, roots + " roots, " + hits + " fast-path hits");
    }
}
