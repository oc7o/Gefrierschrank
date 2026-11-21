package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static de.sloow.gefrierschrank.delta.VectorExportJvm.canonical;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every index of {@code vec-repo} (scripts/generate_vector_fixture.py, real ES 9.3.0) exported
 * through the production stack at one and at three ranges per shard, against Elasticsearch's own
 * {@code exclude_vectors: false} fetch of the same documents and the stored bytes.
 */
class SourceVectorsExportFixtureTest {

    private static final Path REPO = VectorExportJvm.FIXTURES.resolve("vec-repo");
    private static final List<String> INDICES = List.of(
        "vec_root", "vec_root_included", "vec_object", "vec_nested", "vec_byte_bit", "vec_sparse", "vec_noindex");
    private static final Set<String> REHYDRATED = Set.of("vec_root", "vec_object", "vec_nested", "vec_byte_bit", "vec_sparse");
    private static final Set<String> RESERIALIZED = Set.of("vec_object", "vec_nested");
    private static final Map<String, String> TAG = Map.of(SourceVectorsPlan.FILE_TAG, SourceVectorsPlan.FILE_TAG_VALUE);

    @TempDir
    static Path tmp;
    private static VectorExportJvm.Result single;
    private static VectorExportJvm.Result ranges;

    @BeforeAll
    static void exportAtOneAndThreeRangesPerShard() throws Exception {
        single = VectorExportJvm.export(REPO, tmp.resolve("k1"), INDICES, 1, 1,
            Map.of("gefrierschrank.shard.threads", "1"));
        ranges = VectorExportJvm.export(REPO, tmp.resolve("k3"), INDICES, 1, 1,
            Map.of("gefrierschrank.shard.threads", "3", ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "5"));
    }

    @Test
    void everyIndexCommitsOneRowPerLiveRootEqualToElasticsearchsFetch() throws Exception {
        for (String index : INDICES) {
            JsonNode report = single.reports().get(index + "#1");
            assertEquals(1, report.path("committedCount").asInt(), report.toString());
            assertEquals(0, report.path("quarantinedCount").asInt(), report.toString());
            Map<String, String> expected = VectorExportJvm.rawExpected("vec-" + index + "-expected.json");
            Map<String, VectorExportJvm.Row> rows = VectorExportJvm.rows(tmp.resolve("k1").resolve(index));
            assertEquals(expected.keySet(), rows.keySet(), index + ": no nested child, deleted or soft-deleted doc");
            for (var row : rows.values()) {
                assertEquals(canonical(expected.get(row.id())), canonical(row.source()), index + "/" + row.id());
            }
        }
    }

    @Test
    void rowsKeepEveryStoredByteOrSayTheyAreElasticsearchsRendering() throws Exception {
        for (String index : INDICES) {
            Map<String, String> expected = VectorExportJvm.rawExpected("vec-" + index + "-expected.json");
            int spliced = 0, reserialized = 0, storedRows = 0;
            for (var row : VectorExportJvm.rows(tmp.resolve("k1").resolve(index)).values()) {
                byte[] stored = single.stored().get(index + "/" + row.id());
                assertNotNull(stored, index + "/" + row.id());
                byte[] source = row.source().getBytes(StandardCharsets.UTF_8);
                String where = index + "/" + row.id();
                switch (row.kind()) {
                    case "stored" -> {
                        storedRows++;
                        assertArrayEquals(stored, source, where);
                    }
                    case "stored+vectors" -> {
                        spliced++;
                        assertSplicedAfterStoredBytes(stored, source, where);
                    }
                    case "stored+vectors-reserialized" -> {
                        reserialized++;
                        assertEquals(expected.get(row.id()), row.source(), where + ": Elasticsearch's fetch text");
                    }
                    default -> throw new AssertionError(where + ": " + row.kind());
                }
            }
            if (index.equals("vec_nested")) assertTrue(storedRows > 0, "roots without nested vectors keep their bytes");
            if (!REHYDRATED.contains(index)) {
                assertEquals(0, spliced + reserialized, index);
            } else if (RESERIALIZED.contains(index)) {
                assertTrue(reserialized > 0 && spliced == 0, index);
            } else {
                assertTrue(spliced > 0 && reserialized == 0, index);
            }
        }
    }

    @Test
    void identicalInputVectorsComeBackWithinFourUlpAndBytesExactly() throws Exception {
        Map<String, VectorExportJvm.Row> rehydrated = VectorExportJvm.rows(tmp.resolve("k1").resolve("vec_root"));
        Map<String, VectorExportJvm.Row> included = VectorExportJvm.rows(tmp.resolve("k1").resolve("vec_root_included"));
        assertEquals(included.keySet(), rehydrated.keySet());
        int compared = 0;
        for (String id : included.keySet()) {
            JsonNode a = VectorExportJvm.EXACT.readTree(rehydrated.get(id).source());
            JsonNode b = VectorExportJvm.EXACT.readTree(included.get(id).source());
            for (String field : List.of("embedding", "emb_bbq")) {
                assertEquals(b.has(field), a.has(field), id + "." + field);
                if (!b.has(field)) continue;
                assertEquals(b.get(field).size(), a.get(field).size());
                for (int i = 0; i < b.get(field).size(); i++) {
                    float expected = b.get(field).get(i).floatValue();
                    float actual = a.get(field).get(i).floatValue();
                    assertTrue(Math.abs(actual - expected) <= 4 * Math.ulp(expected),
                        id + "." + field + "[" + i + "] " + actual + " vs " + expected);
                    compared++;
                }
            }
        }
        assertTrue(compared > 1000, "compared " + compared);

        JsonNode sent = VectorExportJvm.EXACT.readTree(VectorExportJvm.FIXTURES.resolve("vec-vec_byte_bit-input.json").toFile());
        for (var row : VectorExportJvm.rows(tmp.resolve("k1").resolve("vec_byte_bit")).values()) {
            JsonNode exported = VectorExportJvm.EXACT.readTree(row.source());
            JsonNode input = sent.get(row.id());
            assertEquals(input.get("b"), exported.get("b"), row.id());
            assertEquals(signedBytes(input.get("bits")), signedBytes(exported.get("bits")), row.id());
        }
    }

    @Test
    void threeRangesPerShardGiveTheSameRows() throws Exception {
        for (String index : INDICES) {
            JsonNode report = ranges.reports().get(index + "#1");
            assertEquals(1, report.path("committedCount").asInt(), report.toString());
            assertEquals(VectorExportJvm.rows(tmp.resolve("k1").resolve(index)),
                VectorExportJvm.rows(tmp.resolve("k3").resolve(index)), index);
        }
        assertTrue(VectorExportJvm.fileTags(tmp.resolve("k3").resolve("vec_root")).size() > 1,
            "the fixture must really be read as several ranges");
    }

    @Test
    void onlyRehydratingIndicesTagTheirFiles() {
        for (String index : INDICES) {
            for (Path table : List.of(tmp.resolve("k1").resolve(index), tmp.resolve("k3").resolve(index))) {
                for (Map<String, String> tags : VectorExportJvm.fileTags(table)) {
                    assertTrue(tags.containsKey(ShardKey.TAG), tags.toString());
                    Map<String, String> extra = new TreeMap<>(tags);
                    extra.remove(ShardKey.TAG);
                    assertEquals(REHYDRATED.contains(index) ? TAG : Map.of(), extra, index);
                }
            }
        }
    }

    private static void assertSplicedAfterStoredBytes(byte[] stored, byte[] row, String where) throws Exception {
        int close = stored.length - 1;
        while (stored[close] != '}') close--;
        assertArrayEquals(Arrays.copyOfRange(stored, 0, close), Arrays.copyOfRange(row, 0, close), where + ": stored prefix");
        int tail = stored.length - close;
        assertArrayEquals(Arrays.copyOfRange(stored, close, stored.length),
            Arrays.copyOfRange(row, row.length - tail, row.length), where + ": stored suffix");
        String inserted = new String(row, close, row.length - tail - close, StandardCharsets.UTF_8);
        boolean emptyObject = new String(stored, 0, close, StandardCharsets.UTF_8).strip().equals("{");
        String members = emptyObject ? inserted : inserted.substring(1);
        assertTrue(emptyObject || inserted.startsWith(","), where + ": " + inserted);
        JsonNode appended = VectorExportJvm.EXACT.readTree("{" + members + "}");
        JsonNode original = VectorExportJvm.EXACT.readTree(stored);
        Iterator<String> names = appended.fieldNames();
        List<String> keys = new ArrayList<>();
        names.forEachRemaining(keys::add);
        assertTrue(!keys.isEmpty(), where);
        for (String key : keys) assertTrue(!original.has(key), where + ": " + key);
        assertEquals(keys.stream().sorted().toList(), keys, where + ": members in path order");
    }

    private static Set<List<Integer>> signedBytes(JsonNode vector) {
        List<Integer> values = new ArrayList<>();
        if (vector.isTextual()) {
            for (byte b : java.util.HexFormat.of().parseHex(vector.textValue())) values.add((int) b);
        } else {
            vector.forEach(value -> values.add(value.intValue()));
        }
        return new HashSet<>(List.of(values));
    }
}
