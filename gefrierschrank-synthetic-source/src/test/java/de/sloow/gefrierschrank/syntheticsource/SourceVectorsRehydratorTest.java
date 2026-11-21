package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Elasticsearch's own vector loaders, run in the isolated world over a materialized {@code vec-repo}
 * shard, give back exactly what Elasticsearch's fetch with {@code "_source": {"exclude_vectors":
 * false}} returned for the same documents (fixture: {@code scripts/generate_vector_fixture.py}).
 */
class SourceVectorsRehydratorTest {

    static final Path FIXTURES = Path.of("src/test/resources/fixtures");
    private static final ObjectMapper EXACT = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    /**
     * {@code exactText}: one vector field, so the patched document is Elasticsearch's fetch text byte
     * for byte. With several, Elasticsearch's member order follows a per-JVM randomized map; ours is
     * by path, so the text is compared at value level and the members must come sorted.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource({"vec_root, true, false", "vec_object, false, true", "vec_nested, false, true",
        "vec_byte_bit, true, false", "vec_sparse, true, true"})
    void everyLiveRootGetsElasticsearchsFetchBack(String index, boolean rootLeaves, boolean exactText,
                                                  @TempDir Path tmp) throws Exception {
        Shard shard = Shard.materialize(index, tmp);
        Map<String, String> expected = expectedRaw(index);
        int patched = 0;
        try (SourceVectors.Rehydrator rehydrator = shard.open()) {
            assertEquals(shard.maxDoc, rehydrator.docCount());
            IsolatedSourceVectorsSession.Cursor cursor = rehydrator.newCursor();
            for (Root root : shard.roots) {
                String want = expected.get(root.id);
                int patches = cursor.load(root.doc);
                if (patches == 0) {
                    assertEquals(canonical(want), canonical(root.stored), index + "/" + root.id);
                    continue;
                }
                patched++;
                byte[] reserialized = cursor.reserialize(root.stored, root.stored.length);
                if (exactText) {
                    assertEquals(want, new String(reserialized, StandardCharsets.UTF_8), index + "/" + root.id);
                } else {
                    assertEquals(canonical(want), canonical(reserialized), index + "/" + root.id);
                }
                assertEquals(rootLeaves, cursor.rootLeavesOnly(), index + "/" + root.id);
                if (rootLeaves) {
                    String members = new String(cursor.rootMembers(), StandardCharsets.UTF_8);
                    String[] keys = cursor.rootKeys();
                    assertEquals(java.util.Arrays.stream(keys).sorted().toList(), List.of(keys));
                    for (String key : keys) assertTrue(members.contains("\"" + key + "\":"), key);
                    String reserializedText = new String(reserialized, StandardCharsets.UTF_8);
                    assertTrue(reserializedText.endsWith("," + members + "}"), reserializedText + " / " + members);
                    if (exactText) assertTrue(want.endsWith("," + members + "}"), want + " / " + members);
                }
            }
        }
        assertEquals(expected.keySet(), shard.ids(), index);
        assertTrue(patched > 0, index);
    }

    @Test
    void twoCursorsOnTwoThreadsMatchOneCursor(@TempDir Path tmp) throws Exception {
        Shard shard = Shard.materialize("vec_nested", tmp);
        try (SourceVectors.Rehydrator rehydrator = shard.open()) {
            List<String> single = new ArrayList<>();
            IsolatedSourceVectorsSession.Cursor one = rehydrator.newCursor();
            for (Root root : shard.roots) single.add(rendered(one, root));

            int half = shard.roots.size() / 2;
            ExecutorService threads = Executors.newFixedThreadPool(2);
            try {
                Future<List<String>> first = threads.submit(() -> renderAll(rehydrator, shard.roots.subList(0, half)));
                Future<List<String>> second = threads.submit(() -> renderAll(rehydrator, shard.roots.subList(half, shard.roots.size())));
                List<String> parallel = new ArrayList<>(first.get());
                parallel.addAll(second.get());
                assertEquals(single, parallel);
            } finally {
                threads.shutdownNow();
            }
        }
    }

    @Test
    void aDescendingIdRebuildsTheSegmentLoaders(@TempDir Path tmp) throws Exception {
        Shard shard = Shard.materialize("vec_root", tmp);
        try (SourceVectors.Rehydrator rehydrator = shard.open()) {
            IsolatedSourceVectorsSession.Cursor cursor = rehydrator.newCursor();
            List<String> forward = new ArrayList<>();
            for (Root root : shard.roots) forward.add(rendered(cursor, root));
            List<String> backward = new ArrayList<>();
            for (int i = shard.roots.size() - 1; i >= 0; i--) backward.add(0, rendered(cursor, shard.roots.get(i)));
            assertEquals(forward, backward);
        }
    }

    @Test
    void aShardWhoseMapperNamesOtherFieldsIsAMismatch(@TempDir Path tmp) throws Exception {
        Shard shard = Shard.materialize("vec_root", tmp);
        var failure = assertThrows(SourceVectorsMismatchException.class, () -> SourceVectors.Rehydrator.open(
            shard.dir, "vec_root", shard.metadata, Set.of("embedding")));
        assertEquals("the shard does not match the index's plan for vectors kept out of _source; "
            + "it is not exported without its vectors", failure.getMessage());
        assertFalse(failure.getMessage().contains("synthetic source"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"vec_root_included", "vec_noindex"})
    void indicesThatKeepTheirVectorsAreNotRehydrated(String index) throws Exception {
        var inspector = new RepositoryInspector(new FileSystemBlobStore(FIXTURES.resolve("vec-repo")));
        assertEquals(SourceVectors.Verdict.NONE, SourceVectors.probe(index, inspector.indexMetadata(index)).verdict());
    }

    @Test
    void vectorRehydrationFailuresKeepOnlyTheClassName() {
        var failure = new VectorRehydrationException(new IllegalArgumentException("document text 4711"));
        assertEquals("vectors kept out of _source could not be put back (IllegalArgumentException)", failure.getMessage());
        assertEquals(null, failure.getCause());
        assertTrue(failure instanceof de.sloow.gefrierschrank.core.FailureDiagnostics.SafeMessage);
    }

    private static List<String> renderAll(SourceVectors.Rehydrator rehydrator, List<Root> roots) throws IOException {
        IsolatedSourceVectorsSession.Cursor cursor = rehydrator.newCursor();
        List<String> out = new ArrayList<>();
        for (Root root : roots) out.add(rendered(cursor, root));
        return out;
    }

    private static String rendered(IsolatedSourceVectorsSession.Cursor cursor, Root root) throws IOException {
        int patches = cursor.load(root.doc);
        if (patches == 0) return root.id + " none";
        byte[] members = cursor.rootLeavesOnly() ? cursor.rootMembers() : new byte[0];
        return root.id + " " + patches + " " + new String(members, StandardCharsets.UTF_8) + " "
            + new String(cursor.reserialize(root.stored, root.stored.length), StandardCharsets.UTF_8);
    }

    /**
     * {@code vec-<index>-expected.json}: one document per line, {@code "<id>": <raw bytes ES returned>}.
     */
    static Map<String, String> expectedRaw(String index) throws IOException {
        Map<String, String> raw = new TreeMap<>();
        for (String line : Files.readAllLines(FIXTURES.resolve("vec-" + index + "-expected.json"), StandardCharsets.UTF_8)) {
            if (line.equals("{") || line.equals("}")) continue;
            String entry = line.strip();
            if (entry.endsWith(",")) entry = entry.substring(0, entry.length() - 1);
            int colon = entry.indexOf("\": ");
            raw.put(EXACT.readValue(entry.substring(0, colon + 1), String.class), entry.substring(colon + 3));
        }
        return raw;
    }

    /** Value-level form: object members unordered, numbers as exact decimals. */
    static Object canonical(String json) throws IOException {
        return canonical(EXACT.readTree(json));
    }

    static Object canonical(byte[] json) throws IOException {
        return canonical(EXACT.readTree(json));
    }

    private static Object canonical(JsonNode node) {
        if (node.isObject()) {
            Map<String, Object> members = new TreeMap<>();
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                var field = fields.next();
                members.put(field.getKey(), canonical(field.getValue()));
            }
            return members;
        }
        if (node.isArray()) {
            List<Object> items = new ArrayList<>();
            node.forEach(item -> items.add(canonical(item)));
            return items;
        }
        if (node.isNumber()) return node.decimalValue().stripTrailingZeros().toPlainString();
        return node.isNull() ? "null" : node.toString();
    }

    record Root(int doc, String id, byte[] stored) {}

    /** A materialized shard 0 of a vec-repo index and its live roots, read with plain Lucene. */
    record Shard(Path dir, JsonNode metadata, String index, int maxDoc, List<Root> roots) {
        static Shard materialize(String index, Path tmp) throws Exception {
            var store = new FileSystemBlobStore(FIXTURES.resolve("vec-repo"));
            var inspector = new RepositoryInspector(store);
            Path dir = tmp.resolve("shard");
            new ShardMaterializer(store).materialize(inspector.shardSnapshot(index, 0), dir);
            List<Root> roots = new ArrayList<>();
            @SuppressWarnings("unchecked")
            List<Object[]> listed = (List<Object[]>) isolatedCall("liveRoots", dir);
            int maxDoc = (Integer) listed.get(0)[0];
            for (Object[] root : listed.subList(1, listed.size())) {
                roots.add(new Root((Integer) root[0], (String) root[1], (byte[]) root[2]));
            }
            return new Shard(dir, inspector.indexMetadata(index), index, maxDoc, roots);
        }

        private static Object isolatedCall(String method, Path dir) throws Exception {
            ClassLoader loader = IsolatedEsClassLoader.shared();
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            thread.setContextClassLoader(loader);
            try {
                return Class.forName("de.sloow.gefrierschrank.syntheticsource.isolated.StoredSourceProbe", true, loader)
                    .getMethod(method, Path.class).invoke(null, dir);
            } finally {
                thread.setContextClassLoader(previous);
            }
        }

        SourceVectors.Rehydrator open() throws IOException {
            return SourceVectors.Rehydrator.open(dir, index, metadata, SourceVectors.probe(index, metadata).excludedFields());
        }

        Set<String> ids() {
            Map<String, Boolean> ids = new LinkedHashMap<>();
            roots.forEach(root -> ids.put(root.id, true));
            return new java.util.TreeSet<>(ids.keySet());
        }
    }

    @Test
    void canonicalFormComparesNumbersByValue() throws Exception {
        assertEquals(canonical("{\"a\":[1.0,2],\"b\":{\"c\":-0.38599998}}"), canonical("{\"b\":{\"c\":-0.385999980},\"a\":[1,2.00]}"));
        assertFalse(canonical("[0.1]").equals(canonical("[0.10000001]")));
        assertArrayEquals(new BigDecimal[] {new BigDecimal("1")}, new BigDecimal[] {new BigDecimal("1.0").stripTrailingZeros()});
    }
}
