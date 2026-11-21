package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Which vector fields an index left out of its stored {@code _source}.
 *
 * <p>Indices created on ES 9.2+ exclude {@code dense_vector} (when indexed) and {@code sparse_vector}
 * (when stored) values from the stored {@code _source} by default
 * ({@code index.mapping.exclude_source_vectors}); ES re-adds them at search time from the vector
 * files. Exporting such a stored source verbatim silently drops the embeddings. The decision
 * depends on the creation version, the setting, every field's parameters and the source mode, so
 * Elasticsearch's own mapper code makes it: {@link #probe} builds a {@code MapperService} in the
 * isolated world (see {@link IsolatedEsClassLoader}) from a mapping pruned to the vector fields and
 * what index-mode validation needs, and reads {@code MappingLookup.syntheticVectorFields()}.
 * Pruning keeps unrelated custom analyzers, similarities and plugin types from failing the build.
 *
 * <p>{@link Rehydrator} puts the excluded values back: per shard, the same pruned mapping drives
 * Elasticsearch's own {@code SyntheticVectorsLoader} over the materialized shard, as its fetch phase
 * does with {@code "_source": {"exclude_vectors": false}}.
 *
 * <p>Never imports a Lucene or Elasticsearch class; only JDK types cross the loader boundary.
 */
public final class SourceVectors {

    /** Field types whose values ES 9.2+ may keep out of the stored source. */
    public static final Set<String> VECTOR_TYPES = Set.of("dense_vector", "sparse_vector", "rank_vectors", "semantic_text");

    /** Candidate types the isolated mapper registry cannot build (x-pack/inference plugins). */
    private static final Set<String> UNREGISTERED_TYPES = Set.of("rank_vectors", "semantic_text");

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PROBE_CLASS = "de.sloow.gefrierschrank.syntheticsource.isolated.IsolatedSourceVectorsProbe";
    private static final String SESSION_CLASS = "de.sloow.gefrierschrank.syntheticsource.isolated.IsolatedSourceVectorsSessionImpl";

    public enum Verdict {
        /** The stored source is complete: no vector field, exclusion off, or ES excludes nothing. */
        NONE,
        /** ES keeps {@link Result#excludedFields()} out of the stored source. */
        EXCLUDED,
        /** Exclusion is active and ES could not (or not verifiably) say which fields it affects. */
        UNSUPPORTED
    }

    public record Result(Verdict verdict, Set<String> excludedFields) {
        static final Result NONE = new Result(Verdict.NONE, Set.of());
        static final Result UNSUPPORTED = new Result(Verdict.UNSUPPORTED, Set.of());
    }

    private SourceVectors() {}

    /**
     * Full dotted path to type of every vector-typed field in the index mappings: root fields,
     * object and nested sub-fields and multi-fields. Handles the {@code mappings} array and
     * {@code _doc} wrappers. Pure JSON work: never touches an isolated class loader.
     */
    public static Map<String, String> vectorFieldCandidates(JsonNode rawIndexMetadata) {
        Map<String, String> candidates = new LinkedHashMap<>();
        JsonNode mapping = documentMapping(rawIndexMetadata);
        if (mapping != null) collect(mapping.path("properties"), "", candidates);
        return candidates;
    }

    private static void collect(JsonNode properties, String prefix, Map<String, String> candidates) {
        if (!properties.isObject()) return;
        Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String path = prefix + field.getKey();
            String type = field.getValue().path("type").asText("");
            if (VECTOR_TYPES.contains(type)) candidates.put(path, type);
            collect(field.getValue().path("properties"), path + ".", candidates);
            collect(field.getValue().path("fields"), path + ".", candidates);
        }
    }

    /**
     * Evaluates the index in the isolated world. Any failure is {@link Verdict#UNSUPPORTED}, never
     * {@link Verdict#NONE}: a misjudgment must fail loudly instead of exporting stripped rows.
     */
    public static Result probe(String indexName, JsonNode rawIndexMetadata) {
        Map<String, String> candidates = vectorFieldCandidates(rawIndexMetadata);
        if (candidates.isEmpty()) return Result.NONE;
        try {
            Map<String, String> settings = SyntheticSourceReconstructor.extractFlatSettings(rawIndexMetadata);
            String mapping = JSON.writeValueAsString(prunedMapping(documentMapping(rawIndexMetadata), candidates, settings));
            List<String> answer = runProbe(indexName, mapping, settings);
            if (!Boolean.parseBoolean(answer.get(0))) return Result.NONE;
            if (candidates.values().stream().anyMatch(UNREGISTERED_TYPES::contains)) return Result.UNSUPPORTED;
            Set<String> excluded = new TreeSet<>(answer.subList(1, answer.size()));
            if (!candidates.keySet().containsAll(excluded)) return Result.UNSUPPORTED;
            return excluded.isEmpty() ? Result.NONE : new Result(Verdict.EXCLUDED, Set.copyOf(excluded));
        } catch (Exception | LinkageError e) {
            return Result.UNSUPPORTED;
        }
    }

    /**
     * The one place this class obtains an isolated loader: the process-wide one that synthetic
     * reconstruction also uses, so a probe costs a MapperService build instead of loading
     * Elasticsearch's classes again. Never closed here; other threads may be using it.
     */
    private static IsolatedEsClassLoader loader() {
        return IsolatedEsClassLoader.shared();
    }

    @SuppressWarnings("unchecked")
    private static List<String> runProbe(String indexName, String mapping, Map<String, String> settings) throws Exception {
        IsolatedEsClassLoader loader = loader();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        // Same reason as SyntheticSourceReconstructor.open: ES SPI lookups use the context loader.
        thread.setContextClassLoader(loader);
        try {
            return new ArrayList<>((List<String>) Class.forName(PROBE_CLASS, true, loader)
                .getMethod("probe", String.class, String.class, Map.class)
                .invoke(null, indexName, mapping, settings));
        } catch (InvocationTargetException e) {
            discardIfPoisoned(loader, e);
            throw e.getCause() instanceof Exception cause ? cause : e;
        } catch (Exception | LinkageError e) {
            discardIfPoisoned(loader, e);
            throw e;
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    /** Same rule as SyntheticSourceReconstructor.open: a class that failed to link stays failed in its loader. */
    private static void discardIfPoisoned(IsolatedEsClassLoader loader, Throwable failure) {
        if (SyntheticSourceReconstructor.hasLinkageError(failure)) IsolatedEsClassLoader.discard(loader);
    }

    /**
     * One shard's rehydration session in the isolated world, opened eagerly on the shard's thread.
     * Every call into the session or one of its cursors runs with the isolated loader as the
     * thread's context loader, as {@link SyntheticSourceReconstructor} does. Closing it never closes
     * the shared loader.
     */
    public static final class Rehydrator implements IsolatedSourceVectorsSession {
        private final IsolatedSourceVectorsSession session;
        private final IsolatedEsClassLoader loader;

        private Rehydrator(IsolatedSourceVectorsSession session, IsolatedEsClassLoader loader) {
            this.session = session;
            this.loader = loader;
        }

        /**
         * Builds the pruned mapping {@link #probe} built for this index and opens the shard with it.
         * {@code expectedFields} are the probe's excluded fields: a shard whose mapper names other
         * ones fails with {@link SourceVectorsMismatchException}.
         */
        public static Rehydrator open(Path shardDir, String indexName, JsonNode rawIndexMetadata,
                                      Set<String> expectedFields) throws IOException {
            ObjectNode mapping = documentMapping(rawIndexMetadata);
            if (mapping == null) throw new SourceVectorsMismatchException();
            Map<String, String> settings;
            String prunedMapping;
            try {
                settings = SyntheticSourceReconstructor.extractFlatSettings(rawIndexMetadata);
                prunedMapping = JSON.writeValueAsString(prunedMapping(mapping, vectorFieldCandidates(rawIndexMetadata), settings));
            } catch (IOException | RuntimeException e) {
                throw new VectorRehydrationException(e);
            }
            IsolatedEsClassLoader loader = loader();
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            thread.setContextClassLoader(loader);
            try {
                Object session = Class.forName(SESSION_CLASS, true, loader)
                    .getConstructor(Path.class, String.class, String.class, Map.class, Set.class)
                    .newInstance(shardDir, indexName, prunedMapping, settings, Set.copyOf(expectedFields));
                return new Rehydrator((IsolatedSourceVectorsSession) session, loader);
            } catch (InvocationTargetException e) {
                discardIfPoisoned(loader, e);
                Throwable cause = e.getTargetException();
                if (cause instanceof SourceVectorsMismatchException || cause instanceof VectorRehydrationException) {
                    throw (IOException) cause;
                }
                if (cause instanceof Error error) throw error;
                throw new VectorRehydrationException(cause);
            } catch (ReflectiveOperationException | RuntimeException e) {
                discardIfPoisoned(loader, e);
                throw new VectorRehydrationException(e);
            } catch (LinkageError e) {
                IsolatedEsClassLoader.discard(loader);
                throw e;
            } finally {
                thread.setContextClassLoader(previous);
            }
        }

        @Override
        public int docCount() {
            return session.docCount();
        }

        @Override
        public Cursor newCursor() {
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            thread.setContextClassLoader(loader);
            try {
                return new ContextCursor(session.newCursor());
            } finally {
                thread.setContextClassLoader(previous);
            }
        }

        @Override
        public void close() throws IOException {
            isolatedIo(() -> {
                session.close();
                return null;
            });
        }

        private <T> T isolatedIo(IsolatedCall<T> call) throws IOException {
            Thread thread = Thread.currentThread();
            ClassLoader previous = thread.getContextClassLoader();
            thread.setContextClassLoader(loader);
            try {
                return call.run();
            } catch (LinkageError e) {
                IsolatedEsClassLoader.discard(loader);
                throw e;
            } finally {
                thread.setContextClassLoader(previous);
            }
        }

        @FunctionalInterface
        private interface IsolatedCall<T> {
            T run() throws IOException;
        }

        /** Swaps the context loader around every call of the isolated cursor. */
        private final class ContextCursor implements Cursor {
            private final Cursor cursor;

            ContextCursor(Cursor cursor) {
                this.cursor = cursor;
            }

            @Override
            public int load(int globalDocId) throws IOException {
                return isolatedIo(() -> cursor.load(globalDocId));
            }

            @Override
            public boolean rootLeavesOnly() {
                return cursor.rootLeavesOnly();
            }

            @Override
            public String[] rootKeys() {
                return cursor.rootKeys();
            }

            @Override
            public byte[] rootMembers() throws IOException {
                return isolatedIo(cursor::rootMembers);
            }

            @Override
            public byte[] reserialize(byte[] stored, int length) throws IOException {
                return isolatedIo(() -> cursor.reserialize(stored, length));
            }
        }
    }

    private static ObjectNode documentMapping(JsonNode rawIndexMetadata) {
        JsonNode mappings = rawIndexMetadata == null ? null : rawIndexMetadata.path("mappings");
        if (mappings == null) return null;
        if (mappings.isArray()) mappings = mappings.path(0);
        JsonNode mapping = mappings.has("_doc") ? mappings.path("_doc") : mappings;
        return mapping instanceof ObjectNode object ? object : null;
    }

    /**
     * Keeps the dense/sparse vector candidates verbatim with their ancestor objects, the root
     * metadata mappers that decide the source mode and routing, and the fields index-mode
     * validation needs (@timestamp, host.name, dimensions, routing_path, index sort fields). Drops
     * runtime fields and dynamic templates.
     */
    static ObjectNode prunedMapping(ObjectNode mapping, Map<String, String> candidates, Map<String, String> settings) {
        List<Pattern> required = new ArrayList<>(List.of(Pattern.compile(Pattern.quote("@timestamp")),
            Pattern.compile(Pattern.quote("host.name"))));
        settings.forEach((key, value) -> {
            if (key.startsWith("index.routing_path") || key.startsWith("index.sort.field")) {
                for (String field : value.split(",")) required.add(glob(field.strip()));
            }
        });
        Set<String> vectors = new TreeSet<>();
        candidates.forEach((path, type) -> {
            if (!UNREGISTERED_TYPES.contains(type)) vectors.add(path);
        });

        ObjectNode pruned = JSON.createObjectNode();
        for (String key : List.of("_source", "_routing", "_data_stream_timestamp", "subobjects", "dynamic", "enabled")) {
            if (mapping.has(key)) pruned.set(key, mapping.get(key).deepCopy());
        }
        ObjectNode properties = pruneProperties(mapping.path("properties"), "", vectors, required);
        if (properties != null) pruned.set("properties", properties);
        return pruned;
    }

    private static ObjectNode pruneProperties(JsonNode properties, String prefix, Set<String> vectors, List<Pattern> required) {
        if (!properties.isObject()) return null;
        ObjectNode kept = JSON.createObjectNode();
        Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String path = prefix + field.getKey();
            JsonNode definition = field.getValue();
            boolean keep = vectors.contains(path) || definition.path("time_series_dimension").asBoolean(false)
                || required.stream().anyMatch(pattern -> pattern.matcher(path).matches())
                || vectors.stream().anyMatch(vector -> vector.startsWith(path + ".") && hasMultiField(definition));
            if (keep) {
                kept.set(field.getKey(), definition.deepCopy());
                continue;
            }
            ObjectNode children = pruneProperties(definition.path("properties"), path + ".", vectors, required);
            if (children != null && !children.isEmpty()) {
                ObjectNode ancestor = JSON.createObjectNode();
                for (String key : List.of("type", "subobjects", "enabled", "dynamic", "include_in_parent", "include_in_root")) {
                    if (definition.has(key)) ancestor.set(key, definition.get(key).deepCopy());
                }
                ancestor.set("properties", children);
                kept.set(field.getKey(), ancestor);
            }
        }
        return kept;
    }

    private static boolean hasMultiField(JsonNode definition) {
        return definition.path("fields").isObject() && !definition.path("fields").isEmpty();
    }

    private static Pattern glob(String pattern) {
        StringBuilder regex = new StringBuilder();
        for (String part : pattern.split("\\*", -1)) {
            if (!regex.isEmpty()) regex.append(".*");
            regex.append(Pattern.quote(part));
        }
        return Pattern.compile(regex.toString());
    }
}
