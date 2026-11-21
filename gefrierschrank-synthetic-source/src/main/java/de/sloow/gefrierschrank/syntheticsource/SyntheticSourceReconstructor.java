package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.IntConsumer;

/**
 * Ambient-facing entry point: given a materialized shard directory plus an index's raw ES
 * metadata (the full {@code JsonNode} from {@code RepositoryInspector.indexMetadata()} - NOT the
 * narrowed {@code IndexMapping}, see that method's Javadoc), reconstructs {@code _source} for
 * documents that have no stored {@code _source} at all (synthetic source).
 *
 * <p>Never itself imports a Lucene or Elasticsearch class: everything that touches those lives in
 * {@link IsolatedEsClassLoader}'s isolated world (see that class's Javadoc for why - the short
 * version: {@code elasticsearch:9.3.0}'s own jar registers Lucene SPI names identical to
 * {@code gefrierschrank-lucene10}'s existing safe stub classes, so the two must never resolve
 * through the same classloader's cached SPI state).
 */
public final class SyntheticSourceReconstructor implements AutoCloseable {

    /**
     * JVM property, the kill switch of reconstructing documents without stored {@code _source} in
     * ES 8+ indices whose mapping is not synthetic: {@code false} restores 1.1.0's
     * {@code SourceNotSyntheticException} (and, before download, its {@code SourceDisabledException}
     * for {@code index.mapping.source.mode: disabled}); anything else, or unset, leaves it on.
     */
    public static final String RECONSTRUCT_DISABLED_SOURCE_PROPERTY = "gefrierschrank.synthetic.reconstructDisabledSource";

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final IntConsumer NO_IGNORED_DOCS = doc -> {};

    private final IsolatedReconstructionWorker worker;
    private final IsolatedEsClassLoader loader;
    // Set right after construction; the constructor keeps its two parameters for tests that wrap a worker.
    private boolean losesUnmappedValues;

    private SyntheticSourceReconstructor(IsolatedReconstructionWorker worker, IsolatedEsClassLoader loader) {
        this.worker = worker;
        this.loader = loader;
    }

    /**
     * Opens a reconstructor over a materialized shard directory.
     *
     * @param shardDir       the materialized Lucene shard directory (same directory a plain
     *                       {@code Lucene10ShardReader} would open)
     * @param indexName      the ES index name, used only as the {@code MapperService}'s internal
     *                       index identifier - never round-tripped into reconstructed documents
     * @param rawIndexMetadata the full per-index metadata {@code JsonNode} as returned by
     *                       {@code RepositoryInspector.indexMetadata(indexName)} - must contain
     *                       both {@code mappings} and {@code settings} subtrees
     */
    public static SyntheticSourceReconstructor open(Path shardDir, String indexName,
                                                      JsonNode rawIndexMetadata) throws IOException {
        return open(shardDir, indexName, rawIndexMetadata, NO_IGNORED_DOCS);
    }

    /**
     * Same as {@link #open(Path, String, JsonNode)}; {@code ignoredDocs} is told the doc id of every
     * document rebuilt without stored {@code _source} through the rewritten mapping whose
     * {@code _ignored} lists a field (values Elasticsearch ignored are missing from its row), as
     * often as it is reconstructed.
     */
    public static SyntheticSourceReconstructor open(Path shardDir, String indexName, JsonNode rawIndexMetadata,
                                                    IntConsumer ignoredDocs) throws IOException {
        return open(shardDir, indexName, rawIndexMetadata, ignoredDocs, IsolatedEsClassLoader.shared());
    }

    /** Opens through {@code loader} instead of the shared one; tests compare against private loaders. */
    static SyntheticSourceReconstructor open(Path shardDir, String indexName, JsonNode rawIndexMetadata,
                                             IsolatedEsClassLoader loader) throws IOException {
        return open(shardDir, indexName, rawIndexMetadata, NO_IGNORED_DOCS, loader);
    }

    private static SyntheticSourceReconstructor open(Path shardDir, String indexName, JsonNode rawIndexMetadata,
                                                     IntConsumer ignoredDocs, IsolatedEsClassLoader loader) throws IOException {
        Inputs inputs = reconstructionInputs(rawIndexMetadata);

        Thread currentThread = Thread.currentThread();
        ClassLoader previousContextLoader = currentThread.getContextClassLoader();
        try {
            // Some ES internals (e.g. XContentBuilder's static init, which loads
            // XContentBuilderExtension via the single-arg ServiceLoader.load(Class) overload) use
            // the CALLING THREAD's context classloader for SPI lookups, not the calling class's
            // own defining classloader. Without swapping this, those lookups would run against
            // the ambient classloader's classpath view even though every class actually executing
            // is defined by the isolated loader - defeating the isolation for exactly the ES
            // internals most likely to also trip over the codec-name collision. Restored in the
            // finally block below regardless of outcome.
            currentThread.setContextClassLoader(loader);
            Class<?> implClass = Class.forName(
                "de.sloow.gefrierschrank.syntheticsource.isolated.IsolatedReconstructionWorkerImpl", true, loader);
            Object instance = implClass
                .getConstructor(Path.class, String.class, String.class, Map.class, String.class, IntConsumer.class)
                .newInstance(shardDir, indexName, inputs.mappingJson(), inputs.flatSettings(), inputs.rewriteMappingJson(),
                    ignoredDocs);
            var reconstructor = new SyntheticSourceReconstructor((IsolatedReconstructionWorker) instance, loader);
            reconstructor.losesUnmappedValues = (Boolean) implClass.getMethod("losesUnmappedValues").invoke(instance);
            return reconstructor;
        } catch (ReflectiveOperationException e) {
            throw buildFailure(e, loader, indexName);
        } catch (LinkageError e) {
            IsolatedEsClassLoader.discard(loader);
            throw e;
        } finally {
            currentThread.setContextClassLoader(previousContextLoader);
        }
    }

    /**
     * What a failed reflective call into the isolated world throws: the target's IOException as
     * is, anything else wrapped with a fixed text. The commit-point preflight translates its own
     * calls the same way, so it fails with the class and message the worker's open fails with.
     */
    static IOException buildFailure(ReflectiveOperationException e, IsolatedEsClassLoader loader, String indexName) {
        Throwable cause = e instanceof java.lang.reflect.InvocationTargetException ite
            ? ite.getTargetException() : e;
        // A class that failed to link or initialize stays failed in its loader: keep this
        // shard's failure from becoming every later synthetic shard's failure.
        if (hasLinkageError(e)) {
            IsolatedEsClassLoader.discard(loader);
        }
        if (cause instanceof IOException ioe) {
            return ioe;
        }
        return new IOException("failed to build synthetic-source reconstruction worker for index "
            + indexName, cause);
    }

    static boolean hasLinkageError(Throwable failure) {
        Throwable t = failure;
        for (int depth = 0; t != null && depth < 64; depth++, t = t.getCause()) {
            if (t instanceof LinkageError) {
                return true;
            }
        }
        return false;
    }

    public int docCount() {
        return worker.docCount();
    }

    /**
     * Whether documents this reconstructor rebuilds lack, uncounted, values sent under
     * {@code dynamic: false}/{@code runtime} objects: only when {@code --source-disabled-accept
     * unmapped-values} let such an index through.
     */
    public boolean losesUnmappedValues() {
        return losesUnmappedValues;
    }

    /**
     * What a worker is built from: the reconstruction mapping and flat settings, and the same
     * mapping without {@code _source} that rebuilds documents without stored {@code _source} of a
     * non-synthetic index ({@code null} when the kill switch is off).
     */
    record Inputs(String mappingJson, Map<String, String> flatSettings, String rewriteMappingJson) {}

    static Inputs reconstructionInputs(JsonNode rawIndexMetadata) throws IOException {
        String mappingJson = extractMappingJson(rawIndexMetadata);
        return new Inputs(mappingJson, extractFlatSettings(rawIndexMetadata),
            SyntheticCoverage.reconstructsDisabledSource() ? rewriteMappingJson(mappingJson) : null);
    }

    /** {@code mappingJson} without its {@code _source} object (enabled, mode, includes, excludes). */
    static String rewriteMappingJson(String mappingJson) throws IOException {
        ObjectNode mapping = (ObjectNode) JSON.readTree(mappingJson);
        mapping.remove("_source");
        return JSON.writeValueAsString(mapping);
    }

    /** Reconstructed {@code _source} of the given document, as a JSON string. */
    public String readSource(int docId) throws IOException {
        return SyntheticSourceJson.toExportText(elasticsearchSource(docId));
    }

    /** Reconstructed {@code _source} of the given document, as the UTF-8 JSON bytes to export. */
    public byte[] readSourceBytes(int docId) throws IOException {
        return SyntheticSourceJson.toExportJson(elasticsearchSource(docId));
    }

    /**
     * Reconstructed {@code _source} of a run of documents, as the UTF-8 JSON bytes to export, element
     * {@code i} for id {@code i}: the same bytes {@link #readSourceBytes} gives each of them, from
     * Elasticsearch's batched loaders. May cover only a prefix of the ids (see
     * {@link IsolatedReconstructionWorker#readSources}); every array is the caller's to keep.
     */
    public byte[][] readSourcesBytes(int[] ascendingDocIds) throws IOException {
        byte[][] sources = elasticsearchSources(ascendingDocIds);
        for (int i = 0; i < sources.length; i++) {
            sources[i] = SyntheticSourceJson.toExportJson(sources[i]);
        }
        return sources;
    }

    private byte[][] elasticsearchSources(int[] ascendingDocIds) throws IOException {
        Thread currentThread = Thread.currentThread();
        ClassLoader previousContextLoader = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(loader);
        try {
            return worker.readSources(ascendingDocIds);
        } finally {
            currentThread.setContextClassLoader(previousContextLoader);
        }
    }

    /** Elasticsearch's rendering, before {@link SyntheticSourceJson}; tests feed it to the export conversion. */
    byte[] elasticsearchSource(int docId) throws IOException {
        Thread currentThread = Thread.currentThread();
        ClassLoader previousContextLoader = currentThread.getContextClassLoader();
        currentThread.setContextClassLoader(loader);
        try {
            return worker.readSource(docId);
        } finally {
            currentThread.setContextClassLoader(previousContextLoader);
        }
    }

    /** The loader that defined this reconstructor's worker. */
    ClassLoader workerLoader() {
        return worker.getClass().getClassLoader();
    }

    /** Closes only the worker: the isolated loader is shared by every reconstructor. */
    @Override
    public void close() throws IOException {
        worker.close();
    }

    /**
     * Pulls the mapping subtree out of the raw per-index metadata and re-serializes it to a JSON
     * string suitable for {@code MapperService.merge(...)}. Handles the same shape quirks
     * {@code RepositoryInspector.indexMapping()} already handles (mappings wrapped in a
     * single-element array; ES 7+ wraps properties in the {@code _doc} type name) - but keeps the
     * FULL mappings subtree (including {@code _source}, {@code _data_stream_timestamp}, etc.),
     * not just {@code properties}, since {@code MapperService.merge()} needs the whole thing.
     *
     * Unsupported persisted field types must fail reconstruction, never be omitted.
     */
    static String extractMappingJson(JsonNode rawIndexMetadata) throws IOException {
        JsonNode mappings = rawIndexMetadata.path("mappings");
        if (mappings.isArray()) {
            mappings = mappings.path(0);
        }
        JsonNode docMapping = mappings.has("_doc") ? mappings.path("_doc") : mappings;
        if (!(docMapping instanceof ObjectNode mappingObject)) {
            throw new IOException("index metadata has no object mapping");
        }
        ObjectNode reconstructionMapping = mappingObject.deepCopy();
        // Templates affect future indexing only. Validating them can require plugins/analyzers
        // unrelated to the persisted fields we are reconstructing.
        reconstructionMapping.remove("dynamic_templates");
        rejectSemanticTextFields(reconstructionMapping);
        return JSON.writeValueAsString(reconstructionMapping);
    }

    private static void rejectSemanticTextFields(ObjectNode mapping) throws IOException {
        JsonNode properties = mapping.get("properties");
        if (properties instanceof ObjectNode propertiesObject) {
            rejectSemanticTextFieldsFrom(propertiesObject);
        }
        JsonNode fields = mapping.get("fields");
        if (fields instanceof ObjectNode fieldsObject) {
            rejectSemanticTextFieldsFrom(fieldsObject);
        }
    }

    private static void rejectSemanticTextFieldsFrom(ObjectNode fields) throws IOException {
        Iterator<Map.Entry<String, JsonNode>> entries = fields.fields();
        while (entries.hasNext()) {
            Map.Entry<String, JsonNode> entry = entries.next();
            JsonNode field = entry.getValue();
            if ("semantic_text".equals(field.path("type").asText())) {
                throw new UnsupportedSyntheticFieldException();
            } else if (field instanceof ObjectNode fieldDefinition) {
                rejectSemanticTextFields(fieldDefinition);
            }
        }
    }

    public static final class UnsupportedSyntheticFieldException extends IOException {
        public UnsupportedSyntheticFieldException() { super("semantic_text synthetic _source requires an unavailable inference mapper"); }
    }

    /**
     * Flattens the raw settings subtree into {@code key -> string value} pairs suitable for
     * {@code Settings.Builder.put(String, String)}. ES's real settings JSON is usually already
     * flat (dotted keys like {@code "index.mapping.source.mode"}), but this also handles a
     * nested shape defensively (recursing into object nodes, joining keys with '.'), and always
     * stringifies arrays/scalars the way {@code Settings} expects (comma-free single value, or
     * repeated keys with a numeric suffix for real ES list settings like
     * {@code index.routing_path}).
     */
    static Map<String, String> extractFlatSettings(JsonNode rawIndexMetadata) {
        Map<String, String> flat = new LinkedHashMap<>();
        flattenInto(rawIndexMetadata.path("settings"), "", flat);
        return flat;
    }

    private static void flattenInto(JsonNode node, String prefix, Map<String, String> out) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String key = prefix.isEmpty() ? field.getKey() : prefix + "." + field.getKey();
                flattenInto(field.getValue(), key, out);
            }
        } else if (node.isArray()) {
            int i = 0;
            for (JsonNode item : node) {
                out.put(prefix + "." + i, item.asText());
                i++;
            }
        } else if (!node.isMissingNode() && !node.isNull()) {
            out.put(prefix, node.asText());
        }
    }
}
