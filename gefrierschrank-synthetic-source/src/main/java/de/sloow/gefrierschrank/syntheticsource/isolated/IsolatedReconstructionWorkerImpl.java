package de.sloow.gefrierschrank.syntheticsource.isolated;

import de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ProvidedIdFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Lives in this "isolated" subpackage on purpose: everything in it must be defined by
 * {@code IsolatedEsClassLoader} (see that class's Javadoc for why), so its Lucene/Elasticsearch
 * type references resolve against the REAL {@code ES819TSDBDocValuesFormat} etc, not against
 * {@code gefrierschrank-lucene10}'s safe no-op stub classes of the same SPI name.
 *
 * <p>Opens its OWN {@link DirectoryReader} over the shard directory (a plain Lucene reader, not
 * the ambient {@code Lucene10ShardReader}) so every codec class it touches - most importantly the
 * TSDB doc-values format used to decode the very doc values synthetic source is reconstructed
 * from - is resolved through this isolated world, never the ambient one. Implements the ambient
 * {@link IsolatedReconstructionWorker} interface (loaded by the AMBIENT classloader, shared
 * across the boundary - see that interface's Javadoc) so callers outside the isolated world can
 * use it without ever naming a Lucene/ES type themselves.
 */
public final class IsolatedReconstructionWorkerImpl implements IsolatedReconstructionWorker {

    private final Directory directory;
    private final DirectoryReader reader;
    private final PrunedSegmentReaders.Tripwire tripwire;
    private final Setup setup;
    private final MapperService mapperService;
    private final SourceLoader sourceLoader;
    private final StoredFieldLoader storedFieldLoader;
    private final long batchBytes = Long.getLong(BATCH_BYTES_PROPERTY, DEFAULT_BATCH_BYTES);

    // Per-leaf cursor, reused while doc ids ascend within one segment (guarded by this).
    private int cursorLeafOrd = -1;
    private int cursorLeafDoc = -1;
    private SourceLoader.Leaf cursorSource;
    private LeafStoredFieldLoader cursorStoredFields;
    // Per-leaf state of enterLeaf, by leaf ord; an entry exists only for a leaf that was entered successfully.
    private final java.util.Map<Integer, LeafEntry> leafEntries = new java.util.HashMap<>();
    // Receives the global ids of rewrite-mode documents whose _ignored lists a field.
    private java.util.function.IntConsumer ignoredDocs;

    /**
     * @param rewriteMappingJson the mapping without {@code _source}, to rebuild documents without
     *                           stored {@code _source} of a non-synthetic ES 8+ index; null keeps
     *                           1.1.0's {@link SourceNotSyntheticException}
     * @param ignoredDocs        told the global id of every document rebuilt through the rewrite whose
     *                           {@code _ignored} lists a field; null for none
     */
    public IsolatedReconstructionWorkerImpl(Path shardDir, String indexName, String rawMappingJson,
                                             Map<String, String> flatSettings, String rewriteMappingJson,
                                             java.util.function.IntConsumer ignoredDocs) throws IOException {
        boolean memoryMapped = memoryMapped();
        this.setup = Setup.build(indexName, rawMappingJson, flatSettings, rewriteMappingJson);
        try {
            this.mapperService = setup.mapperService();
            this.ignoredDocs = ignoredDocs == null ? doc -> {} : ignoredDocs;
            this.sourceLoader = setup.sourceLoader();
            this.storedFieldLoader = setup.storedFieldLoader();
            OpenedReader opened = openReader(shardDir, memoryMapped, true);
            this.directory = opened.directory();
            this.reader = opened.reader();
            this.tripwire = opened.tripwire();
        } catch (IOException | RuntimeException | Error e) {
            try { setup.close(); } catch (IOException close) { e.addSuppressed(close); }
            throw e;
        }
    }

    /**
     * Everything a reconstruction is built from before a shard file is opened, built once by the
     * worker and by the selective-fetch probe alike: the MapperService (the rewrite when documents
     * without stored {@code _source} of a non-synthetic index are rebuilt), its coverage plan, the
     * nested root filter, and the source and stored-field loaders. Closing it closes the MapperService.
     *
     * @param coverage   non-null when the mapping is not synthetic and documents without stored
     *                   {@code _source} are rebuilt through the rewritten mapping ({@code mapperService}
     *                   is then the rewrite)
     * @param rootFilter selects root documents; null when the mapping has no nested fields, so no id can be a child
     */
    record Setup(MapperService mapperService, BitSetProducerCache producers, ReconstructionCoverage coverage,
                 boolean losesUnmappedValues, Query rootFilter, SourceLoader sourceLoader,
                 StoredFieldLoader storedFieldLoader) implements java.io.Closeable {

        static Setup build(String indexName, String rawMappingJson, Map<String, String> flatSettings,
                           String rewriteMappingJson) throws IOException {
            IsolatedEsBootstrap.ensure();
            BitSetProducerCache producers = new BitSetProducerCache();
            MapperService openedMapper = null;
            try {
                openedMapper = buildMapperService(indexName, rawMappingJson, flatSettings, producers::producer);
                // Before any file is opened: without synthetic source, and without a rewrite that can
                // stand in for it, there is nothing to reconstruct, whatever the segments are (an ES 7
                // index without _source is also too old to open here).
                boolean acceptUnmappedValues = de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage.acceptsUnmappedValues();
                ReconstructionCoverage coverage = ReconstructionCoverage.open(indexName, openedMapper, rewriteMappingJson,
                    flatSettings, producers::producer, acceptUnmappedValues);
                boolean losesUnmappedValues = false;
                if (coverage != null) {
                    openedMapper = coverage.rewrite();
                    losesUnmappedValues = acceptUnmappedValues && coverage.losesUnmappedValues();
                }
                Query rootFilter = null;
                if (openedMapper.mappingLookup().nestedLookup() != org.elasticsearch.index.mapper.NestedLookup.EMPTY) {
                    rootFilter = org.elasticsearch.common.lucene.search.Queries.newNonNestedFilter(
                        openedMapper.getIndexSettings().getIndexVersionCreated());
                }
                SourceLoader sourceLoader = openedMapper.mappingLookup()
                    .newSourceLoader(new SourceFilter(null, null), SourceFieldMetrics.NOOP);
                // sourceLoader.requiredStoredFields() (not a hardcoded empty set): some field types'
                // synthetic-source reconstruction is FALLBACK-mode, not NATIVE (doc-values-based) -
                // e.g. shape/geo_shape (AbstractGeometryFieldMapper doesn't override
                // FieldMapper.syntheticSourceSupport(), so it defaults to SyntheticSourceSupport.FALLBACK)
                // - and reads back through the _ignored_source metadata field rather than doc values.
                // Loading no stored fields at all (Set.of()) would silently produce an empty/
                // missing value for exactly those types instead of failing loudly - verified by hand
                // against the version/counted_keyword/unsigned_long/shape/geo_shape fields of the
                // test_plain_synth fixture.
                // forceSequentialReader: read through each segment's merge instance, see
                // SequentialStoredFieldsDirectoryReader.
                StoredFieldLoader storedFieldLoader = StoredFieldLoader.create(false, sourceLoader.requiredStoredFields(), true);
                return new Setup(openedMapper, producers, coverage, losesUnmappedValues, rootFilter, sourceLoader,
                    storedFieldLoader);
            } catch (IOException | RuntimeException | Error e) {
                if (openedMapper != null) try { openedMapper.close(); } catch (IOException close) { e.addSuppressed(close); }
                throw e;
            }
        }

        /**
         * Everything a read method needs before it builds a leaf's loaders: in rewrite mode the leaf's
         * coverage check (a refusal throws) and its documents listing a field in {@code _ignored}; the
         * leaf's root documents when the mapping has nested fields (Elasticsearch's nested loader
         * collects a root's children as the documents between the previous root and it, so a child id
         * would silently get a wrong block).
         */
        LeafEntry enterLeaf(LeafReaderContext leaf) throws IOException {
            org.apache.lucene.util.FixedBitSet ignored = null;
            if (coverage != null) {
                coverage.requireCovered(leaf.reader());
                ignored = ReconstructionCoverage.ignoredDocuments(leaf.reader());
            }
            org.apache.lucene.util.BitSet roots = rootFilter == null ? null : producers.producer(rootFilter).getBitSet(leaf);
            return new LeafEntry(rootFilter != null, roots, ignored);
        }

        @Override
        public void close() throws IOException {
            mapperService.close();
        }
    }

    public IsolatedReconstructionWorkerImpl(Path shardDir, String indexName,
                                             String rawMappingJson, Map<String, String> flatSettings) throws IOException {
        this(shardDir, indexName, rawMappingJson, flatSettings, null, null);
    }

    /** {@code tripwire}: of the reader's guard stubs; never tripped when nothing was left out. */
    record OpenedReader(Directory directory, DirectoryReader reader, PrunedSegmentReaders.Tripwire tripwire) {}

    /** The directory property, checked before anything else, so an invalid value fails every open alike. */
    static boolean memoryMapped() {
        return switch (System.getProperty(DIRECTORY_PROPERTY, "mmap")) {
            case "mmap" -> true;
            case "niofs" -> false;
            default -> throw new IllegalArgumentException("-D" + DIRECTORY_PROPERTY + " must be mmap or niofs");
        };
    }

    /**
     * Memory-mapped by default, as Elasticsearch reads these files: synthetic source looks up doc
     * values at random (a terms-dictionary block per keyword value), and through NIOFS every such
     * lookup is a 1 KB pread. Mapping reserves virtual address space of about the shard's size; when
     * that fails (ulimit -v, "Map failed") the reader opens once more on NIOFS, so a segment that is
     * really corrupt fails the same way again and surfaces unchanged. {@code storedFieldWrappers}:
     * the sequential and JNI-zstd stored-field readers reconstruction reads through; a reader that
     * never loads stored fields (vector rehydration) opens without them.
     */
    static OpenedReader openReader(Path shardDir, boolean memoryMapped, boolean storedFieldWrappers) throws IOException {
        return openReader(shardDir, memoryMapped, storedFieldWrappers, PrunedSegmentReaders.Mode.GUARD);
    }

    /** {@code mode}: how the stored-field reader answers structures selective fetch left out (the probe's PROBE). */
    static OpenedReader openReader(Path shardDir, boolean memoryMapped, boolean storedFieldWrappers,
                                   PrunedSegmentReaders.Mode mode) throws IOException {
        if (!memoryMapped) {
            return openReader(new NIOFSDirectory(shardDir), storedFieldWrappers, mode);
        }
        try {
            return openReader(memoryMappedDirectories.open(shardDir), storedFieldWrappers, mode);
        } catch (IOException | OutOfMemoryError mappingFailure) {
            try {
                return openReader(new NIOFSDirectory(shardDir), storedFieldWrappers, mode);
            } catch (IOException | RuntimeException | Error retryFailure) {
                retryFailure.addSuppressed(mappingFailure);
                throw retryFailure;
            }
        }
    }

    /** Takes ownership of {@code directory}: a failed open closes it. */
    static OpenedReader openReader(Directory directory, boolean storedFieldWrappers) throws IOException {
        return openReader(directory, storedFieldWrappers, PrunedSegmentReaders.Mode.GUARD);
    }

    private static OpenedReader openReader(Directory directory, boolean storedFieldWrappers, PrunedSegmentReaders.Mode mode)
            throws IOException {
        try {
            if (!storedFieldWrappers) return new OpenedReader(directory, DirectoryReader.open(directory), new PrunedSegmentReaders.Tripwire());
            // Structures selective fetch left out of the directory are guarded, see PrunedSegmentReaders.
            PrunedSegmentReaders.Opened opened = PrunedSegmentReaders.open(directory, mode);
            return new OpenedReader(directory,
                SequentialStoredFieldsDirectoryReader.wrap(JniZstdStoredFieldsReader.wrap(opened.reader())), opened.tripwire());
        } catch (IOException | RuntimeException | Error e) {
            try { directory.close(); } catch (IOException close) { e.addSuppressed(close); }
            throw e;
        }
    }

    /** Creates the memory-mapped directory; a test seam to simulate a mapping failure. */
    @FunctionalInterface
    interface DirectoryFactory {
        Directory open(Path path) throws IOException;
    }

    static volatile DirectoryFactory memoryMappedDirectories = MMapDirectory::new;

    /** Missing stored source is not evidence that reconstructable synthetic source exists. */
    public static final class SourceNotSyntheticException extends IOException {
        SourceNotSyntheticException() {
            super("stored _source is absent and the mapping does not enable synthetic _source");
        }
    }

    @Override
    public int docCount() {
        return reader.maxDoc();
    }

    /**
     * Builds the segment's source and stored-field loaders once and reuses them while doc ids
     * ascend, as ES's {@code ConcurrentSegmentSourceProvider} does. Doc-values iterators only move
     * forward, so a doc id at or before the previous one, another segment, or any failure rebuilds
     * them. Leaves get {@code null} doc ids: the caller may skip documents (deleted and nested ones
     * leave gaps), and ES's singleton doc-values loaders would desync on any id they were not built
     * for. {@link #readSources} passes explicit ids, built per call.
     */
    @Override
    public synchronized byte[] readSource(int docId) throws IOException {
        // The merge instance only asserts its bounds.
        Objects.checkIndex(docId, reader.maxDoc());
        List<LeafReaderContext> leaves = reader.leaves();
        int leafOrd = ReaderUtil.subIndex(docId, leaves);
        LeafReaderContext leaf = leaves.get(leafOrd);
        int leafDocId = docId - leaf.docBase;
        try {
            LeafEntry entry = enterLeaf(leaf);
            entry.requireRoot(leafDocId);
            if (leafOrd != cursorLeafOrd || leafDocId <= cursorLeafDoc) {
                cursorLeafOrd = -1;
                cursorSource = sourceLoader.leaf(leaf.reader(), null);
                cursorStoredFields = storedFieldLoader.getLoader(leaf, null);
                cursorLeafOrd = leafOrd;
            }
            cursorLeafDoc = leafDocId;
            cursorStoredFields.advanceTo(leafDocId);
            Source source = cursorSource.source(cursorStoredFields, leafDocId);
            byte[] reconstructed = reconstructed(entry, leaf, leafDocId, BytesReference.toBytes(source.internalSourceRef()));
            tripwire.check();
            return reconstructed;
        } catch (IOException | RuntimeException | Error e) {
            cursorLeafOrd = -1;
            throw e;
        }
    }

    @Override
    public synchronized byte[][] readSources(int[] ascendingGlobalDocIds) throws IOException {
        int count = ascendingGlobalDocIds.length;
        if (count == 0) {
            throw new IllegalArgumentException("no doc ids");
        }
        for (int i = 0; i < count; i++) {
            Objects.checkIndex(ascendingGlobalDocIds[i], reader.maxDoc());
            if (i > 0 && ascendingGlobalDocIds[i] <= ascendingGlobalDocIds[i - 1]) {
                throw new IllegalArgumentException("doc ids are not strictly ascending");
            }
        }
        byte[][] sources = new byte[count][];
        long total = 0;
        List<LeafReaderContext> leaves = reader.leaves();
        int next = 0;
        while (next < count) {
            LeafReaderContext leaf = leaves.get(ReaderUtil.subIndex(ascendingGlobalDocIds[next], leaves));
            int leafEnd = leaf.docBase + leaf.reader().maxDoc();
            int end = next;
            while (end < count && ascendingGlobalDocIds[end] < leafEnd) end++;
            // The singleton doc-values loaders fail when a visited doc is not the next of these ids,
            // so the arrays hold exactly the documents visited below, and die with this call.
            int[] idsInLeaf = new int[end - next];
            for (int k = 0; k < idsInLeaf.length; k++) {
                idsInLeaf[k] = ascendingGlobalDocIds[next + k] - leaf.docBase;
            }
            LeafEntry entry = enterLeaf(leaf);
            for (int leafDocId : idsInLeaf) {
                entry.requireRoot(leafDocId);
            }
            SourceLoader.Leaf source = sourceLoader.leaf(leaf.reader(), idsInLeaf);
            LeafStoredFieldLoader storedFields = storedFieldLoader.getLoader(leaf, idsInLeaf);
            for (int leafDocId : idsInLeaf) {
                storedFields.advanceTo(leafDocId);
                byte[] bytes = reconstructed(entry, leaf, leafDocId,
                    BytesReference.toBytes(source.source(storedFields, leafDocId).internalSourceRef()));
                sources[next++] = bytes;
                total += bytes.length;
                if (total >= batchBytes && next < count) {
                    tripwire.check();
                    return Arrays.copyOf(sources, next);
                }
            }
        }
        tripwire.check();
        return sources;
    }

    /** {@link Setup#enterLeaf} once per leaf; a refusal is thrown again on every later entry, never cached. */
    private LeafEntry enterLeaf(LeafReaderContext leaf) throws IOException {
        LeafEntry entry = leafEntries.get(leaf.ord);
        if (entry == null) {
            entry = setup.enterLeaf(leaf);
            leafEntries.put(leaf.ord, entry);
        }
        return entry;
    }

    /** {@code roots} null with {@code nested} set: the leaf holds no root at all. */
    record LeafEntry(boolean nested, org.apache.lucene.util.BitSet roots, org.apache.lucene.util.FixedBitSet ignored) {
        void requireRoot(int leafDocId) {
            if (nested && (roots == null || !roots.get(leafDocId))) {
                throw new IllegalArgumentException("doc id is not a root document");
            }
        }
    }

    /** Elasticsearch's rendering of a reconstructed document, completed and counted in rewrite mode. */
    private byte[] reconstructed(LeafEntry entry, LeafReaderContext leaf, int leafDocId, byte[] esJson) throws IOException {
        ReconstructionCoverage coverage = setup.coverage();
        if (coverage == null) return esJson;
        byte[] completed = coverage.hasConstants() ? coverage.withConstants(esJson) : esJson;
        if (entry.ignored() != null && entry.ignored().get(leafDocId)) {
            ignoredDocs.accept(leaf.docBase + leafDocId);
        }
        return completed;
    }

    /**
     * Whether documents rebuilt here lose values sent under {@code dynamic: false}/{@code runtime}
     * objects without a count, because {@code --source-disabled-accept unmapped-values} let the
     * index through. Called reflectively by the ambient reconstructor.
     */
    public boolean losesUnmappedValues() {
        return setup.losesUnmappedValues();
    }

    @Override
    public void close() throws IOException {
        try (directory; reader; setup) { /* close every owned resource */ }
    }

    /**
     * Builds a standalone {@link MapperService} from raw ES mapping/settings JSON - no cluster,
     * no real ES node. Every argument here was verified empirically against the real 9.3.0 jars,
     * not assumed from ES's (module-path-oriented, harder-to-use-standalone) internal call sites.
     */
    static MapperService buildMapperService(String indexName, String rawMappingJson,
                                                      Map<String, String> flatSettings) throws IOException {
        return buildMapperService(indexName, rawMappingJson, flatSettings, query -> null);
    }

    /**
     * Same as {@link #buildMapperService(String, String, Map)}, with {@code bitsetProducer} as the
     * MapperService's nested-parent bitset producer. The three-argument overload passes
     * {@code query -> null}, which is only safe for mappings without nested fields: ES's nested
     * synthetic-source loader dereferences the producer's result.
     */
    static MapperService buildMapperService(String indexName, String rawMappingJson,
                                                      Map<String, String> flatSettings,
                                                      Function<Query, BitSetProducer> bitsetProducer) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder();
        for (Map.Entry<String, String> entry : flatSettings.entrySet()) {
            settingsBuilder.put(entry.getKey(), entry.getValue());
        }
        Settings settings = settingsBuilder.build();

        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        // List.of(new MapperExtrasPlugin(), new GefrierschrankXPackMapperPlugin()): registers
        // scaled_float/token_count/rank_feature/rank_features/search_as_you_type/match_only_text
        // (mapper-extras) plus aggregate_metric_double/histogram/constant_keyword/wildcard/
        // version/counted_keyword/unsigned_long/shape/geo_shape (x-pack). All fifteen types live in
        // ES's own bundled modules/plugins (not part of org.elasticsearch:elasticsearch core) -
        // without this, merge() below throws "No mapper found for type [...]" for any index using
        // one of them. See org/elasticsearch/index/mapper/extras/package-info.java (mapper-extras,
        // tri-licensed) and org/elasticsearch/xpack/*/package-info.java (x-pack,
        // EL2.0-only - a materially different, more restrictive license, documented distinctly
        // there and in this module's LICENSE) for why these are vendored rather than pulled in as
        // dependencies, and why GefrierschrankXPackMapperPlugin (project-written, not vendored) is
        // the minimal facade used instead of vendoring ES's own real x-pack plugin classes.
        MapperRegistry mapperRegistry = new IndicesModule(
            List.of(new MapperExtrasPlugin(), new GefrierschrankXPackMapperPlugin())
        ).getMapperRegistry();
        SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
        // Never invoked for the field types this reconstructs (numeric/keyword/date/boolean/geo_point
        // doc-values-backed synthetic source). Runtime-script-backed fields would need a real
        // SearchExecutionContext; not exercised or claimed supported here.
        Supplier<SearchExecutionContext> searchExecutionContextSupplier =
            () -> {
                throw new UnsupportedOperationException(
                    "synthetic-source reconstruction needed a real SearchExecutionContext "
                        + "(likely a runtime-script-backed field) - not supported");
            };

        MapperService mapperService = new MapperService(
            (Supplier<TransportVersion>) TransportVersion::current,
            indexSettings,
            reconstructionAnalyzers(),
            XContentParserConfiguration.EMPTY,
            similarityService,
            mapperRegistry,
            searchExecutionContextSupplier,
            ProvidedIdFieldMapper.NO_FIELD_DATA,
            ScriptCompiler.NONE,
            bitsetProducer,
            MapperMetrics.NOOP,
            null);

        try {
            mapperService.merge("_doc", new CompressedXContent(rawMappingJson), MapperService.MergeReason.MAPPING_UPDATE);
            return mapperService;
        } catch (IOException | RuntimeException | Error e) {
            try { mapperService.close(); } catch (IOException close) { e.addSuppressed(close); }
            throw e;
        }
    }

    /**
     * Analyzed field types (text, match_only_text, search_as_you_type) construct their mapper
     * through {@code TextParams.Analyzers}, which requireNonNull's the index/search analyzers -
     * passing no analyzers at all would fail the whole mapping with a message-less NPE, even though
     * reconstruction never analyzes anything: it reads values back out of doc values and stored
     * fields. Standard analyzers satisfy that constructor; the analysis chain's actual behavior is
     * irrelevant to reading. Explicit custom analyzer names are still validated by Elasticsearch
     * and may require additional analysis support.
     */
    static IndexAnalyzers reconstructionAnalyzers() {
        NamedAnalyzer standard = new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer());
        NamedAnalyzer keyword = new NamedAnalyzer("keyword", AnalyzerScope.INDEX, new KeywordAnalyzer());
        return IndexAnalyzers.of(Map.of("default", standard, "keyword", keyword));
    }
}
