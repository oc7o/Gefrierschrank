package de.sloow.gefrierschrank.syntheticsource.isolated;

import de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker;
import de.sloow.gefrierschrank.syntheticsource.IsolatedSourceVectorsSession;
import de.sloow.gefrierschrank.syntheticsource.SourceVectorsMismatchException;
import de.sloow.gefrierschrank.syntheticsource.VectorRehydrationException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * One shard's vector rehydration: Elasticsearch's own {@code SyntheticVectorsLoader}, built from the
 * index's pruned mapping, over a plain {@link DirectoryReader} of the materialized shard. Patches and
 * their JSON text come from Elasticsearch ({@code SourceLoader.applySyntheticVectors},
 * {@code Source.fromMap}), exactly as its fetch phase computes them. Isolated for the same reason as
 * {@link IsolatedReconstructionWorkerImpl}; only JDK types cross the boundary.
 *
 * <p>The mapping, loader and reader are shared read-only by every cursor; per-segment field loaders,
 * vector values, doc values and scorers are built per cursor, on its thread, as Elasticsearch builds
 * them per search thread from one reader.
 */
public final class IsolatedSourceVectorsSessionImpl implements IsolatedSourceVectorsSession {

    private final MapperService mapperService;
    private final SourceLoader.SyntheticVectorsLoader loader;
    private final Directory directory;
    private final DirectoryReader reader;

    public IsolatedSourceVectorsSessionImpl(Path shardDir, String indexName, String prunedMappingJson,
                                            Map<String, String> flatSettings, Set<String> expectedFields) throws IOException {
        boolean memoryMapped = memoryMapped();
        IsolatedEsBootstrap.ensure();
        MapperService opened = null;
        try {
            opened = IsolatedReconstructionWorkerImpl.buildMapperService(indexName, prunedMappingJson, flatSettings,
                QueryBitSetProducer::new);
            Settings.Builder settings = Settings.builder();
            flatSettings.forEach(settings::put);
            if (!IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.get(settings.build())
                || !new TreeSet<>(opened.mappingLookup().syntheticVectorFields()).equals(new TreeSet<>(expectedFields))) {
                throw new SourceVectorsMismatchException();
            }
            SourceLoader.SyntheticVectorsLoader vectors = opened.mappingLookup().getMapping().syntheticVectorsLoader(null);
            if (vectors == null) throw new SourceVectorsMismatchException();
            IsolatedReconstructionWorkerImpl.OpenedReader files =
                IsolatedReconstructionWorkerImpl.openReader(shardDir, memoryMapped, false);
            this.mapperService = opened;
            this.loader = vectors;
            this.directory = files.directory();
            this.reader = files.reader();
        } catch (SourceVectorsMismatchException e) {
            closeAfterFailure(opened, e);
            throw e;
        } catch (IOException | RuntimeException e) {
            VectorRehydrationException failure = new VectorRehydrationException(e);
            closeAfterFailure(opened, failure);
            throw failure;
        } catch (Error e) {
            closeAfterFailure(opened, e);
            throw e;
        }
    }

    private static void closeAfterFailure(MapperService opened, Throwable failure) {
        if (opened != null) try { opened.close(); } catch (IOException close) { failure.addSuppressed(new VectorRehydrationException(close)); }
    }

    /** Same property and values as the reconstruction worker's directory choice. */
    private static boolean memoryMapped() {
        return switch (System.getProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY, "mmap")) {
            case "mmap" -> true;
            case "niofs" -> false;
            default -> throw new IllegalArgumentException("-D" + IsolatedReconstructionWorker.DIRECTORY_PROPERTY
                + " must be mmap or niofs");
        };
    }

    @Override
    public int docCount() {
        return reader.maxDoc();
    }

    @Override
    public Cursor newCursor() {
        return new VectorCursor();
    }

    @Override
    public void close() throws IOException {
        try (directory; reader; mapperService) { /* close every owned resource */ }
    }

    /**
     * Elasticsearch emits the patches of one object in the iteration order of its mapper's
     * {@code Map.copyOf} children, which is randomized per JVM, so the order of several vector
     * members differs between Elasticsearch nodes and between runs of this process. Ordered by path
     * at every level (nested offsets keep document order), a row is the same in every run. Nested
     * patches without any vector below them (a root whose nested objects hold none) are dropped:
     * they change nothing, and the document keeps its stored bytes.
     */
    static List<SourceLoader.SyntheticVectorPatch> ordered(List<SourceLoader.SyntheticVectorPatch> patches) {
        List<SourceLoader.SyntheticVectorPatch> ordered = new ArrayList<>(patches.size());
        for (SourceLoader.SyntheticVectorPatch patch : patches) {
            if (patch instanceof SourceLoader.NestedSyntheticVectorPath nested) {
                List<SourceLoader.SyntheticVectorPatch> children = ordered(nested.children());
                if (!children.isEmpty()) ordered.add(new SourceLoader.NestedSyntheticVectorPath(nested.fullPath(), children));
            } else if (patch instanceof SourceLoader.NestedOffsetSyntheticVectorPath offset) {
                List<SourceLoader.SyntheticVectorPatch> children = ordered(offset.children());
                if (!children.isEmpty()) ordered.add(new SourceLoader.NestedOffsetSyntheticVectorPath(offset.offset(), children));
            } else {
                ordered.add(patch);
            }
        }
        ordered.sort(java.util.Comparator.comparing(IsolatedSourceVectorsSessionImpl::orderKey));
        return ordered;
    }

    private static String orderKey(SourceLoader.SyntheticVectorPatch patch) {
        if (patch instanceof SourceLoader.LeafSyntheticVectorPath leaf) return leaf.fullPath();
        if (patch instanceof SourceLoader.NestedSyntheticVectorPath nested) return nested.fullPath();
        return "";
    }

    /** Keeps its segment's loaders while root ids ascend; anything else, or any failure, rebuilds them. */
    private final class VectorCursor implements Cursor {
        private final List<SourceLoader.SyntheticVectorPatch> patches = new ArrayList<>();
        private int leafOrd = -1;
        private int leafDoc = -1;
        private SourceLoader.SyntheticVectorsLoader.Leaf leaf;

        @Override
        public int load(int globalDocId) throws IOException {
            try {
                Objects.checkIndex(globalDocId, reader.maxDoc());
                List<LeafReaderContext> leaves = reader.leaves();
                int ord = ReaderUtil.subIndex(globalDocId, leaves);
                LeafReaderContext context = leaves.get(ord);
                int doc = globalDocId - context.docBase;
                patches.clear();
                if (ord != leafOrd || doc <= leafDoc) {
                    leafOrd = -1;
                    leaf = loader.leaf(context);
                    leafOrd = ord;
                }
                leafDoc = doc;
                if (leaf != null) leaf.load(doc, patches);
                if (patches.size() > 1 || !rootLeavesOnly()) {
                    List<SourceLoader.SyntheticVectorPatch> ordered = ordered(patches);
                    patches.clear();
                    patches.addAll(ordered);
                }
                return patches.size();
            } catch (IOException | RuntimeException e) {
                leafOrd = -1;
                patches.clear();
                throw new VectorRehydrationException(e);
            }
        }

        @Override
        public boolean rootLeavesOnly() {
            for (SourceLoader.SyntheticVectorPatch patch : patches) {
                if (!(patch instanceof SourceLoader.LeafSyntheticVectorPath leafPatch) || leafPatch.fullPath().indexOf('.') >= 0) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String[] rootKeys() {
            return rootMap().keySet().toArray(String[]::new);
        }

        @Override
        public byte[] rootMembers() throws IOException {
            try {
                byte[] object = BytesReference.toBytes(Source.fromMap(rootMap(), XContentType.JSON).internalSourceRef());
                if (object.length < 2 || object[0] != '{' || object[object.length - 1] != '}') {
                    throw new IllegalStateException("not a JSON object");
                }
                return Arrays.copyOfRange(object, 1, object.length - 1);
            } catch (RuntimeException e) {
                throw new VectorRehydrationException(e);
            }
        }

        private Map<String, Object> rootMap() {
            Map<String, Object> members = new LinkedHashMap<>();
            for (SourceLoader.SyntheticVectorPatch patch : patches) {
                var leafPatch = (SourceLoader.LeafSyntheticVectorPath) patch;
                members.put(leafPatch.fullPath(), leafPatch.value());
            }
            return members;
        }

        @Override
        public byte[] reserialize(byte[] stored, int length) throws IOException {
            try {
                Source patched = SourceLoader.applySyntheticVectors(Source.fromBytes(new BytesArray(stored, 0, length)), patches);
                if (patched.sourceContentType() == XContentType.JSON) {
                    return BytesReference.toBytes(patched.internalSourceRef());
                }
                return BytesReference.toBytes(Source.fromMap(patched.source(), XContentType.JSON).internalSourceRef());
            } catch (RuntimeException e) {
                throw new VectorRehydrationException(e);
            }
        }
    }
}
