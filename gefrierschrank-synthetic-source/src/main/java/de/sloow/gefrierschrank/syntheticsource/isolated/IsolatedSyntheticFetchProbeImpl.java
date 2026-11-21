package de.sloow.gefrierschrank.syntheticsource.isolated;

import de.sloow.gefrierschrank.core.repo.SelectiveFetch.Structure;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;

/**
 * Isolated side of the selective-fetch probe ({@code SyntheticFetchProbe}): which index structures
 * Elasticsearch's own reconstruction reads on a shard. The shard is opened with every structure
 * selective fetch left out answered by recording PROBE stubs, the reconstruction is set up exactly
 * as the worker sets it up ({@link IsolatedReconstructionWorkerImpl.Setup}), every leaf with live
 * documents is entered as the worker enters it (coverage check, {@code _ignored}, roots), and a
 * sample of its live root documents is reconstructed through both of the worker's loader paths.
 * Only structure names and a status cross the boundary, never a field name or a value.
 */
public final class IsolatedSyntheticFetchProbeImpl {

    /** Live roots reconstructed from each end of a leaf. */
    static final int SAMPLE_PER_END = 64;

    public static final String OK = "ok";
    public static final String FAILED = "failed";

    private IsolatedSyntheticFetchProbeImpl() {}

    /**
     * Returns {@link #OK} or {@link #FAILED}, followed by the names of the structures the stubs were
     * asked for. A failure that comes with touched structures may be the stubs' empty answers at
     * work (a check that reads "no norms" as "no value"); only a failure without them is conclusive.
     */
    public static List<String> probe(Path shardDir, String indexName, String mappingJson, Map<String, String> flatSettings,
                                     String rewriteMappingJson) throws IOException {
        boolean failed = false;
        PrunedSegmentReaders.Tripwire tripwire = null;
        try (IsolatedReconstructionWorkerImpl.Setup setup =
                 IsolatedReconstructionWorkerImpl.Setup.build(indexName, mappingJson, flatSettings, rewriteMappingJson)) {
            IsolatedReconstructionWorkerImpl.OpenedReader opened = IsolatedReconstructionWorkerImpl.openReader(shardDir,
                IsolatedReconstructionWorkerImpl.memoryMapped(), true, PrunedSegmentReaders.Mode.PROBE);
            tripwire = opened.tripwire();
            try (var directory = opened.directory(); DirectoryReader reader = opened.reader()) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    int[] sample = sample(leaf.reader());
                    if (sample.length == 0) continue;
                    IsolatedReconstructionWorkerImpl.LeafEntry entry = setup.enterLeaf(leaf);
                    int[] roots = roots(entry, sample);
                    if (roots.length == 0) continue;
                    reconstruct(setup, leaf, roots, null);
                    reconstruct(setup, leaf, roots, roots);
                }
            }
        } catch (Exception | AssertionError e) {
            // Never passed on: the export's normal path surfaces the real failure, if there is one.
            failed = true;
        }
        List<String> result = new ArrayList<>();
        result.add(failed ? FAILED : OK);
        if (tripwire != null) for (Structure structure : new TreeSet<>(tripwire.touched())) result.add(structure.name());
        return result;
    }

    /**
     * The worker's two loader paths over {@code docs}: {@code ids} null builds the leaf loaders the
     * way {@code readSource} does, explicit ids the way {@code readSources} does. Results are discarded.
     */
    static void reconstruct(IsolatedReconstructionWorkerImpl.Setup setup, LeafReaderContext leaf, int[] docs, int[] ids)
            throws IOException {
        SourceLoader.Leaf source = setup.sourceLoader().leaf(leaf.reader(), ids);
        LeafStoredFieldLoader storedFields = setup.storedFieldLoader().getLoader(leaf, ids);
        for (int doc : docs) {
            storedFields.advanceTo(doc);
            source.source(storedFields, doc).internalSourceRef();
        }
    }

    /**
     * The first and last {@value #SAMPLE_PER_END} of {@link #liveRoots}, ascending, in one forward
     * pass that keeps only the sample: a force-merged segment of hundreds of millions of documents
     * must not cost an array of all its roots on every probe call.
     */
    static int[] sample(LeafReader leaf) throws IOException {
        int[] head = new int[SAMPLE_PER_END];
        int[] tail = new int[SAMPLE_PER_END];
        long[] seen = new long[1];
        forEachLiveRoot(leaf, doc -> {
            long n = seen[0]++;
            if (n < SAMPLE_PER_END) head[(int) n] = doc;
            else tail[(int) ((n - SAMPLE_PER_END) % SAMPLE_PER_END)] = doc;
        });
        long count = seen[0];
        if (count <= SAMPLE_PER_END) return Arrays.copyOf(head, (int) count);
        long after = count - SAMPLE_PER_END;
        int kept = (int) Math.min(after, SAMPLE_PER_END);
        int oldest = after > SAMPLE_PER_END ? (int) (after % SAMPLE_PER_END) : 0;
        int[] sample = Arrays.copyOf(head, SAMPLE_PER_END + kept);
        for (int i = 0; i < kept; i++) sample[SAMPLE_PER_END + i] = tail[(oldest + i) % SAMPLE_PER_END];
        return sample;
    }

    /**
     * The live documents of {@code leaf} that hold a {@code _primary_term} (every root of an index
     * created by 6.1+; nested children and tombstones have none), or every live document when the
     * leaf has no such field: the documents the export reads. Hard and soft deletes excluded. Ascending.
     */
    static int[] liveRoots(LeafReader leaf) throws IOException {
        IntStream.Builder roots = IntStream.builder();
        forEachLiveRoot(leaf, roots::add);
        return roots.build().toArray();
    }

    /** Calls {@code consumer} with each of {@link #liveRoots}, ascending. */
    private static void forEachLiveRoot(LeafReader leaf, IntConsumer consumer) throws IOException {
        Bits live = leaf.getLiveDocs();
        NumericDocValues softDeletes = leaf.getNumericDocValues(Lucene.SOFT_DELETES_FIELD);
        DocIdSetIterator candidates;
        if (leaf.getFieldInfos().fieldInfo(SeqNoFieldMapper.PRIMARY_TERM_NAME) == null) {
            candidates = DocIdSetIterator.all(leaf.maxDoc());
        } else {
            NumericDocValues primaryTerms = leaf.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
            candidates = primaryTerms != null ? primaryTerms : DocIdSetIterator.empty();
        }
        int softDeleted = -1;
        for (int doc = candidates.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = candidates.nextDoc()) {
            if (live != null && !live.get(doc)) continue;
            if (softDeletes != null && softDeleted < doc) softDeleted = softDeletes.advance(doc);
            if (softDeleted == doc) continue;
            consumer.accept(doc);
        }
    }

    /** The documents of {@code sample} the worker accepts as roots (all of them without nested fields). */
    static int[] roots(IsolatedReconstructionWorkerImpl.LeafEntry entry, int[] sample) {
        if (!entry.nested()) return sample;
        return java.util.Arrays.stream(sample).filter(doc -> entry.roots() != null && entry.roots().get(doc)).toArray();
    }
}
