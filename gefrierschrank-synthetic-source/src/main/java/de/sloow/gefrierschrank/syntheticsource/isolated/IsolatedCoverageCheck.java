package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.index.mapper.MapperService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/**
 * Isolated entry of the commit-point preflight ({@code SyntheticCoverage}): the worker's coverage
 * plan, built from the same mapping through the same builder, and its refusals evaluated on the
 * FieldInfos of a shard's commit (segments_N, .si, .fnm or the compound pair) instead of opened
 * segments. JDK types only across the boundary; a plan is an opaque handle the ambient side
 * memoizes per index metadata. Thread-safe.
 */
public final class IsolatedCoverageCheck {

    // Null for a synthetic mapping: nothing beyond the MapperService build can refuse.
    private final ReconstructionCoverage coverage;

    private IsolatedCoverageCheck(ReconstructionCoverage coverage) {
        this.coverage = coverage;
    }

    /**
     * Throws what the worker's open throws before it reads a file, except the mapping-level
     * refusals ({@link #requireMapping} checks those, so a memoized plan honours the opt-in of
     * every call).
     */
    public static Object plan(String indexName, String mappingJson, Map<String, String> flatSettings, String rewriteMappingJson)
            throws IOException {
        IsolatedEsBootstrap.ensure();
        BitSetProducerCache producers = new BitSetProducerCache();
        MapperService original = IsolatedReconstructionWorkerImpl.buildMapperService(indexName, mappingJson, flatSettings,
            producers::producer);
        try {
            return new IsolatedCoverageCheck(ReconstructionCoverage.openUnchecked(indexName, original, rewriteMappingJson,
                flatSettings, producers::producer));
        } finally {
            original.close();
        }
    }

    public static void requireMapping(Object plan, boolean acceptUnmappedValues) throws IOException {
        ReconstructionCoverage coverage = ((IsolatedCoverageCheck) plan).coverage;
        if (coverage != null) coverage.requireMappingCovered(acceptUnmappedValues);
    }

    /** Whether reconstruction through this plan loses values under {@code dynamic: false}/{@code runtime} objects. */
    public static boolean losesUnmappedValues(Object plan) {
        ReconstructionCoverage coverage = ((IsolatedCoverageCheck) plan).coverage;
        return coverage != null && coverage.losesUnmappedValues();
    }

    /**
     * The per-segment refusals of the named segments, in commit order: the first refusing segment
     * throws, as the worker's first entry into that leaf would.
     */
    public static void requireSegments(Object plan, Path shardDir, Set<String> segments) throws IOException {
        ReconstructionCoverage coverage = ((IsolatedCoverageCheck) plan).coverage;
        if (coverage == null || segments.isEmpty()) return;
        try (Directory directory = new NIOFSDirectory(shardDir)) {
            SegmentInfos commit = SegmentInfos.readLatestCommit(directory);
            for (SegmentCommitInfo segment : commit) {
                if (!segments.contains(segment.info.name)) continue;
                LeafMetaData metaData = new LeafMetaData(commit.getIndexCreatedVersionMajor(), segment.info.getMinVersion(),
                    segment.info.getIndexSort(), segment.info.getHasBlocks());
                coverage.requireCovered(new FieldInfosLeafReader(fieldInfos(segment), segment.info.maxDoc(), metaData));
            }
        }
    }

    /** A segment's current FieldInfos, read as Lucene's IndexWriter reads them (field-infos generation, compound file). */
    static FieldInfos fieldInfos(SegmentCommitInfo segment) throws IOException {
        Codec codec = segment.info.getCodec();
        if (segment.hasFieldUpdates()) {
            return codec.fieldInfosFormat().read(segment.info.dir, segment.info,
                Long.toString(segment.getFieldInfosGen(), Character.MAX_RADIX), IOContext.READONCE);
        }
        if (segment.info.getUseCompoundFile()) {
            Directory compound = codec.compoundFormat().getCompoundReader(segment.info.dir, segment.info);
            try (Closeable ignored = compound) {
                return codec.fieldInfosFormat().read(compound, segment.info, "", IOContext.READONCE);
            }
        }
        return codec.fieldInfosFormat().read(segment.info.dir, segment.info, "", IOContext.READONCE);
    }
}
