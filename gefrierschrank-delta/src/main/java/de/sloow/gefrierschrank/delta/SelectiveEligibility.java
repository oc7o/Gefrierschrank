package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.CommitPoint;
import de.sloow.gefrierschrank.syntheticsource.SourceVectors;

/**
 * Whether an index may take the name-based selective plan, which serves documents from their
 * stored {@code _source} only. Eligible: metadata present, no synthetic-source signal, every
 * vector kept in the stored source (rehydrated or refused vectors are read from the vector
 * files), and {@code _source} retained (an index whose {@code _source} was disabled after creation
 * can mix stored and source-less documents in one segment, which looks like a stored segment but
 * sends documents to reconstruction). Per shard, {@link de.sloow.gefrierschrank.core.repo.SelectiveFetch#plan}
 * adds the segment-level conditions. The vector verdict is the export's own
 * {@link SourceVectorsPlan#forIndex} evaluation, so a vector index runs the isolated probe once per run.
 */
final class SelectiveEligibility {

    private SelectiveEligibility() {}

    static boolean of(String indexName, JsonNode metadata) {
        if (metadata == null || !metadata.isObject()) return false;
        if (ExportPreflight.syntheticSourceSignal(metadata) != null) return false;
        if (ExportPreflight.SourceRetention.of(metadata, -1) != ExportPreflight.SourceRetention.RETAINED) return false;
        return !vectorsFromVectorFiles(indexName, metadata);
    }

    /**
     * Whether Elasticsearch's loaders reconstruct live documents of this shard, which makes it a
     * candidate for the synthetic-source plan: the index has the synthetic-source signal, or its
     * {@code _source} is not retained (then any segment may mix stored and source-less documents,
     * which FieldInfos cannot tell from an all-stored segment), or a segment that may hold live
     * documents has no stored {@code _source}.
     */
    static boolean reconstructs(JsonNode metadata, CommitPoint commit) {
        if (metadata == null || !metadata.isObject()) return false;
        if (ExportPreflight.syntheticSourceSignal(metadata) != null) return true;
        if (ExportPreflight.SourceRetention.of(metadata, -1) != ExportPreflight.SourceRetention.RETAINED) return true;
        for (CommitPoint.Segment segment : commit.segments()) {
            if (!segment.hasSource() && segment.liveDocs() != 0) return true;
        }
        return false;
    }

    /** Rehydrated or refused vectors are read from the vector files (rule F1 of the synthetic plan). */
    static boolean vectorsFromVectorFiles(String indexName, JsonNode metadata) {
        return SourceVectorsPlan.forIndex(indexName, metadata).verdict() != SourceVectors.Verdict.NONE;
    }
}
