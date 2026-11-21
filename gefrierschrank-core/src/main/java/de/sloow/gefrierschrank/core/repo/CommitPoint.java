package de.sloow.gefrierschrank.core.repo;

import java.util.List;
import java.util.Set;

/**
 * What a shard's latest Lucene commit says about its segments, read from the commit files alone
 * ({@code segments_N}, {@code .si}, {@code .fnm}, compound {@code .cfs/.cfe}) before anything else
 * is downloaded. Lucene-free, so the fetch plan and the preflight live outside the reader modules.
 */
public record CommitPoint(String segmentsFile, List<Segment> segments) {

    /** {@link Segment#liveDocs()} when the commit cannot tell. */
    public static final long UNKNOWN = -1;

    public CommitPoint {
        segments = List.copyOf(segments);
    }

    /**
     * One segment of the commit. {@code softDeletesFlagged}: FieldInfos name a soft-deletes field,
     * so {@code softDelCount} is maintained; {@code softDeletesField}: the segment has a
     * {@code __soft_deletes} field at all. {@code files}: every file the segment needs, its
     * field-infos, doc-values-update and live-docs generations included.
     */
    public record Segment(String name, boolean compound, int maxDoc, int delCount, int softDelCount,
                          boolean softDeletesFlagged, boolean softDeletesField, boolean hasSource,
                          boolean hasPrimaryTerm, boolean hasNestedPath, boolean hasLegacyType, Set<String> files) {

        public Segment {
            files = Set.copyOf(files);
        }

        /**
         * Documents neither hard- nor soft-deleted, nested children included, or {@link #UNKNOWN}
         * when the segment has soft deletes that Lucene did not count. A document deleted both
         * ways is counted once: Lucene's soft-delete count excludes hard-deleted documents.
         */
        public long liveDocs() {
            if (softDeletesFlagged) return (long) maxDoc - delCount - softDelCount;
            return softDeletesField ? UNKNOWN : (long) maxDoc - delCount;
        }

        /** Without {@code _primary_term}, roots are told from nested children through postings. */
        public boolean needsLegacyNestedMarkers() {
            return !hasPrimaryTerm && (hasNestedPath || hasLegacyType);
        }
    }
}
