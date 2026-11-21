package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Aggregate result of a {@code --all-indices} run: one entry per index attempted, plus counts by
 * outcome category. Written incrementally (after every index, not just once at the end) for the
 * same reason {@link JsonReportWriter} writes its per-index report after every shard - a
 * long run that's hard-killed partway through should still leave a manifest showing which
 * indices finished before the kill.
 *
 * <p>This lives in {@code gefrierschrank-cli}, not {@code gefrierschrank-delta}: it summarizes
 * multiple {@link de.sloow.gefrierschrank.delta.RunReport} runs and their report files, which is
 * an orchestration concern of the CLI command, not something the per-index pipeline needs to
 * know about.
 *
 * <p>{@code behindLatestCount} counts indices whose table is as of an older retained snapshot
 * than the index's latest one (a resumed export completes the snapshot it started from). It does
 * not affect {@code complete}: those tables are complete for their snapshot.
 *
 * <p>{@code aliasCount} (only with {@code --mounted-shells skip-if-original}, absent otherwise)
 * counts {@code ALIAS_OF} entries: mounted shells written as no table because their original's
 * table holds exactly the same data. The new keys are left out when null so that a default run's
 * manifest keeps the 1.1.0 bytes.
 */
record AllIndicesManifest(
    int totalIndices,
    int completedCount,
    int completedWithQuarantineCount,
    int failedCount,
    List<IndexEntry> indices,
    List<String> pendingIndices,
    boolean complete,
    int behindLatestCount,
    @JsonInclude(JsonInclude.Include.NON_NULL) Integer aliasCount
) {

    AllIndicesManifest(int totalIndices, int completedCount, int completedWithQuarantineCount, int failedCount,
                       List<IndexEntry> indices, List<String> pendingIndices, boolean complete) {
        this(totalIndices, completedCount, completedWithQuarantineCount, failedCount, indices, pendingIndices,
            complete, 0);
    }

    AllIndicesManifest(int totalIndices, int completedCount, int completedWithQuarantineCount, int failedCount,
                       List<IndexEntry> indices, List<String> pendingIndices, boolean complete, int behindLatestCount) {
        this(totalIndices, completedCount, completedWithQuarantineCount, failedCount, indices, pendingIndices,
            complete, behindLatestCount, null);
    }

    /** {@code ALIAS_OF}: no table of its own, see {@link IndexEntry#aliasOf()}; neutral for the exit code. */
    enum Outcome { COMPLETED, COMPLETED_WITH_QUARANTINE, FAILED, ALIAS_OF }

    /**
     * {@code reportFile} is the per-index report's file name (relative to the manifest's own
     * directory), or {@code null} when either no {@code --report} directory was requested, or the
     * index is {@code FAILED}: a failed-outright index produced no usable report, so there is
     * nothing trustworthy to point to even if a partial file happens to exist on disk.
     * {@code snapshotUuid} and {@code newerSnapshotAvailable} name the snapshot the table is as of;
     * both are {@code null} for a {@code FAILED} index.
     * {@code aliasOf}, only on an {@code ALIAS_OF} entry, names the original index whose table
     * {@code <root>/<aliasOf>} holds this name's data; such an entry has no report file and no
     * snapshot fields of its own (the original's entry carries them).
     */
    record IndexEntry(String indexName, Outcome outcome, String reportFile, String errorClass, String errorMessage,
                      String snapshotUuid, Boolean newerSnapshotAvailable,
                      @JsonInclude(JsonInclude.Include.NON_NULL) String aliasOf) {
        IndexEntry(String indexName, Outcome outcome, String reportFile, String errorClass, String errorMessage) {
            this(indexName, outcome, reportFile, errorClass, errorMessage, null, null);
        }

        IndexEntry(String indexName, Outcome outcome, String reportFile, String errorClass, String errorMessage,
                   String snapshotUuid, Boolean newerSnapshotAvailable) {
            this(indexName, outcome, reportFile, errorClass, errorMessage, snapshotUuid, newerSnapshotAvailable, null);
        }

        static IndexEntry alias(String indexName, String original) {
            return new IndexEntry(indexName, Outcome.ALIAS_OF, null, null, null, null, null, original);
        }
    }
}
