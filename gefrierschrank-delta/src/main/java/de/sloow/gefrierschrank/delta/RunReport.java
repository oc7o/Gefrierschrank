package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Structured, content-free summary of a {@link DumpToDeltaPipeline#run} call: shard
 * counts by outcome, rows/bytes moved, and per-shard timing.
 *
 * <p>Timestamps are ISO-8601 strings (not {@code java.time.Instant}) and durations are plain
 * millis, so this record serializes with a bare Jackson {@code ObjectMapper} - no
 * {@code jackson-datatype-jsr310} module needed.
 *
 * <p>{@code shards} only ever holds shard identity, counts, byte sizes, timings, and a sanitized
 * exception class/stack locations for quarantined shards - see
 * {@link de.sloow.gefrierschrank.core.FailureDiagnostics} for what that last field leaves out and
 * why. Document {@code _source} content must never reach this type.
 */
public record RunReport(
    String indexName,
    String startedAt,
    String asOf,
    long elapsedMillis,
    int totalShards,
    int committedCount,
    int skippedCount,
    int quarantinedCount,
    long rowsWrittenThisRun,
    long bytesProcessedThisRun,
    List<ShardReport> shards,
    SnapshotInfo snapshot
) {

    /** Reports built without snapshot selection context, e.g. by the pipeline itself. */
    public RunReport(String indexName, String startedAt, String asOf, long elapsedMillis, int totalShards,
                     int committedCount, int skippedCount, int quarantinedCount, long rowsWrittenThisRun,
                     long bytesProcessedThisRun, List<ShardReport> shards) {
        this(indexName, startedAt, asOf, elapsedMillis, totalShards, committedCount, skippedCount,
            quarantinedCount, rowsWrittenThisRun, bytesProcessedThisRun, shards, null);
    }

    public RunReport withSnapshot(SnapshotInfo info) {
        return new RunReport(indexName, startedAt, asOf, elapsedMillis, totalShards, committedCount, skippedCount,
            quarantinedCount, rowsWrittenThisRun, bytesProcessedThisRun, shards, info);
    }

    /**
     * Which snapshot this table is being completed from. {@code newerSnapshotAvailable} means the
     * table is as of {@code snapshotUuid}, not the index's latest retained snapshot: a resumed
     * export finishes the snapshot it started from (see {@link SnapshotSelection}).
     */
    public record SnapshotInfo(
        String selection,
        String snapshotUuid,
        String snapshotName,
        String latestSnapshotUuid,
        boolean newerSnapshotAvailable
    ) {}

    public enum Outcome { COMMITTED, SKIPPED_ALREADY_COMMITTED, SKIPPED_NO_DATA, QUARANTINED }

    /**
     * One shard's outcome. {@code bytesFetched} and {@code fetch} say what the shard downloaded in
     * this run: {@code fetch} is {@code full}, {@code selective} (the selective plan),
     * {@code completed} (selective, then completed in place), {@code preflight} (quarantined by the
     * commit-point preflight after its commit files) or {@code none} (no download in this run).
     * {@code convertedSources}, {@code ignoredFieldDocuments} and
     * {@code unrecoverableUnmappedValues} describe a committed shard and are absent unless set.
     * The last five components are absent from the JSON when null.
     */
    public record ShardReport(
        int shardId,
        String snapshotName,
        Outcome outcome,
        long rowsWritten,
        long bytesProcessed,
        String finishedAt,
        long elapsedMillis,
        String errorClass,
        String errorMessage,
        @JsonInclude(JsonInclude.Include.NON_NULL) Long bytesFetched,
        @JsonInclude(JsonInclude.Include.NON_NULL) String fetch,
        @JsonInclude(JsonInclude.Include.NON_NULL) Long convertedSources,
        @JsonInclude(JsonInclude.Include.NON_NULL) Long ignoredFieldDocuments,
        @JsonInclude(JsonInclude.Include.NON_NULL) Boolean unrecoverableUnmappedValues
    ) {
        @JsonCreator
        public ShardReport {}

        /** Without the fetch and count fields, which then stay absent from the JSON. */
        public ShardReport(int shardId, String snapshotName, Outcome outcome, long rowsWritten, long bytesProcessed,
                           String finishedAt, long elapsedMillis, String errorClass, String errorMessage) {
            this(shardId, snapshotName, outcome, rowsWritten, bytesProcessed, finishedAt, elapsedMillis, errorClass,
                errorMessage, null, null, null, null, null);
        }
    }
}
