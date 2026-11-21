package de.sloow.gefrierschrank.delta;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Per-run knobs of {@link FullIndexExporter}: which snapshot to export, how many shards of an
 * index run concurrently, and how much temp disk those concurrent shards may occupy.
 *
 * @param tempBudgetMiB temp-disk budget for concurrently materialized shards in MiB; 0 means
 *                      80% of the usable space of {@code java.io.tmpdir}. Unused at parallelism 1,
 *                      where exactly one shard is on disk at a time, as before parallelism existed.
 */
public record ExportSettings(SnapshotSelection snapshotSelection, int parallelism, long tempBudgetMiB) {

    public static final int MAX_PARALLELISM = 256;
    public static final ExportSettings DEFAULT = new ExportSettings(SnapshotSelection.DEFAULT, 1, 0);

    private static final long MIB = 1024L * 1024L;

    public ExportSettings {
        Objects.requireNonNull(snapshotSelection, "snapshotSelection");
        if (parallelism < 1 || parallelism > MAX_PARALLELISM)
            throw new IllegalArgumentException("parallelism must be between 1 and " + MAX_PARALLELISM);
        if (tempBudgetMiB < 0 || tempBudgetMiB > Integer.MAX_VALUE)
            throw new IllegalArgumentException("temp budget must be 0 (the default) or between 1 and "
                + Integer.MAX_VALUE + " MiB");
    }

    /**
     * Configurations likely to fail at runtime rather than at parse time: every in-flight shard
     * can hold a large document plus its Parquet row group in heap, every range of a shard read in
     * several ranges (the JVM's {@code gefrierschrank.shard.threads}) one more row group, a
     * synthetic-source range also its reconstruction batch, and one more document that it reads
     * before the document memory gate knows its size, and every one of
     * a shard's download streams needs an S3 connection from the source client's shared pool.
     */
    List<String> resourceWarnings(long maxHeapBytes, int downloadConcurrency, int s3MaxConnections) {
        return resourceWarnings(maxHeapBytes, downloadConcurrency, s3MaxConnections, true);
    }

    /** {@link #resourceWarnings(long, int, int)}; the upload-buffer warning only for an {@code s3a://} target. */
    List<String> resourceWarnings(long maxHeapBytes, int downloadConcurrency, int s3MaxConnections, boolean s3aTarget) {
        var warnings = new ArrayList<String>();
        int shardThreads = requestedShardThreads();
        if (parallelism == 1 && shardThreads == 1) return warnings;
        if (parallelism > 1 && maxHeapBytes != Long.MAX_VALUE && (long) parallelism * 1024 * MIB > maxHeapBytes)
            warnings.add("--parallelism " + parallelism + " x 1 GiB exceeds the maximum heap of "
                + maxHeapBytes / MIB + " MiB; raise -Xmx or lower --parallelism");
        if (shardThreads > 1) {
            int bound = ShardRanges.heapBound(maxHeapBytes, parallelism);
            long syntheticRangeBytes = ShardRanges.PER_RANGE_HEAP_BYTES + ShardRanges.syntheticReadAheadBytes();
            int syntheticBound = ShardRanges.heapBound(maxHeapBytes, parallelism, ShardRanges.syntheticReadAheadBytes());
            String synthetic = syntheticBound < bound ? " (" + syntheticBound + " for synthetic-source indices, whose ranges"
                + " also hold a reconstruction batch)" : "";
            if (bound < shardThreads)
                warnings.add("--parallelism " + parallelism + " x --shard-threads " + shardThreads + " x "
                    + ShardRanges.PER_RANGE_HEAP_BYTES / MIB + " MiB per range exceeds the maximum heap of "
                    + maxHeapBytes / MIB + " MiB; each shard is read in at most " + bound + " range(s)" + synthetic
                    + ", raise -Xmx for more");
            else if (syntheticBound < shardThreads)
                warnings.add("--parallelism " + parallelism + " x --shard-threads " + shardThreads + " x "
                    + syntheticRangeBytes / MIB + " MiB per synthetic-source range (its reconstruction batch included)"
                    + " exceeds the maximum heap of " + maxHeapBytes / MIB + " MiB; a synthetic-source shard is read in"
                    + " at most " + syntheticBound + " range(s), raise -Xmx for more");
            if (s3aTarget)
                warnings.add("--parallelism " + parallelism + " x --shard-threads " + shardThreads + " writes up to "
                    + (long) parallelism * shardThreads + " Parquet files at once; on an s3a:// target each buffers its "
                    + "upload blocks in fs.s3a.buffer.dir, outside --temp-budget");
            // Counted like heapBound: every range also keeps its own heap, and every shard in flight the reserve.
            long readers = (long) parallelism * shardThreads;
            long firstReads = readers * (ShardRanges.FIRST_READ_HEAP_BYTES + ShardRanges.PER_RANGE_HEAP_BYTES)
                + parallelism * ShardRanges.HEAP_RESERVE_BYTES;
            if (maxHeapBytes != Long.MAX_VALUE && firstReads > maxHeapBytes)
                warnings.add("--parallelism " + parallelism + " x --shard-threads " + shardThreads + " ranges can each read"
                    + " a document before the document memory gate knows its size: " + readers + " x ("
                    + ShardRanges.FIRST_READ_HEAP_BYTES / MIB + " + " + ShardRanges.PER_RANGE_HEAP_BYTES / MIB
                    + ") MiB for documents of up to 100 MiB, plus " + parallelism + " x " + ShardRanges.HEAP_RESERVE_BYTES / MIB
                    + " MiB reserve, exceeds the maximum heap of " + maxHeapBytes / MIB
                    + " MiB; raise -Xmx or lower --shard-threads if the index has such documents");
        }
        if (parallelism > 1 && (long) parallelism * downloadConcurrency > s3MaxConnections)
            warnings.add("--parallelism " + parallelism + " x download concurrency " + downloadConcurrency
                + " exceeds gefrierschrank.s3.maxConnections=" + s3MaxConnections
                + "; S3 downloads will queue for connections");
        return warnings;
    }

    /** An invalid setting is reported by the export itself, which fails the index before any shard. */
    private int requestedShardThreads() {
        try {
            return ShardRanges.requested(null, parallelism);
        } catch (IllegalArgumentException invalid) {
            return 1;
        }
    }
}
