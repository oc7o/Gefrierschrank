package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Intra-shard parallelism: how many contiguous doc-id ranges one shard is read and staged as,
 * where the cuts fall, and the row iterators over them. Every range has its own reader (the
 * shard's reader or one of its forks) and thread; the shard still commits once.
 */
public final class ShardRanges {

    /** Threads per shard: {@code auto} or 1-64. Hadoop configuration key and JVM property. */
    public static final String SHARD_THREADS_KEY = "gefrierschrank.shard.threads";
    /**
     * "auto" by default. Rows are identical at every thread count on every measured data set; "1"
     * keeps the serial path and one Parquet file per shard.
     */
    public static final String DEFAULT_SHARD_THREADS = "auto";
    public static final int MAX_SHARD_THREADS = 64;
    /** auto = clamp(cores / workers, 1, AUTO_MAX_THREADS). */
    static final int AUTO_MAX_THREADS = 8;

    /** A range below this many live documents is not worth a thread; the property lowers it for tests. */
    static final int MIN_RANGE_LIVE_DOCS = 250_000;
    static final String MIN_RANGE_LIVE_DOCS_PROPERTY = "gefrierschrank.shard.threads.minRangeLiveDocs";

    /**
     * Heap of one staging range besides its documents: the Parquet row group being filled
     * ({@link DeltaTableWriter#ROW_GROUP_BYTES} of compressed pages), open pages, dictionaries,
     * encoder and decompression buffers, and a synthetic fork's own reconstructor. Measured: one
     * range retained at most 38.7 MiB while writing 1M 1 KB documents (LZ4 and ZSTD stored fields,
     * ZSTD Parquet), a synthetic reconstructor 0.13 MiB.
     */
    static final long PER_RANGE_HEAP_BYTES = DeltaTableWriter.ROW_GROUP_BYTES + (8L << 20);
    /**
     * Heap a synthetic-source range keeps besides {@link #PER_RANGE_HEAP_BYTES}: its reader
     * reconstructs documents ahead of the caller in batches of up to the worker's byte limit
     * ({@link IsolatedReconstructionWorker#DEFAULT_BATCH_BYTES}, or its property), and holds them
     * twice while converting them to the export text. The one document that crosses the limit is
     * the reader's current document, which the memory gate's reading room covers like a stored one.
     */
    static long syntheticReadAheadBytes() {
        long batchBytes = Long.getLong(IsolatedReconstructionWorker.BATCH_BYTES_PROPERTY,
            IsolatedReconstructionWorker.DEFAULT_BATCH_BYTES);
        return 2 * Math.max(0, batchBytes);
    }

    /**
     * What a range of an index with this metadata reads ahead of its caller:
     * {@link #syntheticReadAheadBytes} for a synthetic-source index, whose reader is the batching
     * fallback reader, else nothing. Without metadata no fallback reader is used.
     */
    static long readAheadBytes(JsonNode indexMetadata) {
        return indexMetadata != null && (ExportPreflight.syntheticSourceSignal(indexMetadata) != null
            || ExportPreflight.SourceRetention.of(indexMetadata, -1) != ExportPreflight.SourceRetention.RETAINED)
            ? syntheticReadAheadBytes() : 0;
    }

    /** {@link #readAheadBytes(JsonNode)} plus a rehydrating range's own heap ({@link SourceVectorsShardReader#RANGE_HEAP_BYTES}). */
    static long readAheadBytes(JsonNode indexMetadata, SourceVectorsPlan vectors) {
        return readAheadBytes(indexMetadata)
            + (vectors != null && vectors.rehydrates() ? SourceVectorsShardReader.RANGE_HEAP_BYTES : 0);
    }

    /** Heap kept free per worker for everything that is not a range. */
    static final long HEAP_RESERVE_BYTES = 512L << 20;
    /**
     * Heap one range may take reading a document of up to 100 MiB, Elasticsearch's default
     * {@code http.max_content_length}, before the document memory gate learns its size. Once the
     * gate has seen such a document it keeps this room for every reading range; for the first ones,
     * read by several ranges at the same moment, there is only the heap itself.
     */
    static final long FIRST_READ_HEAP_BYTES = (long) (DocumentMemoryGate.READ_MULTIPLIER * (100L << 20));

    private static final int CHECKPOINT_DOCS = 4096;
    /** Name prefix of a large document's temporary file while it waits for heap. */
    static final String WAITING_PREFIX = "gefrierschrank-waiting-";

    private ShardRanges() {}

    /**
     * The requested threads per shard: the Hadoop configuration key, then the JVM property, then
     * {@link #DEFAULT_SHARD_THREADS}; {@code auto} resolves against {@code workers} concurrent shards.
     *
     * @throws IllegalArgumentException for anything but auto or 1-64
     */
    static int requested(Configuration conf, int workers) {
        String value = conf == null ? null : conf.getTrimmed(SHARD_THREADS_KEY);
        if (value == null) value = System.getProperty(SHARD_THREADS_KEY);
        if (value == null) value = DEFAULT_SHARD_THREADS;
        return resolve(value.trim(), workers);
    }

    /**
     * Threads per shard for {@code value}, auto resolved against {@code workers} concurrent shards.
     *
     * @throws IllegalArgumentException for anything but auto or 1-64
     */
    public static int resolve(String value, int workers) {
        if (value.equalsIgnoreCase("auto")) {
            int perWorker = Runtime.getRuntime().availableProcessors() / Math.max(1, workers);
            return Math.max(1, Math.min(AUTO_MAX_THREADS, perWorker));
        }
        int threads;
        try {
            threads = Integer.parseInt(value);
        } catch (NumberFormatException invalid) {
            threads = 0;
        }
        if (threads < 1 || threads > MAX_SHARD_THREADS)
            throw new IllegalArgumentException(SHARD_THREADS_KEY + " must be auto or between 1 and " + MAX_SHARD_THREADS);
        return threads;
    }

    static int minRangeLiveDocs() {
        return Math.max(1, Integer.getInteger(MIN_RANGE_LIVE_DOCS_PROPERTY, MIN_RANGE_LIVE_DOCS));
    }

    /** How many ranges without read-ahead the heap carries for each of {@code workers} concurrent shards. */
    static int heapBound(long maxHeapBytes, int workers) {
        return heapBound(maxHeapBytes, workers, 0);
    }

    /** How many ranges that each read {@code readAheadBytes} ahead the heap carries per concurrent shard. */
    static int heapBound(long maxHeapBytes, int workers, long readAheadBytes) {
        if (maxHeapBytes == Long.MAX_VALUE) return Integer.MAX_VALUE;
        long perWorker = maxHeapBytes / Math.max(1, workers) - HEAP_RESERVE_BYTES;
        return (int) Math.max(1, Math.min(Integer.MAX_VALUE, perWorker / (PER_RANGE_HEAP_BYTES + readAheadBytes)));
    }

    /** min(requested, liveDocs / minimum range, heap bound), at least 1. */
    static int effective(int requested, long liveDocs, long maxHeapBytes, int workers) {
        return effective(requested, liveDocs, maxHeapBytes, workers, 0);
    }

    /** {@link #effective(int, long, long, int)} for ranges that each read {@code readAheadBytes} ahead. */
    static int effective(int requested, long liveDocs, long maxHeapBytes, int workers, long readAheadBytes) {
        long byDocs = Math.max(1, liveDocs / minRangeLiveDocs());
        int bound = heapBound(maxHeapBytes, workers, readAheadBytes);
        return (int) Math.max(1, Math.min(requested, Math.min(byDocs, bound)));
    }

    /** {@code k} ranges: range r is doc ids [bounds[r], bounds[r + 1]). */
    record Plan(int k, int[] bounds, long liveDocs) {}

    /**
     * Splits [0, maxDoc) into at most {@code requested} contiguous ranges with equal live counts
     * (range r starts at the live document numbered floor(live * r / k)), from one isLive pass that
     * keeps a live count every {@value #CHECKPOINT_DOCS} documents. Deterministic for a given k.
     */
    static Plan plan(ShardReader reader, int requested, long maxHeapBytes, int workers) throws IOException {
        return plan(reader, requested, maxHeapBytes, workers, 0);
    }

    /** {@link #plan(ShardReader, int, long, int)} for ranges that each read {@code readAheadBytes} ahead. */
    static Plan plan(ShardReader reader, int requested, long maxHeapBytes, int workers, long readAheadBytes)
            throws IOException {
        int maxDoc = reader.docCount();
        int[] checkpoints = new int[maxDoc / CHECKPOINT_DOCS + 1];
        int live = 0;
        for (int doc = 0; doc < maxDoc; doc++) {
            if (doc % CHECKPOINT_DOCS == 0) checkpoints[doc / CHECKPOINT_DOCS] = live;
            if (reader.isLive(doc)) live++;
        }
        int k = effective(requested, live, maxHeapBytes, workers, readAheadBytes);
        int[] bounds = new int[k + 1];
        bounds[k] = maxDoc;
        for (int r = 1; r < k; r++) {
            long before = (long) live * r / k;
            // The last checkpoint at or below the target, then a scan to the live document itself.
            int block = 0;
            int low = 0, high = checkpoints.length - 1;
            while (low <= high) {
                int mid = (low + high) >>> 1;
                if (checkpoints[mid] <= before) {
                    block = mid;
                    low = mid + 1;
                } else {
                    high = mid - 1;
                }
            }
            int doc = block * CHECKPOINT_DOCS;
            long seen = checkpoints[block];
            while (true) {
                if (reader.isLive(doc)) {
                    if (seen == before) break;
                    seen++;
                }
                doc++;
            }
            bounds[r] = doc;
        }
        return new Plan(k, bounds, live);
    }

    /**
     * Rows of documents stored as SMILE, CBOR and YAML (indexes 0, 1, 2) over every range, by the
     * base part of their {@code _source_kind}.
     */
    static long[] convertedSources(java.util.List<ArchiveRowIterator> ranges) {
        long[] sums = new long[3];
        for (var range : ranges) {
            long[] counts = range.convertedSources();
            for (int format = 0; format < sums.length; format++) sums[format] += counts[format];
        }
        return sums;
    }

    /**
     * The archive rows of doc ids [start, end), in ascending order, as rows over the bytes the
     * reader handed out. A document the {@link SourceValidator} does not accept takes the legacy
     * per-document path, so rejections and failures are exactly those of the JSON row path. Once
     * the shard's ranges are cancelled, hasNext() throws instead of ending, so a range can never
     * pass for complete.
     *
     * <p>With a {@link DocumentMemoryGate}, the iterator registers as a writing range when it is
     * first used and admits every document right after the reader hands it over, before any copy;
     * the previous document's reservation goes when the next one is asked for (the writer has
     * appended it by then) and at {@link #close}, which the thread that iterated calls. It passes the
     * writer's reports about its open file on to the gate, and has the writer close that file when a
     * document does not fit. A document that still has to wait for heap waits in a temporary file in
     * {@code waitingDir}, not in the heap, and is read back once admitted.
     *
     * <p>An iterator that owns its reader (a fork) closes it in {@link #close}, on the range's thread
     * as soon as the range is staged: a stored-fields reader keeps the last block it decompressed,
     * and after a range's last documents that block can hold the next range's large document.
     */
    static final class ArchiveRowIterator
            implements java.util.Iterator<ArchiveRow>, AutoCloseable, DeltaTableWriter.OpenFileHeap {
        private final ShardReader reader;
        private final int start;
        private final int end;
        private final AtomicBoolean cancelled;
        private final java.util.function.BooleanSupplier cancelCheck;
        private final DocumentMemoryGate gate;
        private final Path waitingDir;
        private final boolean ownsReader;
        private final long readAheadBytes;
        private final SourceValidator validator = new SourceValidator();
        private DocumentMemoryGate.Range memory;
        private DeltaTableWriter.OpenFileCloser closeOpenFile;
        private int next;
        private byte[] source;
        private int length;
        private long perDocumentValidations;
        private final long[] convertedSources = new long[3];
        private boolean readerClosed;
        private Exception readerCloseFailure;

        /** The whole shard, as one range, without a memory gate. */
        ArchiveRowIterator(ShardReader reader) {
            this(reader, 0, -1, null, null, null);
        }

        /** Doc ids [start, end), or [start, docCount) for end -1, without a memory gate; {@code cancelled} may be null. */
        ArchiveRowIterator(ShardReader reader, int start, int end, AtomicBoolean cancelled) {
            this(reader, start, end, cancelled, null, null);
        }

        /** {@code gate}, {@code cancelled} and {@code waitingDir} may be null; null waits in the default temp dir. */
        ArchiveRowIterator(ShardReader reader, int start, int end, AtomicBoolean cancelled, DocumentMemoryGate gate,
                           Path waitingDir) {
            this(reader, start, end, cancelled, gate, waitingDir, false);
        }

        /** With {@code ownsReader}, {@link #close} closes {@code reader}. */
        ArchiveRowIterator(ShardReader reader, int start, int end, AtomicBoolean cancelled, DocumentMemoryGate gate,
                           Path waitingDir, boolean ownsReader) {
            this(reader, start, end, cancelled, gate, waitingDir, ownsReader, 0);
        }

        /** Registers with the gate as a range whose reader holds up to {@code readAheadBytes} ahead of it. */
        ArchiveRowIterator(ShardReader reader, int start, int end, AtomicBoolean cancelled, DocumentMemoryGate gate,
                           Path waitingDir, boolean ownsReader, long readAheadBytes) {
            this.reader = reader;
            this.start = start;
            this.end = end;
            this.cancelled = cancelled;
            this.cancelCheck = cancelled == null ? null : cancelled::get;
            this.gate = gate;
            this.waitingDir = waitingDir;
            this.ownsReader = ownsReader;
            this.readAheadBytes = readAheadBytes;
            this.next = start;
        }

        private int end() throws IOException {
            return end < 0 ? reader.docCount() : end;
        }

        @Override
        public boolean hasNext() {
            if (cancelled != null && cancelled.get()) throw new CancellationException("another range of the shard failed");
            if (gate != null && memory == null) {
                try {
                    memory = gate.register(cancelCheck, readAheadBytes);
                } catch (IOException e) {
                    throw new IllegalStateException("waiting for heap to read the shard", e);
                }
            }
            try {
                while (next < end() && !reader.isLive(next)) next++;
                return next < end();
            } catch (IOException e) {
                throw new IllegalStateException("counting shard documents", e);
            }
        }

        @Override
        public ArchiveRow next() {
            if (!hasNext()) throw new NoSuchElementException();
            ArchiveRow row;
            try {
                if (memory != null) memory.release();
                source = null;
                reader.readSource(next, (bytes, length) -> {
                    source = bytes;
                    this.length = length;
                });
                byte[] bytes = source;
                source = null;
                if (memory != null && bytes != null && !memory.tryAdmit(length, closeOpenFile)) {
                    Path waiting = moveOutOfHeap(bytes, length);
                    bytes = null;
                    try {
                        memory.awaitAdmission(length, cancelCheck);
                        bytes = readBack(waiting, length);
                    } finally {
                        deleteQuietly(waiting);
                    }
                }
                if (bytes != null && validator.accepts(bytes, length)) {
                    var metadata = reader.documentMetadata(next);
                    countConverted(metadata.sourceKind());
                    row = new ArchiveRow(bytes, length, metadata);
                } else {
                    // The legacy path, in its order: decode, blank check, metadata, validation.
                    perDocumentValidations++;
                    String text = bytes == null ? null : new String(bytes, 0, length, StandardCharsets.UTF_8);
                    if (text == null || text.isBlank()) throw new IOException("Live document has no source payload");
                    var metadata = reader.documentMetadata(next);
                    ArchiveContext.validateSource(text);
                    countConverted(metadata.sourceKind());
                    byte[] encoded = text.getBytes(StandardCharsets.UTF_8);
                    row = new ArchiveRow(encoded, encoded.length, metadata);
                }
            } catch (IOException e) {
                throw new IllegalStateException("reading _source of doc " + next, e);
            }
            next++;
            return row;
        }

        private Path moveOutOfHeap(byte[] bytes, int length) throws IOException {
            Path file = waitingDir == null ? de.sloow.gefrierschrank.core.TempRoot.newFile(WAITING_PREFIX, ".json")
                : Files.createTempFile(waitingDir, WAITING_PREFIX, ".json");
            try (var out = Files.newOutputStream(file)) {
                out.write(bytes, 0, length);
            } catch (IOException | RuntimeException failure) {
                deleteQuietly(file);
                throw failure;
            }
            return file;
        }

        private static byte[] readBack(Path file, int length) throws IOException {
            byte[] bytes = new byte[length];
            try (var in = Files.newInputStream(file)) {
                if (in.readNBytes(bytes, 0, length) != length) throw new IOException("a waiting document's temporary file is incomplete");
            }
            return bytes;
        }

        /** The pipeline deletes the shard's temp dir, where the file lives, in any case. */
        private static void deleteQuietly(Path file) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException ignored) {
                // see above
            }
        }

        int start() {
            return start;
        }

        @Override
        public void closeOpenFileWith(DeltaTableWriter.OpenFileCloser closer) {
            closeOpenFile = closer;
        }

        @Override
        public void openFileRetains(long bytes) {
            if (memory != null) memory.openFileRetains(bytes);
        }

        @Override
        public void openFileClosed() {
            if (memory != null) memory.openFileClosed();
        }

        @Override
        public boolean openFileCloseRequested() {
            return memory != null && memory.closeFileRequested();
        }

        /** Gives back this range's document memory and closes a reader it owns; idempotent. */
        @Override
        public void close() {
            if (memory != null) memory.close();
            if (ownsReader && !readerClosed) {
                readerClosed = true;
                try {
                    reader.close();
                } catch (Exception failure) {
                    readerCloseFailure = failure;
                }
            }
        }

        /** Why closing the owned reader failed, or null. */
        Exception readerCloseFailure() {
            return readerCloseFailure;
        }

        /** Documents that took the per-document path, accepted or not. */
        long perDocumentValidations() {
            return perDocumentValidations;
        }

        /** Rows of documents stored as SMILE, CBOR and YAML (indexes 0, 1, 2), whatever suffix their kind has. */
        long[] convertedSources() {
            return convertedSources.clone();
        }

        private void countConverted(String kind) {
            if (kind == null || !kind.startsWith("stored-")) return;
            if (kind.startsWith("stored-smile")) {
                convertedSources[0]++;
            } else if (kind.startsWith("stored-cbor")) {
                convertedSources[1]++;
            } else if (kind.startsWith("stored-yaml")) {
                convertedSources[2]++;
            }
        }
    }
}
