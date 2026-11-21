package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.IndexMapping;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Streams JSONL documents into a Delta table. Parquet files are written with
 * parquet-mr (streaming: memory holds one row group at a time); files are
 * committed to the Delta transaction log via delta-standalone as AddFile actions.
 *
 * <p>Schema: inferred from the first document. Nested objects/arrays are
 * serialized as JSON strings to keep the schema flat and stable across shards —
 * matching how delta handles heterogeneous upstream records without evolving
 * the table schema mid-write.
 */
public final class DeltaTableWriter {

    /**
     * Parquet compression codec: {@code zstd}, {@code snappy} or {@code uncompressed}. Read from the
     * Hadoop configuration, then from the JVM system property of the same name; unset means
     * {@link #defaultParquetCodec()}. The codec is per column chunk, so a table may mix codecs
     * across resumed runs.
     */
    public static final String PARQUET_CODEC_KEY = "gefrierschrank.parquet.codec";

    /**
     * Target size of one data file in bytes (Hadoop size suffixes such as {@code 256m} work),
     * read from the Hadoop configuration, then from the JVM system property of the same name.
     */
    public static final String TARGET_FILE_BYTES_KEY = "gefrierschrank.parquet.target-file-bytes";
    static final long DEFAULT_TARGET_FILE_BYTES = 256L << 20;
    // The written-size check is cheap but not free, and a file only needs to end near its target.
    // Its second trigger counts row JSON chars for JSON rows and source bytes for archive rows.
    private static final int ROWS_PER_SIZE_CHECK = 1_000;
    private static final long MAX_UNITS_PER_SIZE_CHECK = 8L << 20;

    /**
     * Parquet row group size. A writer holds its current row group's compressed pages in heap, so
     * this bounds the heap of every concurrently written file; smaller groups also let S3A upload a
     * large file while it is still being written.
     */
    static final long ROW_GROUP_BYTES = 32L << 20;

    // Attempts per commit when another process committed concurrently; our entry never landed.
    private static final int COMMIT_ATTEMPTS = 4;
    /**
     * True once the process's temp run root has ended. An exiting process holds the root's name
     * with a file (TempRoot), so S3A cannot create its buffer directory there: a DiskErrorException
     * then says nothing about the disk. A test seam.
     */
    static volatile java.util.function.BooleanSupplier runRootEnded = de.sloow.gefrierschrank.core.TempRoot::hasEnded;

    // One commit at a time per table in this process, across writer instances and DeltaLog
    // objects: LocalLogStore only synchronizes on its own instance, so two DeltaLogs of one table
    // could otherwise race on the same log version.
    private static final ConcurrentHashMap<String, ReentrantLock> COMMIT_LOCKS = new ConcurrentHashMap<>();

    private final String tableUri;
    private final ReentrantLock commitLock;
    private final ObjectMapper json = de.sloow.gefrierschrank.core.DocumentJson.newMapper();
    private DeltaLog deltaLog;
    private Configuration deltaLogConf;
    // Staged shards whose commit outcome was unknown when it failed; guarded by commitLock.
    private final List<StagedShard> pendingCleanup = new ArrayList<>();
    // Set by requireArchiveSnapshot; replaced and advanced only under commitLock.
    private volatile ArchiveGuard guard;
    // Tags every staged AddFile carries besides ShardKey.TAG (withFileTags).
    private volatile Map<String, String> fileTags = Map.of();

    /** Test seam: runs under the commit lock right before a commit is attempted. */
    volatile Runnable beforeCommit = () -> {};

    public DeltaTableWriter(String tableUri) {
        this.tableUri = tableUri;
        this.commitLock = COMMIT_LOCKS.computeIfAbsent(lockKey(tableUri), key -> new ReentrantLock());
    }

    /** Consumes one JSON document per element. Returns total rows written. */
    public long write(Iterator<String> documents) throws IOException {
        return write(new Configuration(), documents);
    }

    /** Same, with an explicit Hadoop configuration (S3A credentials etc.). */
    public long write(Configuration hadoopConf, Iterator<String> documents) throws IOException {
        return write(hadoopConf, documents, null);
    }

    /**
     * Same, but typing the schema from the index's real ES mapping instead of guessing from the
     * first document. If {@code mapping} is null, falls back to first-document inference.
     */
    public long write(Configuration hadoopConf, Iterator<String> documents, IndexMapping mapping) throws IOException {
        return write(hadoopConf, documents, mapping, null);
    }

    /**
     * Same, but tagging every committed file with {@code shardKey} (see {@link ShardKey}) so a
     * later run can tell, from the Delta log alone, which shards are already committed and skip
     * them on resume. {@code shardKey} may be null, in which case files carry no shard tag -
     * preserving the behavior of the other overloads for callers that don't need resumability.
     *
     * <p>Safe to call concurrently for distinct shard keys: the Parquet files are written without
     * any open transaction, and only the short commit is serialized per table. A key that is
     * already committed returns 0 without writing anything.
     */
    public long write(Configuration hadoopConf, Iterator<String> documents, IndexMapping mapping, String shardKey) throws IOException {
        CompressionCodecName codec = usableParquetCodec(hadoopConf);
        long targetFileBytes = targetFileBytes(hadoopConf);
        return onTarget(() -> {
            DeltaLog log = log(hadoopConf);
            OptimisticTransaction current = log.startTransaction();
            if (shardKey != null && current.txnVersion(shardKey) >= 0) return 0L;
            verifyGuard(log, current.readVersion());
            StructType tableSchema = current.readVersion() < 0 ? null : current.metadata().getSchema();
            StagedShard staged = stage(hadoopConf, new JsonRows(documents), mapping, shardKey, tableSchema, codec, targetFileBytes);
            return commit(hadoopConf, log, staged).rows();
        });
    }

    /**
     * Outcome of an archive shard write. {@code alreadyCommitted} is true exactly when the shard key
     * was found committed, before staging, at commit time or when an ambiguous commit failure turned
     * out to be another writer's commit; nothing of this call is in the table then.
     */
    record WriteResult(long rows, boolean alreadyCommitted) {}

    /** {@link #writeArchive(Configuration, List, Runnable, ArchiveContext, int, String)} of a shard read as one range. */
    WriteResult writeArchive(Configuration hadoopConf, List<? extends Iterator<ArchiveRow>> ranges,
                             ArchiveContext archive, int shard, String shardKey) throws IOException {
        if (ranges.size() != 1) throw new IllegalArgumentException("several ranges need a cancellation");
        return writeArchive(hadoopConf, ranges, () -> {}, archive, shard, shardKey);
    }

    /**
     * Archive shard write: the rows of one shard, as contiguous doc-id ranges in ascending order,
     * staged as Parquet files and committed once under {@code shardKey}, with the fixed archive
     * schema of {@link ArchiveContext#mapping()}. Every column but {@code _source}, {@code _id},
     * {@code _routing} and {@code _doc} is constant per shard. Rows are appended one at a time, each
     * before its iterator is asked for the next one.
     *
     * <p>One range is staged on the calling thread. Several are staged concurrently, each on its
     * own thread; their files form one staged shard, in range order, committed once. If any range
     * fails, {@code cancelRanges} makes the others' iterators throw, and every range's files are
     * deleted. This method returns or throws only after every range thread has ended, and an Error
     * of any range is rethrown as that Error. An already committed key starts no range.
     */
    WriteResult writeArchive(Configuration hadoopConf, List<? extends Iterator<ArchiveRow>> ranges, Runnable cancelRanges,
                             ArchiveContext archive, int shard, String shardKey) throws IOException {
        if (ranges.isEmpty()) throw new IllegalArgumentException("a shard has at least one range");
        CompressionCodecName codec = usableParquetCodec(hadoopConf);
        long targetFileBytes = targetFileBytes(hadoopConf);
        return onTarget(() -> {
            DeltaLog log = log(hadoopConf);
            OptimisticTransaction current = log.startTransaction();
            if (current.txnVersion(shardKey) >= 0) return new WriteResult(0, true);
            verifyGuard(log, current.readVersion());
            StructType tableSchema = current.readVersion() < 0 ? null : current.metadata().getSchema();
            StagedShard staged;
            if (ranges.size() == 1) {
                try {
                    staged = stage(hadoopConf, new ArchiveRows(ranges.get(0), new ArchiveColumns(archive, shard)),
                        ArchiveContext.mapping(), shardKey, tableSchema, codec, targetFileBytes);
                } finally {
                    closeRange(ranges.get(0));
                }
            } else {
                staged = stageRanges(hadoopConf, ranges, cancelRanges, archive, shard, shardKey, tableSchema, codec, targetFileBytes);
            }
            return commit(hadoopConf, log, staged);
        });
    }

    /**
     * Stages each range on a dedicated platform daemon thread with the caller's context class
     * loader, for the reasons {@link ShardWorkPool} gives, and never through a shared executor.
     * A range thread records its failures, of staging and of closing its range, in slots
     * allocated here, in the order they happen: adding one to a collection allocates, and an
     * OutOfMemoryError there would lose it, so that Error would never end the run.
     */
    private StagedShard stageRanges(Configuration hadoopConf, List<? extends Iterator<ArchiveRow>> ranges,
                                    Runnable cancelRanges, ArchiveContext archive, int shard, String shardKey,
                                    StructType tableSchema, CompressionCodecName codec, long targetFileBytes)
            throws IOException {
        int count = ranges.size();
        var staged = new StagedShard[count];
        var failed = new Throwable[2 * count + 1];
        var failedCount = new java.util.concurrent.atomic.AtomicInteger();
        var threads = new ArrayList<Thread>(count);
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        for (int range = 0; range < count; range++) {
            int index = range;
            Thread thread = new Thread(() -> {
                try {
                    staged[index] = stage(hadoopConf, new ArchiveRows(ranges.get(index), new ArchiveColumns(archive, shard)),
                        ArchiveContext.mapping(), shardKey, tableSchema, codec, targetFileBytes);
                } catch (Throwable failure) {
                    failed[failedCount.getAndIncrement()] = failure;
                    cancelRanges.run();
                } finally {
                    try {
                        closeRange(ranges.get(index));
                    } catch (Throwable closeFailure) {
                        failed[failedCount.getAndIncrement()] = closeFailure;
                    }
                }
            }, "gefrierschrank-range-" + shard + "-" + range);
            thread.setDaemon(true);
            thread.setContextClassLoader(contextLoader);
            try {
                thread.start();
            } catch (Throwable startFailure) {
                failed[failedCount.getAndIncrement()] = startFailure;
                cancelRanges.run();
                break;
            }
            threads.add(thread);
        }
        joinUninterruptibly(threads);

        var failures = new ArrayList<Throwable>(java.util.Arrays.asList(failed).subList(0, failedCount.get()));

        List<AddFile> files = new ArrayList<>();
        long rows = 0;
        boolean allEmpty = true;
        for (StagedShard range : staged) {
            if (range == null) continue;
            files.addAll(range.files());
            rows += range.rows();
            allEmpty &= range.rows() == 0;
        }
        if (failures.isEmpty()) {
            for (StagedShard range : staged) {
                if (!range.schema().equals(staged[0].schema())) {
                    failures.add(new IllegalStateException("the ranges of a shard staged different schemas"));
                    break;
                }
            }
        }
        if (!failures.isEmpty()) {
            Throwable failure = firstFailure(failures);
            deleteFiles(hadoopConf, files, failure);
            if (failure instanceof Error error) throw error;
            if (failure instanceof IOException io) throw io;
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new IOException("staging a range failed", failure);
        }
        // An empty range stages one empty file; the shard keeps one only if it has no rows at all.
        List<AddFile> kept = new ArrayList<>();
        for (int range = 0; range < count; range++) {
            if (staged[range].rows() > 0 || allEmpty && range == 0) kept.addAll(staged[range].files());
            else deleteFiles(hadoopConf, staged[range].files(), null);
        }
        return new StagedShard(shardKey, List.copyOf(kept), staged[0].schema(), rows);
    }

    /**
     * A range whose iterator holds resources (the pipeline's document memory) gives them back on
     * the thread that staged it, as soon as its staging ends, so no other range waits for them.
     */
    private static void closeRange(Iterator<ArchiveRow> range) {
        if (range instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // Nothing of the shard's output depends on it; the pipeline closes the range again.
            }
        }
    }

    /**
     * An Error first, otherwise the first failure that is not another range's cancellation; the
     * rest are suppressed. Among Errors, the target's {@link FSError} comes last: it becomes a
     * {@link TargetStorageException}, which must never hide another range's fatal Error.
     */
    private static Throwable firstFailure(java.util.Collection<Throwable> failures) {
        Throwable chosen = null;
        for (Throwable failure : failures) if (failure instanceof Error && !(failure instanceof FSError)) { chosen = failure; break; }
        if (chosen == null) for (Throwable failure : failures) if (failure instanceof FSError) { chosen = failure; break; }
        if (chosen == null) {
            for (Throwable failure : failures) {
                if (!(failure instanceof java.util.concurrent.CancellationException)) { chosen = failure; break; }
            }
        }
        if (chosen == null) chosen = failures.iterator().next();
        for (Throwable failure : failures) if (failure != chosen) chosen.addSuppressed(failure);
        return chosen;
    }

    /** Waits for every thread even if interrupted, so no range still reads when the caller closes its reader. */
    static void joinUninterruptibly(List<Thread> threads) {
        boolean interrupted = false;
        for (Thread thread : threads) {
            while (true) {
                try {
                    thread.join();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    @FunctionalInterface
    private interface TargetAction<T> {
        T run() throws IOException;
    }

    /**
     * Runs {@code action} against the target, turning its storage exhaustion into a
     * {@link TargetStorageException}: {@link FSError}, which Hadoop's local filesystem throws for
     * any I/O error on a data stream (ENOSPC, EFBIG, EIO, quota), and the {@link DiskErrorException}
     * of S3A's local block buffers, also where a row iterator wraps it (a file the memory gate has
     * the writer close between two rows), unless the process is exiting ({@link #runRootEnded}).
     * No other Error is caught.
     */
    private static <T> T onTarget(TargetAction<T> action) throws IOException {
        try {
            return action.run();
        } catch (FSError storage) {
            throw targetStorageFailure(storage);
        } catch (IOException | RuntimeException failure) {
            if (!(failure instanceof TargetStorageException) && diskError(failure) != null && !runRootEnded.getAsBoolean()) {
                throw targetStorageFailure(failure);
            }
            throw failure;
        }
    }

    private static DiskErrorException diskError(Throwable failure) {
        var seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
        for (Throwable t = failure; t != null && seen.add(t); t = t.getCause()) {
            if (t instanceof DiskErrorException disk) return disk;
        }
        return null;
    }

    /**
     * The storage failure as the root cause of what callers see, so reports name
     * TargetStorageException; the original failure stays attached as suppressed. Its message is
     * errno text at best, but only the fixed vocabulary and the class of the FSError or
     * DiskErrorException, not of what wraps it, reach the new message.
     */
    static TargetStorageException targetStorageFailure(Throwable failure) {
        Throwable storage = failure instanceof FSError || diskError(failure) == null ? failure : diskError(failure);
        var translated = new TargetStorageException(storageReason(failure) + ", " + storage.getClass().getSimpleName(), null);
        translated.addSuppressed(failure);
        return translated;
    }

    private static String storageReason(Throwable failure) {
        var seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
        for (Throwable t = failure; t != null && seen.add(t); t = t.getCause()) {
            String message = t.getMessage() == null ? "" : t.getMessage().toLowerCase(java.util.Locale.ROOT);
            if (message.contains("no space")) return "no space left";
            if (message.contains("file too large")) return "file too large";
            if (message.contains("quota")) return "quota exceeded";
        }
        return "I/O error";
    }

    /** Parquet files of one write() call that no committed log entry references yet. */
    private record StagedShard(String key, List<AddFile> files, StructType schema, long rows) {
        Set<String> paths() {
            Set<String> paths = new java.util.HashSet<>();
            for (AddFile file : files) paths.add(file.getPath());
            return paths;
        }
    }

    /**
     * Rows whose producer accounts for the heap that the writer's open Parquet file keeps for large
     * values ({@link DocumentMemoryGate}). parquet-mr keeps copies of each row group's smallest and
     * largest value for the footer, and its codec's output buffer at the largest compressed page,
     * until the file is closed. The writer reports that after every row of at least
     * {@link DocumentMemoryGate#BYPASS_BYTES}, and closes its open file when the producer asks.
     */
    interface OpenFileHeap {
        /** {@code closer} closes the writer's open file, if any; given before the first row. */
        void closeOpenFileWith(OpenFileCloser closer);

        /** The open file keeps about {@code bytes} of heap for the values written to it so far. */
        void openFileRetains(long bytes);

        /** The open file was closed. */
        void openFileClosed();

        /** True when the writer should close its open file after the current row. */
        boolean openFileCloseRequested();
    }

    /** Closes the writer's open file on the thread that stages the rows, between two rows. */
    @FunctionalInterface
    interface OpenFileCloser {
        void close() throws IOException;
    }

    /** One shard's rows as the staging loop consumes them: prepare one row, then append it. */
    private interface RowSource {
        /** Prepares the next row; false when there is none. */
        boolean advance() throws IOException;

        /** Units of the prepared row for the size-check cadence. */
        long sizeUnits();

        /** Fixes the file schema from the prepared row if it is inferred from the first document. */
        void inferSchema(JsonSchemaMapper inferrer);

        /** Appends the prepared row and lets go of it. */
        void appendTo(RollingParquetFile file) throws IOException;

        /** How the producer of the rows can have the open file closed; see {@link OpenFileHeap}. */
        default void closeOpenFileWith(OpenFileCloser closer) {}

        /** The open file was closed. */
        default void fileClosed() {}

        /** True when the open file should be closed after the row just appended. */
        default boolean closeFileNow() {
            return false;
        }
    }

    /** JSON documents, one object per element; blank elements are skipped. */
    private final class JsonRows implements RowSource {
        private final Iterator<String> documents;
        private String line;
        private JsonNode doc;

        JsonRows(Iterator<String> documents) {
            this.documents = documents;
        }

        @Override public boolean advance() throws IOException {
            while (documents.hasNext()) {
                String next = documents.next();
                if (next.isBlank()) continue;
                JsonNode parsed = json.readTree(next);
                if (parsed == null || !parsed.isObject()) {
                    throw new IOException("expected a JSON object document");
                }
                line = next;
                doc = parsed;
                return true;
            }
            return false;
        }

        @Override public long sizeUnits() {
            return line.length();
        }

        @Override public void inferSchema(JsonSchemaMapper inferrer) {
            if (inferrer.notYetInferred()) inferrer.inferFrom(doc);
        }

        @Override public void appendTo(RollingParquetFile file) throws IOException {
            JsonNode row = doc;
            line = null;
            doc = null;
            file.append(row);
        }
    }

    /** Archive rows; the schema is always the archive mapping. */
    private static final class ArchiveRows implements RowSource {
        private final Iterator<? extends ArchiveRow> rows;
        private final ArchiveColumns columns;
        private final OpenFileHeap heap;
        private ArchiveRow row;

        ArchiveRows(Iterator<? extends ArchiveRow> rows, ArchiveColumns columns) {
            this.rows = rows;
            this.columns = columns;
            this.heap = rows instanceof OpenFileHeap accounted ? accounted : null;
        }

        @Override public void closeOpenFileWith(OpenFileCloser closer) {
            if (heap != null) heap.closeOpenFileWith(closer);
        }

        @Override public void fileClosed() {
            if (heap != null) heap.openFileClosed();
        }

        @Override public boolean closeFileNow() {
            return heap != null && heap.openFileCloseRequested();
        }

        @Override public boolean advance() {
            if (!rows.hasNext()) return false;
            row = rows.next();
            return true;
        }

        @Override public long sizeUnits() {
            return row.length();
        }

        @Override public void inferSchema(JsonSchemaMapper inferrer) {}

        @Override public void appendTo(RollingParquetFile file) throws IOException {
            ArchiveRow appended = row;
            row = null;
            file.append(appended, columns);
            if (heap != null && appended.length() >= DocumentMemoryGate.BYPASS_BYTES) heap.openFileRetains(file.retainedBytes());
        }
    }

    /**
     * The Avro values of one shard's archive rows, equal to what the JSON row path produces for
     * {@link ArchiveContext#document}: strings as {@link AvroStrings#utf8} UTF-8, {@code _source} as
     * its stored bytes, longs, and {@code _unmapped_fields} null because every field is mapped.
     */
    static final class ArchiveColumns {
        static final List<String> NAMES = List.of("_index", "_repository_index_id", "_snapshot_uuid", "_snapshot",
            "_source_index", "_source_snapshot_uuid", "_source", "_id", "_routing", "_source_kind", "_shard", "_doc",
            JsonSchemaMapper.UNMAPPED_FIELDS_FIELD);
        private static final int SOURCE = 6, ID = 7, ROUTING = 8, KIND = 9, SHARD = 10, DOC = 11, UNMAPPED = 12;
        private static final int LINEAGE = 6;

        private final ArchiveContext archive;
        private final Long shard;
        private Utf8[] lineage;
        private Utf8 stored;
        private Utf8 synthetic;

        ArchiveColumns(ArchiveContext archive, int shard) {
            this.archive = archive;
            this.shard = (long) shard;
        }

        /** Field position of every name in {@link #NAMES}, in that order. */
        static int[] positions(Schema schema) {
            var byName = new java.util.HashMap<String, Integer>();
            for (Schema.Field field : schema.getFields()) byName.put(field.name(), field.pos());
            if (!byName.keySet().equals(Set.copyOf(NAMES))) {
                throw new IllegalStateException("the table's columns are not the archive columns");
            }
            int[] positions = new int[NAMES.size()];
            for (int i = 0; i < positions.length; i++) positions[i] = byName.get(NAMES.get(i));
            return positions;
        }

        GenericData.Record record(Schema schema, int[] positions, ArchiveRow row) {
            // Converted at the first row, where the JSON row path converts them too.
            if (lineage == null) {
                String[] values = {archive.indexName(), archive.repositoryIndexId(), archive.snapshotUuid(),
                    archive.snapshotName(), archive.sourceIndex(), archive.sourceSnapshotUuid()};
                Utf8[] converted = new Utf8[LINEAGE];
                for (int i = 0; i < LINEAGE; i++) converted[i] = utf8(values[i]);
                lineage = converted;
                stored = new Utf8("stored");
                synthetic = new Utf8("synthetic");
            }
            var record = new GenericData.Record(schema);
            for (int i = 0; i < LINEAGE; i++) record.put(positions[i], lineage[i]);
            var source = new Utf8(row.source());
            if (row.length() < row.source().length) source.setByteLength(row.length());
            record.put(positions[SOURCE], source);
            var metadata = row.metadata();
            record.put(positions[ID], utf8(metadata.id()));
            record.put(positions[ROUTING], utf8(metadata.routing()));
            String kind = metadata.sourceKind();
            record.put(positions[KIND], "stored".equals(kind) ? stored : "synthetic".equals(kind) ? synthetic : utf8(kind));
            record.put(positions[SHARD], shard);
            record.put(positions[DOC], (long) metadata.slot());
            record.put(positions[UNMAPPED], null);
            return record;
        }

        private static Utf8 utf8(String value) {
            return value == null ? null : AvroStrings.utf8(value);
        }
    }

    private StagedShard stage(Configuration hadoopConf, RowSource rows, IndexMapping mapping, String shardKey,
                              StructType tableSchema, CompressionCodecName codec, long targetFileBytes) throws IOException {
        // Schema is fixed on the table's first-ever commit and reused for every commit after
        // that. Re-asserting an unchanged schema on every shard commit
        // is needless log churn, and - more importantly for a resumed run - it must never let a
        // shard's independently-inferred schema silently evolve or corrupt an in-progress table.
        Map<String, String> tags = new java.util.HashMap<>(fileTags);
        if (shardKey != null) tags.put(ShardKey.TAG, shardKey);
        tags = Map.copyOf(tags);
        long unitsPerSizeCheck = Math.min(MAX_UNITS_PER_SIZE_CHECK, targetFileBytes);

        var inferrer = new JsonSchemaMapper();
        if (mapping != null) {
            inferrer.inferFromEsMapping(mapping);
        }
        long total = 0;
        long rowsSinceSizeCheck = 0;
        long unitsSinceSizeCheck = 0;
        // Per-call unique prefix: the file counter alone restarts at 0 on every write() call, so
        // without this, every shard's "part-0.parquet" would collide with every other shard's
        // in the same table directory (which would throw on a local filesystem and silently
        // overwrite another shard's data file on S3A).
        var files = new StageFiles(hadoopConf, "part-" + java.util.UUID.randomUUID(), inferrer, codec, tags, rows);

        try {
            rows.closeOpenFileWith(files::closeOpen);
            while (rows.advance()) {
                rows.inferSchema(inferrer);
                if (total == 0 && tableSchema != null) {
                    inferrer.useTableSchema(tableSchema);
                }
                RollingParquetFile file = files.open();
                long units = rows.sizeUnits();
                rows.appendTo(file);
                total++;

                rowsSinceSizeCheck++;
                unitsSinceSizeCheck += units;
                if (rowsSinceSizeCheck >= ROWS_PER_SIZE_CHECK || unitsSinceSizeCheck >= unitsPerSizeCheck) {
                    rowsSinceSizeCheck = 0;
                    unitsSinceSizeCheck = 0;
                    if (file.dataSize() >= targetFileBytes) files.closeOpen();
                }
                if (rows.closeFileNow()) files.closeOpen();
            }

            if (total == 0 && !inferrer.notYetInferred()) {
                if (tableSchema != null) inferrer.useTableSchema(tableSchema);
                files.open();
            }
            files.closeOpen();
        } catch (IOException | RuntimeException | Error failure) {
            // Nothing has been committed yet. Close the active stream and remove this attempt's
            // files so repeatedly quarantined shards do not exhaust disk or leave S3 uploads open.
            files.abort(failure);
            throw failure;
        }
        if (files.closed.isEmpty() && total == 0) {
            throw new IOException("no documents to write");
        }
        return new StagedShard(shardKey, List.copyOf(files.closed), inferrer.deltaSchema(), total);
    }

    /**
     * The Parquet files of one stage() call: the open one and the closed ones. The open file is
     * closed when it reaches its target size, at the end, or between two rows when the producer of
     * the rows needs the heap it keeps (see {@link OpenFileHeap}).
     */
    private final class StageFiles {
        private final Configuration conf;
        private final String prefix;
        private final JsonSchemaMapper inferrer;
        private final CompressionCodecName codec;
        private final Map<String, String> tags;
        private final RowSource rows;
        private final List<AddFile> closed = new ArrayList<>();
        private RollingParquetFile open;
        private long counter;

        StageFiles(Configuration conf, String prefix, JsonSchemaMapper inferrer, CompressionCodecName codec,
                   Map<String, String> tags, RowSource rows) {
            this.conf = conf;
            this.prefix = prefix;
            this.inferrer = inferrer;
            this.codec = codec;
            this.tags = tags;
            this.rows = rows;
        }

        RollingParquetFile open() throws IOException {
            if (open == null) open = new RollingParquetFile(conf, tableUri, prefix + "-" + (counter++), inferrer, codec);
            return open;
        }

        void closeOpen() throws IOException {
            if (open == null) return;
            closed.add(open.close(tags));
            open = null;
            rows.fileClosed();
        }

        void abort(Throwable failure) {
            if (open != null) {
                // A full disk fails the close again; the file must still go.
                try { open.writer.close(); } catch (Exception | FSError closeFailure) { failure.addSuppressed(closeFailure); }
                try { open.filePath.getFileSystem(conf).delete(open.filePath, false); }
                catch (Exception | FSError cleanupFailure) { failure.addSuppressed(cleanupFailure); }
                open = null;
            }
            deleteFiles(conf, closed, failure);
        }
    }

    /**
     * Commits a staged shard in one short transaction under the table's commit lock. Staged files
     * are deleted only when no log entry can reference them: before our entry is written, after a
     * conflict (Delta checks conflicts before writing), or once the log shows the key is absent.
     */
    private WriteResult commit(Configuration conf, DeltaLog log, StagedShard staged) throws IOException {
        var actions = new ArrayList<io.delta.standalone.actions.Action>(staged.files());
        if (staged.key() != null) actions.add(new io.delta.standalone.actions.SetTransaction(
            staged.key(), 0, java.util.Optional.of(System.currentTimeMillis())));
        commitLock.lock();
        try {
            for (int attempt = 1; ; attempt++) {
                OptimisticTransaction txn;
                try {
                    txn = log.startTransaction();
                    // Every file already in the table is read, so a concurrent append from another
                    // process fails this commit instead of passing unseen.
                    txn.readWholeTable();
                    if (staged.key() != null && txn.txnVersion(staged.key()) >= 0) {
                        deleteFiles(conf, staged.files(), null);
                        return new WriteResult(0, true);
                    }
                    verifyGuard(log, txn.readVersion());
                    if (txn.readVersion() < 0) {
                        txn.updateMetadata(new Metadata(
                            java.util.UUID.randomUUID().toString(),
                            null,
                            null,
                            new io.delta.standalone.actions.Format("parquet", Map.of()),
                            List.of(),
                            Map.of(),
                            java.util.Optional.of(System.currentTimeMillis()),
                            staged.schema()));
                    } else if (!txn.metadata().getSchema().equals(staged.schema())) {
                        throw new IOException("the table schema changed while the shard was being written");
                    }
                    beforeCommit.run();
                } catch (IOException | RuntimeException | FSError beforeLogWrite) {
                    deleteFiles(conf, staged.files(), beforeLogWrite);
                    throw beforeLogWrite;
                }
                try {
                    txn.commit(actions, new Operation(Operation.Name.WRITE), "gefrierschrank dump");
                    return new WriteResult(staged.rows(), false);
                } catch (io.delta.standalone.exceptions.DeltaConcurrentModificationException conflict) {
                    if (attempt < COMMIT_ATTEMPTS) continue;
                    deleteFiles(conf, staged.files(), conflict);
                    throw conflict;
                } catch (FSError storage) {
                    return resolveUnknownOutcome(conf, log, staged, targetStorageFailure(storage), false);
                } catch (Exception unknownOutcome) {
                    // A local buffer failure happened before any upload, so the entry cannot land later.
                    boolean buffer = diskError(unknownOutcome) != null;
                    return resolveUnknownOutcome(conf, log, staged,
                        buffer && !runRootEnded.getAsBoolean() ? targetStorageFailure(unknownOutcome) : unknownOutcome, !buffer);
                }
            }
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * The commit failed after it may have written its log entry: the log decides. A storage
     * failure of the target ({@code mayLandLater} false) happened in this thread's own synchronous
     * write, so an entry absent now can never appear later and the files go at once.
     */
    private WriteResult resolveUnknownOutcome(Configuration conf, DeltaLog log, StagedShard staged, Exception failure,
                                              boolean mayLandLater) throws IOException {
        if (staged.key() == null) throw rethrowable(failure);
        Set<String> committed;
        try {
            committed = pathsByKey(log.update()).getOrDefault(staged.key(), Set.of());
        } catch (Exception | FSError refreshFailure) {
            // Unknown whether the log references the files: reconcilePendingCleanup decides once
            // the log is readable again.
            failure.addSuppressed(refreshFailure);
            pendingCleanup.add(staged);
            throw rethrowable(failure);
        }
        if (!committed.isEmpty() && committed.containsAll(staged.paths())) {
            System.err.println("warning: shard commit reported " + failure.getClass().getSimpleName()
                + " after its log entry was written; the shard is committed");
            return new WriteResult(staged.rows(), false);
        }
        // Not ours (absent, or committed by another writer). A late write could still land, so
        // deletion waits for reconcilePendingCleanup.
        if (mayLandLater) pendingCleanup.add(staged);
        else deleteFiles(conf, staged.files(), failure);
        if (committed.isEmpty()) throw rethrowable(failure);
        return new WriteResult(0, true);
    }

    /**
     * Deletes the files of commits that failed with an unknown outcome, once the log shows they
     * never landed. Called at the end of each index run; best effort, and a crash before it leaves
     * the files behind as unreferenced orphans.
     */
    public void reconcilePendingCleanup(Configuration conf) {
        commitLock.lock();
        try {
            if (pendingCleanup.isEmpty()) return;
            Map<String, Set<String>> committed;
            try {
                committed = pathsByKey(log(conf).update());
            } catch (Exception | FSError refreshFailure) {
                System.err.println("warning: cannot re-read the Delta log to clean up after a failed commit ("
                    + refreshFailure.getClass().getSimpleName() + "); its Parquet files are kept");
                return;
            }
            for (StagedShard staged : pendingCleanup) {
                if (!java.util.Collections.disjoint(committed.getOrDefault(staged.key(), Set.of()), staged.paths())) continue;
                var failure = new IOException("cleanup after a failed commit");
                deleteFiles(conf, staged.files(), failure);
                if (failure.getSuppressed().length > 0) {
                    System.err.println("warning: could not delete Parquet files of a failed commit ("
                        + failure.getSuppressed()[0].getClass().getSimpleName() + ")");
                }
            }
            pendingCleanup.clear();
        } finally {
            commitLock.unlock();
        }
    }

    private static Map<String, Set<String>> pathsByKey(io.delta.standalone.Snapshot snapshot) {
        Map<String, Set<String>> paths = new java.util.HashMap<>();
        if (snapshot.getVersion() < 0) return paths;
        for (AddFile file : snapshot.getAllFiles()) {
            String key = file.getTags() == null ? null : file.getTags().get(ShardKey.TAG);
            if (key != null) paths.computeIfAbsent(key, k -> new java.util.HashSet<>()).add(file.getPath());
        }
        return paths;
    }

    private void deleteFiles(Configuration conf, List<AddFile> files, Throwable failure) {
        for (AddFile file : files) {
            Path path = new Path(tableUri + "/" + file.getPath());
            try {
                path.getFileSystem(conf).delete(path, false);
            } catch (Exception | FSError cleanupFailure) {
                if (failure != null) failure.addSuppressed(cleanupFailure);
            }
        }
    }

    private static IOException rethrowable(Exception failure) {
        if (failure instanceof IOException io) return io;
        if (failure instanceof RuntimeException runtime) throw runtime;
        // delta-standalone throws checked exceptions undeclared, e.g. from lockInterruptibly.
        if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
        return new IOException("commit failed", failure);
    }

    /**
     * One DeltaLog per writer, so every shard reuses the replayed log state instead of loading the
     * table again. A caller passing another Configuration gets a DeltaLog built from it.
     */
    private synchronized DeltaLog log(Configuration conf) {
        if (deltaLog == null || deltaLogConf != conf) {
            deltaLog = DeltaLog.forTable(conf, tableUri);
            deltaLogConf = conf;
        }
        return deltaLog;
    }

    /**
     * The same table must map to the same lock whether it is named as a path, a URI or through a
     * symlink (macOS: /tmp is /private/tmp).
     */
    static String lockKey(String tableUri) {
        java.net.URI uri = new Path(tableUri).toUri();
        String scheme = uri.getScheme() == null ? "file" : uri.getScheme().toLowerCase(java.util.Locale.ROOT);
        String path = uri.getPath();
        if (scheme.equals("file")) path = canonicalLocalPath(path);
        return scheme + "://" + (uri.getAuthority() == null ? "" : uri.getAuthority()) + path;
    }

    private static String canonicalLocalPath(String path) {
        var file = new java.io.File(path).getAbsoluteFile();
        try {
            // Resolves the symlinks of the longest existing prefix; the table may not exist yet.
            return file.getCanonicalPath();
        } catch (IOException unresolvable) {
            return file.toPath().normalize().toString();
        }
    }

    /**
     * Shard keys (see {@link ShardKey}) already committed to this table, read directly from the
     * Delta transaction log - the log is the single source of truth for "what's committed", so
     * resuming a crashed run needs no separate checkpoint file. Empty if the table doesn't exist
     * yet (first run ever).
     */
    Set<String> committedShardKeys(Configuration hadoopConf) {
        validateOutputSettings(hadoopConf);
        try {
            return onTarget(() -> new java.util.HashSet<>(pathsByKey(log(hadoopConf).update()).keySet()));
        } catch (IOException storage) {
            // Only a translated storage failure is checked here; the signature stays unchecked.
            throw new java.io.UncheckedIOException(storage);
        }
    }

    /**
     * Every AddFile this writer stages from now on also carries {@code extra}; called once per run,
     * before the first write. {@link ShardKey#TAG} stays the writer's own.
     */
    DeltaTableWriter withFileTags(Map<String, String> extra) {
        if (extra.containsKey(ShardKey.TAG)) throw new IllegalArgumentException("the shard key tag is the writer's own");
        this.fileTags = Map.copyOf(extra);
        return this;
    }

    /**
     * Fails when a committed AddFile of a shard ({@link ShardKey#TAG}) lacks the tag
     * {@code key=value}: the table holds rows written without what that tag stands for. An empty
     * or missing table passes.
     */
    void requireTaggedFiles(Configuration conf, String key, String value) throws IOException {
        validateOutputSettings(conf);
        boolean untagged = onTarget(() -> {
            io.delta.standalone.Snapshot snapshot = log(conf).update();
            if (snapshot.getVersion() < 0) return false;
            for (AddFile file : snapshot.getAllFiles()) {
                Map<String, String> tags = file.getTags();
                if (tags != null && tags.containsKey(ShardKey.TAG) && !value.equals(tags.get(key))) return true;
            }
            return false;
        });
        if (untagged) throw new VectorlessTableException();
    }

    public static final class ArchiveTargetConflictException extends IOException {
        public ArchiveTargetConflictException() {
            super("Target contains a legacy export or a different snapshot; use a new output location for a complete latest-state export");
        }
    }

    /**
     * Fails unless the table is absent or holds only this archive snapshot's shards with the archive
     * schema. On success, arms this writer: every later commit through it first verifies the table
     * versions added since, atomically with that commit, so a concurrent export of another snapshot
     * cannot mix into the table.
     */
    ArchiveGuard requireArchiveSnapshot(Configuration conf, ArchiveContext context, int shards) throws IOException {
        validateOutputSettings(conf);
        var expected = new java.util.HashSet<String>();
        for (int shard = 0; shard < shards; shard++) expected.add(context.key(shard));
        var armed = new ArchiveGuard(expected, archiveSchema());
        return onTarget(() -> {
            DeltaLog log = log(conf);
            commitLock.lock();
            try {
                armed.verify(log.update());
                guard = armed;
            } finally {
                commitLock.unlock();
            }
            return armed;
        });
    }

    private void verifyGuard(DeltaLog log, long version) throws ArchiveTargetConflictException {
        ArchiveGuard armed = guard;
        if (armed == null) return;
        commitLock.lock();
        try {
            armed.verifyThrough(log, version);
        } finally {
            commitLock.unlock();
        }
    }

    /**
     * Rejects invalid output settings before any table or shard work, so a typo fails the index
     * once instead of quarantining every shard.
     */
    static void validateOutputSettings(Configuration conf) {
        usableParquetCodec(conf);
        targetFileBytes(conf);
    }

    /**
     * The configured codec, if it runs in this JVM. A native library that cannot load would
     * otherwise surface as a LinkageError, which is an Error and not quarantined, in the middle
     * of the first shard.
     */
    private static CompressionCodecName usableParquetCodec(Configuration conf) {
        CompressionCodecName codec = parquetCodec(conf);
        String failure = parquetCodecFailure(codec);
        if (failure != null) {
            throw new IllegalStateException("Parquet codec " + codec.name().toLowerCase(java.util.Locale.ROOT)
                + " cannot run in this JVM (" + failure + "): its native library could not be loaded; set "
                + PARQUET_CODEC_KEY + "=uncompressed or give the JVM an executable java.io.tmpdir");
        }
        return codec;
    }

    /** Validated {@link #PARQUET_CODEC_KEY}; unset means {@link #defaultParquetCodec()}. */
    public static CompressionCodecName parquetCodec(Configuration conf) {
        String value = setting(conf, PARQUET_CODEC_KEY);
        if (value == null) return defaultParquetCodec();
        return switch (value.toLowerCase(java.util.Locale.ROOT)) {
            case "zstd" -> CompressionCodecName.ZSTD;
            case "snappy" -> CompressionCodecName.SNAPPY;
            case "uncompressed" -> CompressionCodecName.UNCOMPRESSED;
            default -> throw new IllegalArgumentException(PARQUET_CODEC_KEY + " must be zstd, snappy or uncompressed");
        };
    }

    /**
     * ZSTD, or UNCOMPRESSED (the 1.0.0 layout) where zstd-jni cannot load its native library in
     * this JVM, e.g. from a noexec java.io.tmpdir: a host that can read the repository can still
     * export it. Decided once per JVM, with one warning on stderr when it falls back.
     */
    public static CompressionCodecName defaultParquetCodec() {
        return DefaultCodec.CODEC;
    }

    private static final class DefaultCodec {
        static final CompressionCodecName CODEC = defaultCodec(parquetCodecFailure(CompressionCodecName.ZSTD));
    }

    static CompressionCodecName defaultCodec(String zstdFailure) {
        if (zstdFailure == null) return CompressionCodecName.ZSTD;
        System.err.println("warning: Parquet ZSTD compression unavailable (" + zstdFailure + "); writing uncompressed "
            + "Parquet. For ZSTD, give the JVM an executable temp directory with -DZstdTempFolder=<dir> "
            + "or -Djava.io.tmpdir=<dir>");
        return CompressionCodecName.UNCOMPRESSED;
    }

    private static final ConcurrentHashMap<CompressionCodecName, Optional<String>> CODEC_FAILURES = new ConcurrentHashMap<>();

    /**
     * Null if {@code codec} compresses in this JVM, otherwise the simple class name of the reason:
     * zstd-jni and snappy-java unpack their native library into a temp directory, which may be
     * mounted noexec. Probed once per codec, because a failed native load stays failed in a JVM.
     */
    public static String parquetCodecFailure(CompressionCodecName codec) {
        return CODEC_FAILURES.computeIfAbsent(codec, DeltaTableWriter::probeCodec).orElse(null);
    }

    private static Optional<String> probeCodec(CompressionCodecName codec) {
        var factory = new CodecFactory(new Configuration(false), 64 * 1024);
        try {
            factory.getCompressor(codec).compress(BytesInput.from(new byte[]{1}));
            return Optional.empty();
        } catch (VirtualMachineError fatal) {
            throw fatal;
        } catch (Throwable unusable) {
            return Optional.of(unusable.getClass().getSimpleName());
        } finally {
            factory.release();
        }
    }

    /** Validated {@link #TARGET_FILE_BYTES_KEY}. */
    static long targetFileBytes(Configuration conf) {
        String value = setting(conf, TARGET_FILE_BYTES_KEY);
        if (value == null) return DEFAULT_TARGET_FILE_BYTES;
        long bytes;
        try {
            bytes = org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix.string2long(value);
        } catch (RuntimeException invalid) {
            bytes = 0;
        }
        if (bytes <= 0) throw new IllegalArgumentException(TARGET_FILE_BYTES_KEY + " must be a positive byte count");
        return bytes;
    }

    private static String setting(Configuration conf, String key) {
        String value = conf.getTrimmed(key);
        if (value == null) value = System.getProperty(key);
        return value == null ? null : value.trim();
    }

    static StructType archiveSchema() {
        var mapper = new JsonSchemaMapper();
        mapper.inferFromEsMapping(ArchiveContext.mapping());
        return mapper.deltaSchema();
    }

    /** The Parquet layout of every data file; package-private so tests can write reference files. */
    static ParquetWriter<GenericRecord> parquetWriter(Path file, Schema schema, Configuration conf,
                                                     CompressionCodecName codec) throws IOException {
        return parquetWriter(HadoopOutputFile.fromPath(file, conf), schema, conf, codec);
    }

    private static ParquetWriter<GenericRecord> parquetWriter(OutputFile file, Schema schema, Configuration conf,
                                                              CompressionCodecName codec) throws IOException {
        // parquet-mr checks page and row-group sizes after a number of rows it estimates from the
        // rows so far (the first after 100): after many small rows, a run of multi-MB documents
        // would wait uncompressed in one page and one row group, to be compressed all at once
        // outside any document's heap reservation (DocumentMemoryGate). Checking after every row
        // costs about 0.2 us per document.
        return AvroParquetWriter.<GenericRecord>builder(file)
            .withSchema(schema)
            .withConf(conf)
            .withCompressionCodec(codec)
            .withMinRowCountForPageSizeCheck(1)
            .withMaxRowCountForPageSizeCheck(1)
            .withRowGroupSize(ROW_GROUP_BYTES)
            .build();
    }

    /** One Parquet file, appended until its written size reaches the target. */
    private static final class RollingParquetFile {
        private final ParquetWriter<GenericRecord> writer;
        private final JsonSchemaMapper inferrer;
        private final Schema avroSchema;
        private final Path filePath;
        private final Configuration conf;
        private final TrackedOutputFile output;
        private final boolean compressed;
        // Archive rows only: where the last row group ended, the largest _source of the open row group,
        // the sum of the largest _source of every flushed row group, the largest _source of the file,
        // and the largest flushed row group.
        private long flushedBytes;
        private long groupMaxBytes;
        private long flushedGroupsMaxBytes;
        private long maxSourceBytes;
        private long maxFlushedGroupBytes;

        RollingParquetFile(Configuration conf, String tableUri, String fileKey,
                           JsonSchemaMapper inferrer, CompressionCodecName codec) throws IOException {
            this.conf = conf;
            this.filePath = new Path(tableUri + "/" + fileKey + ".parquet");
            this.inferrer = inferrer;
            this.avroSchema = inferrer.avroSchema();
            this.output = new TrackedOutputFile(HadoopOutputFile.fromPath(filePath, conf));
            this.writer = parquetWriter(output, avroSchema, conf, codec);
            this.compressed = codec != CompressionCodecName.UNCOMPRESSED;
            this.flushedBytes = output.position();
        }

        void append(JsonNode doc) throws IOException {
            GenericRecord record = new GenericData.Record(avroSchema);
            for (Schema.Field field : avroSchema.getFields()) {
                Object value = field.name().equals(JsonSchemaMapper.UNMAPPED_FIELDS_FIELD)
                    ? inferrer.unmappedFieldsJson(doc)
                    : JsonSchemaMapper.avroValue(doc.get(inferrer.jsonFieldNameFor(field.name())), field.schema());
                record.put(field.name(), value instanceof String text ? AvroStrings.utf8(text) : value);
            }
            writer.write(record);
        }

        private int[] archivePositions;

        void append(ArchiveRow row, ArchiveColumns columns) throws IOException {
            if (archivePositions == null) archivePositions = ArchiveColumns.positions(avroSchema);
            writer.write(columns.record(avroSchema, archivePositions, row));
            groupMaxBytes = Math.max(groupMaxBytes, row.length());
            maxSourceBytes = Math.max(maxSourceBytes, row.length());
            // parquet-mr writes to the stream only when it flushes a row group, which includes this row.
            long position = output.position();
            if (position != flushedBytes) {
                maxFlushedGroupBytes = Math.max(maxFlushedGroupBytes, position - flushedBytes);
                flushedBytes = position;
                flushedGroupsMaxBytes += groupMaxBytes;
                groupMaxBytes = 0;
            }
        }

        /**
         * Heap this file keeps for the archive rows written so far until it is closed, beyond the one
         * row group {@link ShardRanges#PER_RANGE_HEAP_BYTES} allows for: parquet-mr copies the
         * smallest and largest value of every row group into its statistics and keeps them for the
         * footer (at most twice the largest source per row group); a compressing codec's output buffer
         * grows to the largest compressed page, up to twice its size, and stays (sizes are checked
         * after every row, so a page holds at most one large source, and it is no larger than its row
         * group); and buffered pages beyond one row group, which only the last row can add.
         */
        long retainedBytes() {
            long buffered = Math.max(0, writer.getDataSize() - flushedBytes);
            long largestPage = Math.min(maxSourceBytes, Math.max(maxFlushedGroupBytes, buffered));
            return Math.max(0, buffered - ROW_GROUP_BYTES) + 2 * (flushedGroupsMaxBytes + groupMaxBytes)
                + (compressed ? 2 * largestPage : 0);
        }

        /** Bytes of finished row groups plus buffered pages; no allocation. */
        long dataSize() {
            return writer.getDataSize();
        }

        AddFile close(Map<String, String> tags) throws IOException {
            writer.close();
            return new AddFile(
                filePath.getName(),
                Map.of(),
                filePath.getFileSystem(conf).getFileStatus(filePath).getLen(),
                System.currentTimeMillis(),
                true,
                null,
                tags);
        }
    }

    /** Keeps the stream parquet-mr writes to, so its position shows when a row group was flushed. */
    private static final class TrackedOutputFile implements OutputFile {
        private final OutputFile file;
        private PositionOutputStream stream;

        TrackedOutputFile(OutputFile file) {
            this.file = file;
        }

        long position() throws IOException {
            return stream == null ? 0 : stream.getPos();
        }

        @Override public PositionOutputStream create(long blockSizeHint) throws IOException {
            return stream = file.create(blockSizeHint);
        }

        @Override public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return stream = file.createOrOverwrite(blockSizeHint);
        }

        @Override public boolean supportsBlockSize() {
            return file.supportsBlockSize();
        }

        @Override public long defaultBlockSize() {
            return file.defaultBlockSize();
        }

        @Override public String getPath() {
            return file.getPath();
        }
    }
}
