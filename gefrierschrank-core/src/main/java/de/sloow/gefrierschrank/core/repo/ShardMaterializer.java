package de.sloow.gefrierschrank.core.repo;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * Reassembles the on-disk Lucene directory of one shard snapshot: downloads
 * obfuscated {@code __<uuid>} blobs under their physical names and decodes
 * {@code v__<uuid>} entries (full file content embedded as base64 {@code meta_hash}).
 *
 * <p>Every file is verified against the snapshot metadata ({@link LuceneFooterVerifier}) while
 * its bytes stream in, so the directory is fully verified when {@link #materialize} returns and
 * no second read pass over the local copy is needed. {@link #begin} splits the same download into
 * rounds ({@link ShardFetch}), each verifying what it downloads in the same way.
 *
 * <p>With a concurrency above 1, the shard downloads on a per-call pool of that many threads.
 * A single-blob file up to {@value #RANGE_THRESHOLD_BYTES} bytes is one task. The parts of a
 * chunked file are separate tasks, and a part (or single blob) above that size is fetched as
 * {@value #RANGE_BYTES}-byte ranges pinned to the blob version its first range came from, so one
 * huge {@code .fdt} is not limited to a single stream. Pieces are written at their file position
 * and verified as runs combined in file order. Every task has finished, successfully or not,
 * before {@link #materialize} returns or throws, so callers may delete the directory right after
 * a failure. An Error from any task is thrown, even after another task failed first. Concurrent
 * calls for different directories are independent.
 */
public final class ShardMaterializer {

    /** Download streams per shard; 1 downloads file after file on the calling thread. */
    public static final String CONCURRENCY_PROPERTY = "gefrierschrank.download.concurrency";
    public static final int DEFAULT_CONCURRENCY = 8;
    public static final int MAX_CONCURRENCY = 32;

    static final int COPY_BUFFER_BYTES = 1 << 20;
    static final long RANGE_THRESHOLD_BYTES = 64L << 20;
    static final int RANGE_BYTES = 16 << 20;

    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger();
    private static final ThreadFactory DOWNLOAD_THREADS = task -> {
        var thread = new Thread(task, "gefrierschrank-download-" + THREAD_COUNTER.incrementAndGet());
        thread.setDaemon(true);
        return thread;
    };

    private final BlobStore blobs;
    private final int concurrency;
    private final long rangeThreshold;
    private final int rangeBytes;

    public ShardMaterializer(BlobStore blobs) {
        this(blobs, configuredConcurrency());
    }

    /** The system property, held to the CLI's 1 to {@value #MAX_CONCURRENCY}: a stray -D value cannot fail every shard. */
    static int configuredConcurrency() {
        return Math.max(1, Math.min(MAX_CONCURRENCY, Integer.getInteger(CONCURRENCY_PROPERTY, DEFAULT_CONCURRENCY)));
    }

    public ShardMaterializer(BlobStore blobs, int concurrency) {
        this(blobs, concurrency, RANGE_THRESHOLD_BYTES, RANGE_BYTES);
    }

    /** Test seam: parts above {@code rangeThreshold} bytes download as {@code rangeBytes} ranges. */
    ShardMaterializer(BlobStore blobs, int concurrency, long rangeThreshold, int rangeBytes) {
        if (concurrency < 1) throw new IllegalArgumentException("download concurrency must be at least 1");
        if (rangeThreshold < 0 || rangeBytes < 1) throw new IllegalArgumentException("invalid download range size");
        this.blobs = blobs;
        this.concurrency = concurrency;
        this.rangeThreshold = rangeThreshold;
        this.rangeBytes = rangeBytes;
    }

    record Target(RepoFileInfo file, Path path) {
        boolean embedded() {
            return file.blobName().startsWith("v__");
        }
    }

    /** Cooperative cancellation of one materialize call's downloads. */
    private static final class Downloads {
        final AtomicBoolean cancelled = new AtomicBoolean();
        final Set<Closeable> openStreams = ConcurrentHashMap.newKeySet();
        final LongAdder arrived;

        Downloads(LongAdder arrived) {
            this.arrived = arrived;
        }

        void cancel() {
            cancelled.set(true);
            for (Closeable stream : openStreams) {
                try {
                    stream.close();
                } catch (IOException | RuntimeException ignored) {
                    // the task owning it fails and closes it again
                }
            }
        }

        void checkCancelled() {
            if (cancelled.get()) throw new CancellationException("shard download cancelled");
        }
    }

    /**
     * Downloads the shard into {@code targetDir}, which is created if missing; its parent must
     * exist. Creating missing parents would recreate a temp run root that the exiting process has
     * just deleted, and fill it with a whole shard that nothing deletes any more. Returns the bytes
     * downloaded, embedded files excluded.
     */
    public long materialize(ShardSnapshot shard, Path targetDir) throws IOException {
        return begin(shard, targetDir).fetchAll();
    }

    /**
     * Prepares a download of the shard into {@code targetDir} (created as by {@link #materialize})
     * that the returned {@link ShardFetch} performs in one or more rounds. Every file name of the
     * shard is validated here, also the names of files no round will download.
     */
    public ShardFetch begin(ShardSnapshot shard, Path targetDir) throws IOException {
        if (!Files.isDirectory(targetDir)) Files.createDirectory(targetDir);
        return new ShardFetch(this, shard.shardPrefix(), resolveTargets(shard, targetDir));
    }

    /** One round of a {@link ShardFetch}: downloads and verifies {@code targets}, adding the bytes that arrive to {@code arrived}. */
    void fetch(String shardPrefix, List<Target> targets, LongAdder arrived) throws IOException {
        List<Target> downloads = targets.stream().filter(t -> !t.embedded()).toList();
        long pieces = 0;
        for (Target target : downloads) pieces += pieces(target.file());
        int threads = (int) Math.min(concurrency, pieces);
        if (threads <= 1) {
            byte[] buffer = new byte[COPY_BUFFER_BYTES];
            var sequential = new Downloads(arrived);
            for (Target target : targets) {
                if (target.embedded()) writeEmbedded(target);
                else download(shardPrefix, target, buffer, sequential);
            }
            return;
        }
        for (Target target : targets) {
            if (target.embedded()) writeEmbedded(target);
        }
        downloadConcurrently(shardPrefix, downloads, threads, arrived);
    }

    private static List<Target> resolveTargets(ShardSnapshot shard, Path targetDir) throws IOException {
        var names = new HashSet<String>();
        var targets = new ArrayList<Target>();
        for (RepoFileInfo file : shard.files()) {
            Path physical = Path.of(file.physicalName());
            if (physical.isAbsolute() || physical.getNameCount() != 1 || file.physicalName().equals("..")
                || file.physicalName().equals(".") || !names.add(file.physicalName())) {
                throw new IOException("invalid physical shard filename");
            }
            targets.add(new Target(file, targetDir.resolve(physical)));
        }
        return targets;
    }

    /** Verified before it is written, so corrupt embedded content never reaches the directory. */
    private static void writeEmbedded(Target target) throws IOException {
        RepoFileInfo file = target.file();
        if (file.metaHash() == null) throw new IOException("virtual shard file has no embedded content");
        byte[] content = Base64.getDecoder().decode(file.metaHash());
        var verifier = new LuceneFooterVerifier(file.length(), file.checksum());
        verifier.update(0, content);
        verifier.finish();
        Files.write(target.path(), content);
        requireLength(target);
    }

    /** Whether the file downloads as independent parts or ranges instead of one sequential stream. */
    private boolean split(RepoFileInfo file) {
        return concurrency > 1 && (file.partCount() > 1 || file.length() > rangeThreshold);
    }

    private long pieces(RepoFileInfo file) {
        if (!split(file)) return 1;
        long pieces = 0;
        for (int part = 0; part < file.partCount(); part++) {
            long length = partLength(file, part);
            pieces += length > rangeThreshold ? (length + rangeBytes - 1) / rangeBytes : 1;
        }
        return pieces;
    }

    private static long partStart(RepoFileInfo file, int part) {
        return file.partCount() == 1 ? 0 : part * file.partSize();
    }

    private static long partLength(RepoFileInfo file, int part) {
        return file.partCount() == 1 ? file.length() : Math.min(file.partSize(), file.length() - partStart(file, part));
    }

    /** A file downloaded as separate pieces: one verifier sees every piece at its file position. */
    private record SplitFile(Target target, LuceneFooterVerifier verifier) {}

    private record Piece(long bytes, Callable<Void> task) {}

    /** Hands a task to the running pool; a leader task schedules the remaining ranges of its part. */
    private interface Scheduler {
        void submit(Callable<Void> task);
    }

    /**
     * The tasks of one concurrent materialize call, counted and failed without allocating when a
     * task ends. A completion queue allocates a node per finished task: an OutOfMemoryError there
     * lost that task's end, and the waiting thread waited for it forever.
     */
    private static final class Tasks {
        private static final long PARK_NANOS = TimeUnit.SECONDS.toNanos(1);

        private final Thread waiter = Thread.currentThread();
        private final AtomicInteger unfinished = new AtomicInteger();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        /**
         * The task as the pool runs it, counted before it can run: the waiter must never see every
         * task ended while a leader is still scheduling ranges.
         */
        Runnable track(Callable<Void> task) {
            Runnable tracked = () -> {
                try {
                    task.call();
                } catch (Throwable failed) {
                    fail(failed);
                } finally {
                    end();
                }
            };
            unfinished.incrementAndGet();
            return tracked;
        }

        /** Ends a task, also one the pool never ran. */
        void end() {
            if (unfinished.decrementAndGet() == 0) LockSupport.unpark(waiter);
        }

        /** Keeps the first failure, unless a later one is an Error: an exception never hides an Error. */
        private void fail(Throwable failed) {
            while (true) {
                Throwable current = failure.get();
                if (current != null && (current instanceof Error || !(failed instanceof Error))) return;
                if (failure.compareAndSet(current, failed)) break;
            }
            LockSupport.unpark(waiter);
        }

        Throwable failure() {
            return failure.get();
        }

        /**
         * Waits until every task has ended or, with {@code untilFailure}, one has failed or this
         * thread is interrupted. Returns whether it was interrupted.
         */
        boolean await(boolean untilFailure) {
            boolean interrupted = false;
            while (unfinished.get() > 0 && !(untilFailure && (interrupted || failure.get() != null))) {
                LockSupport.parkNanos(this, PARK_NANOS);
                interrupted |= Thread.interrupted();
            }
            return interrupted;
        }
    }

    private void downloadConcurrently(String shardPrefix, List<Target> downloads, int threads, LongAdder arrived)
            throws IOException {
        var state = new Downloads(arrived);
        ThreadLocal<byte[]> buffers = ThreadLocal.withInitial(() -> new byte[COPY_BUFFER_BYTES]);
        ExecutorService pool = Executors.newFixedThreadPool(threads, DOWNLOAD_THREADS);
        var tasks = new Tasks();
        Scheduler scheduler = task -> {
            Runnable tracked = tasks.track(task);
            try {
                pool.execute(tracked);
            } catch (Throwable notQueued) {
                tasks.end();
                throw notQueued;
            }
        };
        var splitFiles = new ArrayList<SplitFile>();
        Throwable failure = null;
        boolean interrupted = false;
        try {
            var pieces = new ArrayList<Piece>();
            for (Target target : downloads) {
                RepoFileInfo file = target.file();
                if (!split(file)) {
                    pieces.add(new Piece(file.length(), () -> {
                        download(shardPrefix, target, buffers.get(), state);
                        return null;
                    }));
                    continue;
                }
                var splitFile = new SplitFile(target, new LuceneFooterVerifier(file.length(), file.checksum()));
                splitFiles.add(splitFile);
                FileChannel.open(target.path(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE).close();
                for (int part = 0; part < file.partCount(); part++) {
                    String key = shardPrefix + file.partName(part);
                    long start = partStart(file, part);
                    long length = partLength(file, part);
                    pieces.add(new Piece(length, length > rangeThreshold
                        ? () -> {
                            downloadRangedPart(key, splitFile, start, length, buffers, state, scheduler);
                            return null;
                        }
                        : () -> {
                            downloadPart(key, splitFile, start, length, buffers.get(), state);
                            return null;
                        }));
                }
            }
            // Largest first, so one big .fdt does not start last and stretch the whole shard.
            pieces.sort(Comparator.comparingLong(Piece::bytes).reversed());
            for (Piece piece : pieces) scheduler.submit(piece.task());
            interrupted = tasks.await(true);
            if (interrupted && tasks.failure() == null) {
                failure = new InterruptedIOException("interrupted while downloading shard files");
            }
        } catch (IOException | RuntimeException | Error e) {
            failure = e;
        } finally {
            if (failure != null || tasks.failure() != null) {
                state.cancel();
                for (Runnable neverRun : pool.shutdownNow()) tasks.end();
            } else {
                pool.shutdown();
            }
            interrupted |= tasks.await(false);
            if (interrupted) Thread.currentThread().interrupt();
        }
        Throwable taskFailure = tasks.failure();
        if (failure == null || taskFailure instanceof Error) failure = taskFailure;
        if (failure == null) {
            for (SplitFile splitFile : splitFiles) {
                splitFile.verifier().finish();
                requireLength(splitFile.target());
            }
            return;
        }
        if (failure instanceof IOException io) throw io;
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
        throw new IOException("shard file download failed", failure);
    }

    /** One file as a single sequential stream over its parts, verified when it ends. */
    private void download(String shardPrefix, Target target, byte[] buffer, Downloads state) throws IOException {
        RepoFileInfo file = target.file();
        var verifier = new LuceneFooterVerifier(file.length(), file.checksum());
        try (FileChannel out = FileChannel.open(target.path(),
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            long position = 0;
            for (int part = 0; part < file.partCount(); part++) {
                state.checkCancelled();
                try (InputStream in = blobs.read(shardPrefix + file.partName(part))) {
                    state.openStreams.add(in);
                    try {
                        int count;
                        while (true) {
                            state.checkCancelled();
                            if ((count = in.readNBytes(buffer, 0, buffer.length)) <= 0) break;
                            verifier.update(position, buffer, 0, count);
                            ByteBuffer chunk = ByteBuffer.wrap(buffer, 0, count);
                            while (chunk.hasRemaining()) out.write(chunk);
                            position += count;
                            state.arrived.add(count);
                        }
                    } finally {
                        state.openStreams.remove(in);
                    }
                }
            }
        }
        verifier.finish();
        requireLength(target);
    }

    /** One part blob of a split file, read whole; a blob longer than the metadata part fails. */
    private void downloadPart(String key, SplitFile file, long start, long length, byte[] buffer, Downloads state)
            throws IOException {
        state.checkCancelled();
        try (InputStream in = blobs.read(key);
             FileChannel out = FileChannel.open(file.target().path(), StandardOpenOption.WRITE)) {
            state.openStreams.add(in);
            try {
                copy(in, out, file.verifier(), start, length, buffer, state);
                if (in.read() >= 0) throw new IOException(LuceneFooterVerifier.LENGTH_MISMATCH);
            } finally {
                state.openStreams.remove(in);
            }
        }
    }

    /**
     * A part above the range threshold. Its first range fixes the blob version and length; the
     * remaining ranges are scheduled pinned to that version, so ranges of different blob contents
     * are never stitched into one file. A store that cannot name a version gets the part read
     * whole instead.
     */
    private void downloadRangedPart(String key, SplitFile file, long start, long length, ThreadLocal<byte[]> buffers,
                                    Downloads state, Scheduler scheduler) throws IOException {
        state.checkCancelled();
        long firstLength = Math.min(rangeBytes, length);
        BlobStore.RangeRead first = blobs.openRange(key, 0, firstLength, null);
        if (first.version() == null || first.objectLength() < 0) {
            first.close();
            downloadPart(key, file, start, length, buffers.get(), state);
            return;
        }
        try (first; FileChannel out = FileChannel.open(file.target().path(), StandardOpenOption.WRITE)) {
            state.openStreams.add(first);
            try {
                if (first.objectLength() != length) throw new IOException(LuceneFooterVerifier.LENGTH_MISMATCH);
                String version = first.version();
                for (long offset = firstLength; offset < length; offset += rangeBytes) {
                    long rangeOffset = offset;
                    long rangeLength = Math.min(rangeBytes, length - offset);
                    scheduler.submit(() -> {
                        downloadRange(key, version, file, start, length, rangeOffset, rangeLength, buffers.get(), state);
                        return null;
                    });
                }
                copy(first.in(), out, file.verifier(), start, firstLength, buffers.get(), state);
            } finally {
                state.openStreams.remove(first);
            }
        }
    }

    private void downloadRange(String key, String version, SplitFile file, long partStart, long partLength,
                               long offset, long length, byte[] buffer, Downloads state) throws IOException {
        state.checkCancelled();
        try (BlobStore.RangeRead range = blobs.openRange(key, offset, length, version);
             FileChannel out = FileChannel.open(file.target().path(), StandardOpenOption.WRITE)) {
            state.openStreams.add(range);
            try {
                // Checked here as well, for a store that ignores the expected version.
                if (!version.equals(range.version())) throw new IOException(BlobStore.BLOB_CHANGED);
                if (range.objectLength() != partLength) throw new IOException(LuceneFooterVerifier.LENGTH_MISMATCH);
                copy(range.in(), out, file.verifier(), partStart + offset, length, buffer, state);
            } finally {
                state.openStreams.remove(range);
            }
        }
    }

    /** Exactly {@code length} bytes of {@code in} to file position {@code position}; fewer is a length mismatch. */
    private static void copy(InputStream in, FileChannel out, LuceneFooterVerifier verifier, long position, long length,
                             byte[] buffer, Downloads state) throws IOException {
        long remaining = length;
        while (remaining > 0) {
            state.checkCancelled();
            int count = in.readNBytes(buffer, 0, (int) Math.min(buffer.length, remaining));
            if (count <= 0) throw new IOException(LuceneFooterVerifier.LENGTH_MISMATCH);
            verifier.update(position, buffer, 0, count);
            ByteBuffer chunk = ByteBuffer.wrap(buffer, 0, count);
            long at = position;
            while (chunk.hasRemaining()) at += out.write(chunk, at);
            position += count;
            remaining -= count;
            state.arrived.add(count);
        }
    }

    /** Defense in depth: what reached the local disk has the length the verifier accepted. */
    private static void requireLength(Target target) throws IOException {
        if (Files.size(target.path()) != target.file().length()) {
            throw new IOException(LuceneFooterVerifier.LENGTH_MISMATCH);
        }
    }
}
