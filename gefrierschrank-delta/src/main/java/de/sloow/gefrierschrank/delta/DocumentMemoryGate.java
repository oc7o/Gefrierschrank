package de.sloow.gefrierschrank.delta;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.function.BooleanSupplier;

/**
 * Admits large documents into the heap. A document of a few hundred MiB needs a multiple of
 * its size while it is turned into a Parquet row; an OutOfMemoryError would end the run, and the
 * same shard would end every rerun. Instead, every document of at least {@link #BYPASS_BYTES}
 * reserves {@code multiplier x size} from one JVM-wide budget before it goes further, and a
 * document that can never fit fails its shard with {@link DocumentTooLargeException}, which
 * quarantines it.
 *
 * <p>The budget is the maximum heap minus {@link ShardRanges#PER_RANGE_HEAP_BYTES} for every
 * registered range and one more for everything else, and minus what each range's reader holds
 * ahead of it (a synthetic-source reader's reconstruction batch, see
 * {@link ShardRanges#syntheticReadAheadBytes}). Against it the gate charges every range:
 * <ul>
 *   <li>what its open Parquet file keeps of the large documents written to it (row-group
 *       statistics, the codec's output buffer), as the writer reports it through
 *       {@link DeltaTableWriter.OpenFileHeap}, until the file is closed;
 *   <li>its admitted document, until the range asks for its next one;
 *   <li>while it is not waiting, at least {@link #READ_MULTIPLIER} x the largest document the gate
 *       has seen: a reader has allocated a document (a stored-fields reader also its decompressed
 *       block) before the gate learns its size, so every range that may be reading needs room for
 *       one more document as large.
 * </ul>
 * A range registers before it reads its first document and waits while its reading room does not
 * fit. A document that does not fit at once first has the writer close the range's open file; if
 * it still does not fit, the caller moves it out of the heap (the pipeline writes it to a temporary
 * file) and waits holding nothing, first come, first served, until the others' charges leave room.
 * Waiting ranges hold nothing, so the first waiter always fits once every other range waits or has
 * finished. New ranges do not register while a document waits.
 *
 * <p>Not covered: documents larger than any seen before that several ranges read at the same
 * moment, before the first of them reaches the gate. Their reading room is the heap itself (see
 * {@link ShardRanges#FIRST_READ_HEAP_BYTES} and the startup warning of
 * {@link ExportSettings#resourceWarnings}).
 */
public final class DocumentMemoryGate {

    /** Documents below this size pass without a reservation: the 1 KB log line never waits. */
    static final long BYPASS_BYTES = 1L << 20;
    /** Overrides {@link #DEFAULT_RESERVE_MULTIPLIER}; read when the JVM-wide gate is created. */
    public static final String RESERVE_MULTIPLIER_PROPERTY = "gefrierschrank.document.reserveMultiplier";
    /** Measured heap per source byte on the byte row path. */
    static final double DEFAULT_RESERVE_MULTIPLIER = 6.0;
    /**
     * Heap per source byte a reader takes before it hands a document over. Lucene's merge instance
     * decompresses the document's whole block into a growing array and then copies the document out
     * of it: reading one 64 MiB document took 2.1x its size.
     */
    static final double READ_MULTIPLIER = 2.5;
    private static final long WAIT_SLICE_MILLIS = 100;

    /** A document whose reservation exceeds the whole budget; the message holds sizes only. */
    public static final class DocumentTooLargeException extends IOException {
        DocumentTooLargeException(long documentBytes, long neededBytes, long budgetBytes) {
            super("a document of " + documentBytes + " bytes needs about " + neededBytes
                + " bytes of heap, more than the " + Math.max(0, budgetBytes)
                + " bytes available for documents; raise -Xmx or lower --parallelism or --shard-threads");
        }
    }

    private final boolean enabled;
    private final long maxKiB;
    private final long perRangeKiB;
    private final double multiplier;
    private final double readMultiplier;

    // Guarded by this gate.
    private final Set<Range> ranges = Collections.newSetFromMap(new IdentityHashMap<>());
    private final ArrayDeque<Range> waiting = new ArrayDeque<>();
    private long largestKiB;
    private long readAheadKiB;
    private long largestRangeReadAheadKiB;
    private long admitted;
    private long overBudgetKiB = Long.MIN_VALUE;
    private long filesClosedForHeap;
    private long documentsWaited;
    private long registrationsWaited;

    /** @param maxMemoryBytes {@link Long#MAX_VALUE} (no heap limit) disables the gate */
    DocumentMemoryGate(long maxMemoryBytes, long perRangeBytes, double multiplier) {
        this(maxMemoryBytes, perRangeBytes, multiplier, READ_MULTIPLIER);
    }

    /** With another reading room than {@link #READ_MULTIPLIER} (tests). */
    DocumentMemoryGate(long maxMemoryBytes, long perRangeBytes, double multiplier, double readMultiplier) {
        if (!(multiplier > 0) || Double.isInfinite(multiplier))
            throw new IllegalArgumentException(RESERVE_MULTIPLIER_PROPERTY + " must be a positive number");
        if (!(readMultiplier >= 0) || Double.isInfinite(readMultiplier))
            throw new IllegalArgumentException("the read multiplier must be a number of at least 0");
        this.enabled = maxMemoryBytes != Long.MAX_VALUE;
        this.maxKiB = enabled ? maxMemoryBytes >> 10 : 0;
        this.perRangeKiB = perRangeBytes >> 10;
        this.multiplier = multiplier;
        this.readMultiplier = readMultiplier;
    }

    static DocumentMemoryGate shared() {
        return Shared.GATE;
    }

    /** Created at the first export, so validating the property alone creates nothing. */
    private static final class Shared {
        static final DocumentMemoryGate GATE = new DocumentMemoryGate(Runtime.getRuntime().maxMemory(),
            ShardRanges.PER_RANGE_HEAP_BYTES, multiplierProperty());
    }

    /**
     * {@code value} as a reserve multiplier.
     *
     * @throws IllegalArgumentException unless it is a finite number above 0
     */
    public static double reserveMultiplier(String value) {
        double parsed;
        try {
            parsed = Double.parseDouble(value.trim());
        } catch (NumberFormatException invalid) {
            parsed = Double.NaN;
        }
        if (!(parsed > 0) || Double.isInfinite(parsed))
            throw new IllegalArgumentException(RESERVE_MULTIPLIER_PROPERTY + " must be a number above 0");
        return parsed;
    }

    /** The CLI rejects an invalid value before any work; a library caller gets a warning and the default. */
    private static double multiplierProperty() {
        String value = System.getProperty(RESERVE_MULTIPLIER_PROPERTY);
        if (value == null) return DEFAULT_RESERVE_MULTIPLIER;
        try {
            return reserveMultiplier(value);
        } catch (IllegalArgumentException invalid) {
            System.err.println("warning: " + invalid.getMessage() + "; using " + DEFAULT_RESERVE_MULTIPLIER);
            return DEFAULT_RESERVE_MULTIPLIER;
        }
    }

    /** {@link #register(BooleanSupplier)} without cancellation. */
    Range register() throws IOException {
        return register(null);
    }

    /** {@link #register(BooleanSupplier, long)} for a reader that holds nothing ahead of its range. */
    Range register(BooleanSupplier cancelled) throws IOException {
        return register(cancelled, 0);
    }

    /**
     * One writing range, used by one thread at a time and closed on every path. Registers once
     * the range's reading room fits beside what the other ranges are charged, no document waits,
     * or no other range is registered. {@code readAheadBytes} is what the range's reader holds
     * ahead of it for as long as the range is registered; it comes off the budget.
     *
     * @throws InterruptedIOException if interrupted while waiting
     * @throws CancellationException  if {@code cancelled} turns true while waiting
     */
    synchronized Range register(BooleanSupplier cancelled, long readAheadBytes) throws IOException {
        long readAhead = Math.max(0, readAheadBytes + 1023) >> 10;
        if (enabled && !ranges.isEmpty() && !mayRegister(readAhead)) {
            registrationsWaited++;
            try {
                do {
                    if (cancelled != null && cancelled.getAsBoolean())
                        throw new CancellationException("another range of the shard failed");
                    wait(WAIT_SLICE_MILLIS);
                } while (!ranges.isEmpty() && !mayRegister(readAhead));
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while waiting for heap to read a shard");
            }
        }
        var range = new Range(readAhead);
        ranges.add(range);
        readAheadKiB += readAhead;
        largestRangeReadAheadKiB = Math.max(largestRangeReadAheadKiB, readAhead);
        return range;
    }

    private boolean mayRegister(long newReadAheadKiB) {
        return waiting.isEmpty()
            && chargedKiB(null) + floorKiB() <= budgetKiB(ranges.size() + 1, readAheadKiB + newReadAheadKiB);
    }

    /** What documents may use now, in KiB. */
    synchronized long budgetKiB() {
        return budgetKiB(ranges.size(), readAheadKiB);
    }

    private long budgetKiB(int rangeCount, long rangesReadAheadKiB) {
        return maxKiB - (rangeCount + 1L) * perRangeKiB - rangesReadAheadKiB;
    }

    private long needKiB(long bytes) {
        return (long) Math.ceil(bytes * multiplier / 1024);
    }

    /** Reading room of every range that is not waiting. */
    private long floorKiB() {
        return (long) Math.ceil(largestKiB * readMultiplier);
    }

    private long chargedKiB(Range except) {
        long floor = floorKiB();
        long charged = 0;
        for (Range range : ranges) if (range != except) charged += range.chargeKiB(floor);
        return charged;
    }

    /**
     * Test view: admitted documents and open files; everything the ranges are charged, reading room
     * included; registered ranges; reservations made; the most an admission ever took the charges
     * above the budget (never positive); how often a range's open file was closed to free heap; how
     * many documents and registrations waited.
     */
    synchronized long reservedKiB() {
        long reserved = 0;
        for (Range range : ranges) reserved += range.documentKiB + range.fileKiB;
        return reserved;
    }

    synchronized long chargedKiB() {
        return chargedKiB(null);
    }

    synchronized long admittedDocuments() {
        return admitted;
    }

    synchronized int registeredRanges() {
        return ranges.size();
    }

    synchronized long maxOverBudgetKiB() {
        return overBudgetKiB;
    }

    synchronized long filesClosedForHeap() {
        return filesClosedForHeap;
    }

    synchronized long documentsWaited() {
        return documentsWaited;
    }

    synchronized long registrationsWaited() {
        return registrationsWaited;
    }

    /** Test view: the largest read-ahead any range registered with, in KiB. */
    synchronized long largestRangeReadAheadKiB() {
        return largestRangeReadAheadKiB;
    }

    final class Range implements AutoCloseable {
        private final long readAheadKiB;
        // Guarded by the gate; the owning thread alone changes documentKiB and fileKiB, so it may
        // read them without the lock.
        private long documentKiB;
        private long fileKiB;
        private boolean waitingForHeap;
        private boolean closed;
        private volatile boolean closeFileRequested;

        private Range(long readAheadKiB) {
            this.readAheadKiB = readAheadKiB;
        }

        private long chargeKiB(long floor) {
            return fileKiB + (waitingForHeap ? 0 : Math.max(documentKiB, floor));
        }

        /** {@link #admit(long, DeltaTableWriter.OpenFileCloser, BooleanSupplier)} without an open file or cancellation. */
        void admit(long bytes) throws IOException {
            admit(bytes, null, null);
        }

        /** {@link #tryAdmit}, else {@link #awaitAdmission}, for a caller that keeps nothing of the document meanwhile. */
        void admit(long bytes, DeltaTableWriter.OpenFileCloser closeOpenFile, BooleanSupplier cancelled) throws IOException {
            if (!tryAdmit(bytes, closeOpenFile)) awaitAdmission(bytes, cancelled);
        }

        /**
         * Releases this range's previous document, then reserves for a document of {@code bytes}
         * that the reader has just handed over, if that fits now, if necessary after
         * {@code closeOpenFile} has closed the writer's open file.
         *
         * @return false if the document has to wait: the caller moves it out of the heap and calls
         *         {@link #awaitAdmission}
         * @throws DocumentTooLargeException if the reservation exceeds the whole budget
         */
        boolean tryAdmit(long bytes, DeltaTableWriter.OpenFileCloser closeOpenFile) throws IOException {
            if (documentKiB == 0 && (!enabled || bytes < BYPASS_BYTES)) return true;
            long need;
            synchronized (DocumentMemoryGate.this) {
                releaseDocument();
                if (!enabled || bytes < BYPASS_BYTES) return true;
                largestKiB = Math.max(largestKiB, (bytes + 1023) >> 10);
                need = needKiB(bytes);
                requireWithinBudget(bytes, need);
                if (waiting.isEmpty() && fits(need)) {
                    grant(need);
                    return true;
                }
                if (fileKiB == 0 || closeOpenFile == null) return false;
            }
            closeOpenFile.close();
            openFileClosed();
            synchronized (DocumentMemoryGate.this) {
                filesClosedForHeap++;
                if (waiting.isEmpty() && fits(need)) {
                    grant(need);
                    return true;
                }
            }
            return false;
        }

        /**
         * Waits, holding nothing, until the document of {@code bytes} that {@link #tryAdmit} could
         * not admit fits, first come, first served, and reserves for it. Ranges that keep writing
         * are asked to close their open files meanwhile.
         *
         * @throws DocumentTooLargeException if the reservation exceeds the whole budget
         * @throws InterruptedIOException    if interrupted while waiting
         * @throws CancellationException     if {@code cancelled} turns true while waiting
         */
        void awaitAdmission(long bytes, BooleanSupplier cancelled) throws IOException {
            synchronized (DocumentMemoryGate.this) {
                long need = needKiB(bytes);
                waitingForHeap = true;
                waiting.add(this);
                documentsWaited++;
                DocumentMemoryGate.this.notifyAll();
                boolean granted = false;
                try {
                    while (true) {
                        requireWithinBudget(bytes, need);
                        if (waiting.peek() == this && fits(need)) {
                            waiting.remove(this);
                            waitingForHeap = false;
                            grant(need);
                            granted = true;
                            return;
                        }
                        for (Range range : ranges) if (range.fileKiB > 0 && !range.waitingForHeap) range.closeFileRequested = true;
                        if (cancelled != null && cancelled.getAsBoolean())
                            throw new CancellationException("another range of the shard failed");
                        DocumentMemoryGate.this.wait(WAIT_SLICE_MILLIS);
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException("interrupted while waiting for heap for a large document");
                } finally {
                    if (!granted) {
                        waiting.remove(this);
                        waitingForHeap = false;
                        DocumentMemoryGate.this.notifyAll();
                    }
                }
            }
        }

        /**
         * The others' charges beside this range's file and the larger of the document's need and
         * its reading room afterwards; alone, the document itself beside the file.
         */
        private boolean fits(long need) {
            long others = chargedKiB(this);
            long budget = budgetKiB();
            if (others + fileKiB + Math.max(need, floorKiB()) <= budget) return true;
            return others == 0 && fileKiB + need <= budget;
        }

        private void requireWithinBudget(long bytes, long need) throws DocumentTooLargeException {
            long budget = budgetKiB();
            if (need > budget) throw new DocumentTooLargeException(bytes, need << 10, budget << 10);
        }

        private void grant(long need) {
            overBudgetKiB = Math.max(overBudgetKiB, chargedKiB(this) + fileKiB + need - budgetKiB());
            documentKiB = need;
            admitted++;
        }

        private void releaseDocument() {
            if (documentKiB == 0) return;
            documentKiB = 0;
            DocumentMemoryGate.this.notifyAll();
        }

        /**
         * The writer's open file now keeps about {@code bytes} for the large documents written to it.
         * Grows only; what it grows by moves over from the document just written, whose reservation
         * covered it while it was written.
         */
        void openFileRetains(long bytes) {
            synchronized (DocumentMemoryGate.this) {
                long grow = ((bytes + 1023) >> 10) - fileKiB;
                if (grow <= 0) return;
                documentKiB -= Math.min(grow, documentKiB);
                fileKiB += grow;
            }
        }

        /** The writer closed its open file; what that file kept is garbage now. */
        void openFileClosed() {
            synchronized (DocumentMemoryGate.this) {
                closeFileRequested = false;
                if (fileKiB == 0) return;
                fileKiB = 0;
                DocumentMemoryGate.this.notifyAll();
            }
        }

        /** A document waits for heap while this range's open file holds some: close it after the current row. */
        boolean closeFileRequested() {
            return fileKiB > 0 && closeFileRequested;
        }

        /** Gives back the current document's reservation, if any. */
        void release() {
            if (documentKiB == 0) return;
            synchronized (DocumentMemoryGate.this) {
                releaseDocument();
            }
        }

        /** Gives back everything this range is charged and its share of the budget; idempotent. */
        @Override
        public void close() {
            synchronized (DocumentMemoryGate.this) {
                if (closed) return;
                closed = true;
                documentKiB = 0;
                fileKiB = 0;
                DocumentMemoryGate.this.readAheadKiB -= readAheadKiB;
                ranges.remove(this);
                waiting.remove(this);
                DocumentMemoryGate.this.notifyAll();
            }
        }
    }
}
