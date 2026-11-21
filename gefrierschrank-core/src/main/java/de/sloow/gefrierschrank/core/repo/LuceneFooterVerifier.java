package de.sloow.gefrierschrank.core.repo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * The single encoding of the repository checksum rules for one Lucene file, applied to bytes as
 * they are downloaded instead of re-reading the materialized file:
 * <ul>
 *   <li>exactly {@code length} bytes, covering every position once;</li>
 *   <li>only when the snapshot metadata carries a non-blank checksum: at least 16 bytes, the
 *       trailing 8 bytes (Lucene's footer checksum) equal the CRC32 of every byte before them,
 *       and {@code Long.toString(footer, 36)} equals the metadata checksum.</li>
 * </ul>
 *
 * <p>{@link #update} is position-addressed so the footer is found wherever it lands, including
 * across {@code .partN} blob boundaries, and so byte ranges of one file may arrive in any order
 * and from several threads. Each contiguous run keeps its own CRC32; {@link #finish} requires the
 * runs to tile the file exactly and combines their CRCs in file order.
 */
public final class LuceneFooterVerifier {

    static final String LENGTH_MISMATCH = "materialized shard file length differs from snapshot metadata";
    static final String NO_FOOTER = "Lucene file has no checksum footer";
    static final String CHECKSUM_MISMATCH = "Lucene file checksum differs from snapshot metadata";

    private static final int FOOTER_CHECKSUM_BYTES = 8;
    private static final int MIN_CHECKSUMMED_LENGTH = 16;

    private final long length;
    private final String checksum;
    private final long crcEnd;
    private final byte[] footer = new byte[FOOTER_CHECKSUM_BYTES];
    /** Runs that are not currently being extended, keyed by their end position. */
    private final Map<Long, Run> idleRunsByEnd = new HashMap<>();
    private final List<Run> allRuns = new ArrayList<>();
    private boolean overlapping;

    private static final class Run {
        final long start;
        long end;
        final CRC32 crc = new CRC32();

        Run(long start) {
            this.start = start;
            this.end = start;
        }
    }

    public LuceneFooterVerifier(long length, String checksum) {
        if (length < 0) throw new IllegalArgumentException("negative file length");
        this.length = length;
        this.checksum = checksum;
        this.crcEnd = Math.max(0, length - FOOTER_CHECKSUM_BYTES);
    }

    /** Feeds {@code count} bytes that sit at absolute file {@code position}. */
    public void update(long position, byte[] bytes, int offset, int count) throws IOException {
        Objects.checkFromIndexSize(offset, count, bytes.length);
        if (position < 0) throw new IllegalArgumentException("negative position");
        if (count == 0) return;
        if (position > length - count) throw new IOException(LENGTH_MISMATCH);

        Run run;
        synchronized (this) {
            run = idleRunsByEnd.remove(position);
            if (run == null) {
                run = new Run(position);
                allRuns.add(run);
            }
        }
        // Outside the lock: a run is extended by one caller at a time, because it is not in
        // idleRunsByEnd while this update owns it.
        long crcBytes = Math.min(count, crcEnd - position);
        if (crcBytes > 0) run.crc.update(bytes, offset, (int) crcBytes);
        synchronized (this) {
            long footerFrom = Math.max(position, length - FOOTER_CHECKSUM_BYTES);
            for (long p = footerFrom; p < position + count; p++) {
                footer[(int) (p - (length - FOOTER_CHECKSUM_BYTES))] = bytes[offset + (int) (p - position)];
            }
            run.end = position + count;
            if (idleRunsByEnd.putIfAbsent(run.end, run) != null) overlapping = true;
        }
    }

    public void update(long position, byte[] bytes) throws IOException {
        update(position, bytes, 0, bytes.length);
    }

    /** Throws unless every byte arrived exactly once and, when a checksum is recorded, it matches. */
    public synchronized void finish() throws IOException {
        List<Run> ordered = new ArrayList<>(allRuns);
        ordered.sort(Comparator.comparingLong(r -> r.start));
        long expected = 0;
        long crc = 0;
        for (Run run : ordered) {
            if (run.start != expected) throw new IOException(LENGTH_MISMATCH);
            long crcLength = Math.max(0, Math.min(run.end, crcEnd) - run.start);
            crc = Crc32Combine.combine(crc, run.crc.getValue(), crcLength);
            expected = run.end;
        }
        if (overlapping || expected != length) throw new IOException(LENGTH_MISMATCH);
        if (checksum == null || checksum.isBlank()) return;
        if (length < MIN_CHECKSUMMED_LENGTH) throw new IOException(NO_FOOTER);
        long stored = 0;
        for (byte b : footer) stored = (stored << 8) | (b & 0xFF);
        if (stored != crc || !Long.toString(stored, 36).equals(checksum)) throw new IOException(CHECKSUM_MISMATCH);
    }
}
