package de.sloow.gefrierschrank.core.repo;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

/**
 * Read access to a snapshot repository's blob space, keyed by repository-relative path.
 * Implementations: filesystem checkout, S3/MinIO. Implementations must be safe for concurrent
 * use: a shard's files are downloaded by several threads at once.
 */
public interface BlobStore extends AutoCloseable {

    Optional<BlobMetadata> stat(String key);

    InputStream read(String key) throws IOException;

    /**
     * An open byte range of one blob. {@code version} identifies the blob content the range was
     * cut from (null when the store cannot tell); {@code objectLength} is the blob's total length
     * (-1 when unknown).
     */
    record RangeRead(InputStream in, String version, long objectLength) implements Closeable {
        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /**
     * Exactly {@code length} bytes of {@code key} starting at {@code offset}. A range that does
     * not lie entirely inside the blob is an IOException. When {@code expectedVersion} is not
     * null, the range must come from that version of the blob (as returned by an earlier call),
     * otherwise IOException: ranges of different blob contents must never be stitched together.
     *
     * <p>This default can pin nothing: it reads the blob from the start, skips to the offset and
     * reports an unknown version and length.
     */
    default RangeRead openRange(String key, long offset, long length, String expectedVersion) throws IOException {
        requireValidRange(offset, length);
        InputStream in = read(key);
        try {
            in.skipNBytes(offset);
        } catch (EOFException e) {
            in.close();
            throw new IOException(RANGE_OUTSIDE_BLOB, e);
        } catch (IOException | RuntimeException e) {
            in.close();
            throw e;
        }
        return new RangeRead(new ExactLengthInputStream(in, length), null, -1);
    }

    String RANGE_OUTSIDE_BLOB = "requested range lies outside the blob";
    String BLOB_CHANGED = "blob changed between range reads";

    static void requireValidRange(long offset, long length) {
        if (offset < 0 || length <= 0 || offset > Long.MAX_VALUE - length) {
            throw new IllegalArgumentException("invalid blob range");
        }
    }

    /** Ends after exactly {@code length} bytes; ending earlier is an EOFException, never a short read. */
    final class ExactLengthInputStream extends FilterInputStream {
        private long remaining;

        public ExactLengthInputStream(InputStream in, long length) {
            super(in);
            this.remaining = length;
        }

        @Override
        public int read() throws IOException {
            if (remaining == 0) return -1;
            int b = in.read();
            if (b < 0) throw new EOFException(RANGE_OUTSIDE_BLOB);
            remaining--;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) return 0;
            if (remaining == 0) return -1;
            int count = in.read(b, off, (int) Math.min(len, remaining));
            if (count < 0) throw new EOFException(RANGE_OUTSIDE_BLOB);
            remaining -= count;
            return count;
        }

        @Override
        public long skip(long n) throws IOException {
            long skipped = in.skip(Math.min(n, remaining));
            remaining -= skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return (int) Math.min(in.available(), remaining);
        }

        @Override
        public boolean markSupported() {
            return false;
        }
    }

    /** All keys under the given prefix (recursively), sorted. */
    List<String> list(String prefix);

    /**
     * All blobs under the given prefix (recursively) with their sizes, gathered in one bulk
     * listing pass - implementations must serve this from the same paginated directory/object
     * listing that already returns sizes (S3's ListObjectsV2 includes size per entry; a local
     * filesystem walk already stats every file it visits), never one stat/HEAD call per key.
     * Exists for bulk reconciliation against a much smaller reachable-key set (e.g. orphan
     * detection) at repository scales where per-object round trips would be impractical.
     */
    List<BlobMetadata> listWithSizes(String prefix);

    void put(String key, byte[] content);

    @Override
    default void close() {}
}
