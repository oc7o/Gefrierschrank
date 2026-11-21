package de.sloow.gefrierschrank.syntheticsource;

import java.io.IOException;

/**
 * Ambient-side contract of one shard's vector rehydration session, implemented in the isolated world
 * ({@code isolated.IsolatedSourceVectorsSessionImpl}). Same rule as
 * {@link IsolatedReconstructionWorker}: never isolated, only JDK types cross the boundary.
 *
 * <p>The session (mapping, reader) is shared read-only by every range of the shard; each range reads
 * through its own {@link Cursor}, created on the thread that uses it, because Elasticsearch's vector
 * field loaders assert they stay on the thread that built them.
 */
public interface IsolatedSourceVectorsSession extends AutoCloseable {

    int docCount();

    /** A cursor for the calling thread; never share it with another thread. */
    Cursor newCursor();

    @Override
    void close() throws IOException;

    /** Patches of one root document at a time, as Elasticsearch's fetch phase computes them. */
    interface Cursor {
        /**
         * Loads Elasticsearch's vector patches of root document {@code globalDocId} and returns how
         * many there are; 0 = nothing to put back. Ascending ids within a segment are the fast path.
         */
        int load(int globalDocId) throws IOException;

        /** Every loaded patch is a leaf vector whose path is a root key (no {@code .}). */
        boolean rootLeavesOnly();

        /** The loaded root keys in Elasticsearch's order; only when {@link #rootLeavesOnly}. */
        String[] rootKeys();

        /**
         * {@code "k1":v1,"k2":v2} of the loaded patches as Elasticsearch's fetch renders them, UTF-8,
         * without braces; only when {@link #rootLeavesOnly}.
         */
        byte[] rootMembers() throws IOException;

        /** Elasticsearch's patched rendering of {@code stored[0, length)} with the loaded patches, as JSON. */
        byte[] reserialize(byte[] stored, int length) throws IOException;
    }
}
