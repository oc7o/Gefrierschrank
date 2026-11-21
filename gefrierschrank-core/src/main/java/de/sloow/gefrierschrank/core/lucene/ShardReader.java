package de.sloow.gefrierschrank.core.lucene;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Read-side abstraction over a materialized Lucene shard directory; the CLI reads every shard
 * through the in-process {@code lucene10} implementation.
 *
 * <p>Usage contract: one reader or {@link #fork} per thread. Within a thread, for each live
 * document the caller invokes {@link #readSource} and then {@link #documentMetadata}, in ascending
 * doc id order; an implementation may serve that pair from a single stored-field decode. Readers
 * are stateful and not thread-safe. {@link DocumentMetadata#slot()} is the global Lucene doc id.
 */
public interface ShardReader extends AutoCloseable {

    /** Identifier of the runtime implementation ("lucene10"). */
    String runtimeName();

    /** Number of document ID slots; deleted slots must be skipped via isLive. */
    int docCount() throws IOException;

    default boolean isLive(int docId) throws IOException { return true; }

    /** True only when the segment holding docId has no _source field, so no document in it can carry stored source. */
    default boolean storedSourceAbsent(int docId) throws IOException { return false; }

    /**
     * An independent reader over the same materialized shard, for exactly one other thread, or
     * null if this reader cannot fork (the caller then reads the shard as one range). Called on this
     * reader's thread before any other thread uses a fork. A fork stays valid until this reader is
     * closed; every fork is closed before this reader, and closing a fork releases only what the
     * fork itself opened.
     */
    default ShardReader fork() throws IOException { return null; }

    default DocumentMetadata documentMetadata(int docId) throws IOException {
        return new DocumentMetadata(null, null, "unknown", docId);
    }

    /**
     * Hands the {@code _source} payload of document {@code docId} to the consumer: bytes
     * {@code [0, length)} of an array the consumer owns and may keep. A document without stored
     * {@code _source} fails with an IOException whose message contains "synthetic source" (the
     * reconstruction trigger); no other failure may use that phrase.
     */
    void readSource(int docId, SourceConsumer consumer) throws IOException;

    @FunctionalInterface
    interface SourceConsumer {
        void accept(byte[] bytes, int length);
    }

    @Override
    void close() throws IOException;
}
