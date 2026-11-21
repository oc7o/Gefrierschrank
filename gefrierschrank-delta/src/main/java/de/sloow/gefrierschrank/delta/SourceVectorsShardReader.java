package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.syntheticsource.IsolatedSourceVectorsSession;
import de.sloow.gefrierschrank.syntheticsource.SourceVectors;
import de.sloow.gefrierschrank.syntheticsource.SourceVectorsMismatchException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

/**
 * Puts the vectors an index kept out of its stored {@code _source} back into every document, as
 * Elasticsearch's fetch does with {@code "_source": {"exclude_vectors": false}}. A document without
 * vectors passes through as the delegate's very array ({@code _source_kind} unchanged); root-level
 * vectors are spliced in after every stored byte ({@code +vectors}); anything else is Elasticsearch's
 * rendering of the patched document ({@code +vectors-reserialized}). The suffix is appended to the
 * delegate's kind, never replacing it.
 *
 * <p>The root reader owns the shard's {@link IsolatedSourceVectorsSession}; every fork reads through
 * its own cursor, created lazily on the fork's thread. Failures propagate (the shard quarantines);
 * none of them carries the phrase that sends a document to synthetic-source reconstruction.
 */
final class SourceVectorsShardReader implements ShardReader {

    static final String SPLICED_SUFFIX = "+vectors";
    static final String RESERIALIZED_SUFFIX = "+vectors-reserialized";
    /**
     * Heap a rehydrating range holds besides {@link ShardRanges#PER_RANGE_HEAP_BYTES}: its cursor's
     * per-segment vector readers and the patched copy of its current document (measured).
     */
    static final long RANGE_HEAP_BYTES = 4L << 20;

    /** Opens a shard's session; a test seam for sessions that fail on purpose. */
    @FunctionalInterface
    interface SessionOpener {
        IsolatedSourceVectorsSession open(Path shardDir, String indexName, JsonNode metadata, Set<String> fields)
            throws IOException;
    }

    static volatile SessionOpener sessions = SourceVectors.Rehydrator::open;

    private final ShardReader delegate;
    private final IsolatedSourceVectorsSession session;
    private final boolean ownsSession;
    private final VectorSplice splice = new VectorSplice();
    private final SourceConsumer capture = (bytes, length) -> {
        stored = bytes;
        storedLength = length;
    };
    private byte[] stored;
    private int storedLength;
    private IsolatedSourceVectorsSession.Cursor cursor;
    private int lastDoc = -1;
    private String lastSuffix;

    private SourceVectorsShardReader(ShardReader delegate, IsolatedSourceVectorsSession session, boolean ownsSession) {
        this.delegate = delegate;
        this.session = session;
        this.ownsSession = ownsSession;
    }

    /**
     * {@code delegate} itself unless {@code plan} rehydrates; otherwise a reader that owns a session
     * opened now, on the calling thread. Refuses a plan whose vectors are excluded but not
     * rehydrated, so no reader exports a document without them. The caller keeps {@code delegate}
     * when this throws.
     */
    static ShardReader wrap(ShardReader delegate, Path shardDir, String indexName, JsonNode metadata,
                            SourceVectorsPlan plan) throws IOException {
        if (plan == null || metadata == null) return delegate;
        plan.requireExportable();
        if (!plan.rehydrates()) return delegate;
        return wrap(delegate, sessions.open(shardDir, indexName, metadata, plan.excludedFields()));
    }

    /** Takes ownership of {@code session}: closes it when the shard does not match it. */
    static ShardReader wrap(ShardReader delegate, IsolatedSourceVectorsSession session) throws IOException {
        try {
            if (session.docCount() != delegate.docCount()) throw new SourceVectorsMismatchException();
        } catch (IOException | RuntimeException mismatch) {
            try { session.close(); } catch (IOException | RuntimeException close) { mismatch.addSuppressed(close); }
            throw mismatch;
        }
        return new SourceVectorsShardReader(delegate, session, true);
    }

    @Override
    public String runtimeName() {
        return delegate.runtimeName();
    }

    @Override
    public int docCount() throws IOException {
        return delegate.docCount();
    }

    @Override
    public boolean isLive(int docId) throws IOException {
        return delegate.isLive(docId);
    }

    @Override
    public boolean storedSourceAbsent(int docId) throws IOException {
        return delegate.storedSourceAbsent(docId);
    }

    @Override
    public ShardReader fork() throws IOException {
        ShardReader forked = delegate.fork();
        return forked == null ? null : new SourceVectorsShardReader(forked, session, false);
    }

    @Override
    public void readSource(int docId, SourceConsumer consumer) throws IOException {
        lastDoc = -1;
        lastSuffix = null;
        stored = null;
        delegate.readSource(docId, capture);
        byte[] bytes = stored;
        int length = storedLength;
        stored = null;
        if (cursor == null) cursor = session.newCursor();
        if (cursor.load(docId) == 0) {
            consumer.accept(bytes, length);
            return;
        }
        byte[] patched = cursor.rootLeavesOnly() ? splice.splice(bytes, length, cursor.rootKeys(), cursor.rootMembers()) : null;
        String suffix = SPLICED_SUFFIX;
        if (patched == null) {
            patched = cursor.reserialize(bytes, length);
            suffix = RESERIALIZED_SUFFIX;
        }
        lastDoc = docId;
        lastSuffix = suffix;
        consumer.accept(patched, patched.length);
    }

    @Override
    public DocumentMetadata documentMetadata(int docId) throws IOException {
        DocumentMetadata metadata = delegate.documentMetadata(docId);
        if (lastSuffix == null || docId != lastDoc) return metadata;
        return new DocumentMetadata(metadata.id(), metadata.routing(), metadata.sourceKind() + lastSuffix, metadata.slot());
    }

    /** Closes the delegate, then, on the root reader, the shard's session; forks are closed before the root. */
    @Override
    public void close() throws IOException {
        try {
            delegate.close();
        } finally {
            if (ownsSession) session.close();
        }
    }
}
