package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.syntheticsource.SyntheticSourceReconstructor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Wraps a fast-path {@link ShardReader} and falls back to real ES-mapper-based synthetic-source
 * reconstruction (see {@code gefrierschrank-synthetic-source}) ONLY for documents the delegate
 * reports as {@link ShardReader#storedSourceAbsent} (their segment has no {@code _source} field at
 * all) or when the fast path's own "likely synthetic source" {@link IOException} fires (see
 * {@code Lucene10ShardReader.readSource}) - a document merely
 * missing for some other reason must still surface as that other, unrelated failure, not be
 * silently swallowed here.
 *
 * <p>The reconstructor is opened lazily, on the FIRST document that actually needs it - most
 * shards never hit synthetic source at all, and constructing a standalone {@code MapperService}
 * (a real, if small, cost) for every shard regardless would be wasted work.
 *
 * <p>{@code rawIndexMetadata} may be {@code null} (index metadata could not be resolved, or the
 * caller deliberately opted out - see {@link DumpToDeltaPipeline}'s test seams): in that case this
 * class is a bare passthrough, never touching {@code gefrierschrank-synthetic-source} at all, so
 * the original fast-path exception propagates completely unchanged - this is the regression
 * guard: reconstruction only ever supersedes the hard failure when it can actually be attempted.
 */
final class SyntheticSourceFallbackShardReader implements ShardReader {

    /** Most documents reconstructed ahead in one batch; the worker also stops at about 8 MiB. */
    static final int BATCH_DOCUMENTS = 1024;
    private static final int[] NO_IDS = new int[0];
    private static final byte[][] NO_SOURCES = new byte[0][];

    private final ShardReader delegate;
    private final Path shardDir;
    private final String indexName;
    private final JsonNode rawIndexMetadata;
    private SyntheticSourceReconstructor reconstructor;
    // Sources reconstructed ahead of the caller: batchSources[i] belongs to batchIds[i], and
    // batchNext is the next one the caller is expected to ask for.
    private int[] batchIds = NO_IDS;
    private byte[][] batchSources = NO_SOURCES;
    private int batchNext;
    // Ids asked for per batch: what the worker's byte limit let through last time, doubling after a
    // batch it returned whole, so large documents do not load doc values for ids never reached.
    private int batchDocuments = BATCH_DOCUMENTS;
    // After a batch failed, documents up to this id are reconstructed one at a time.
    private int perDocumentThrough = -1;
    private int failedBatches;
    private String firstBatchFailure;
    // Shared by the root wrapper and its forks (handed over in fork): global ids of documents rebuilt
    // through the rewritten mapping whose _ignored lists a field, so retried batches and ranges never
    // count one twice. Guarded by itself.
    private java.util.BitSet ignoredDocuments = new java.util.BitSet();
    private volatile boolean ignoredNoteDeferred;
    // Set when a reconstructor opened here loses values under dynamic:false/runtime objects (opt-in).
    private java.util.concurrent.atomic.AtomicBoolean unmappedValuesLost = new java.util.concurrent.atomic.AtomicBoolean();
    private boolean isFork;

    private SyntheticSourceFallbackShardReader(ShardReader delegate, Path shardDir, String indexName,
                                                JsonNode rawIndexMetadata) {
        this.delegate = delegate;
        this.shardDir = shardDir;
        this.indexName = indexName;
        this.rawIndexMetadata = rawIndexMetadata;
    }

    /**
     * The export reader stack, composed only here: {@code delegate} (the runtime reader), then
     * {@link SourceVectorsShardReader} when the index's vector plan rehydrates, then this fallback.
     * Without {@code rawIndexMetadata} returns {@code delegate} completely untouched (no wrapper
     * object at all), so a caller with no metadata gets byte-for-byte the same behavior as before
     * this class existed. When this throws, {@code delegate} is already closed.
     */
    static ShardReader wrap(ShardReader delegate, Path shardDir, String indexName, JsonNode rawIndexMetadata)
            throws IOException {
        if (rawIndexMetadata == null) {
            return delegate;
        }
        ShardReader vectors;
        try {
            vectors = SourceVectorsShardReader.wrap(delegate, shardDir, indexName, rawIndexMetadata,
                SourceVectorsPlan.forIndex(indexName, rawIndexMetadata));
        } catch (IOException | RuntimeException | Error failure) {
            try { delegate.close(); } catch (Exception close) { failure.addSuppressed(close); }
            throw failure;
        }
        return new SyntheticSourceFallbackShardReader(vectors, shardDir, indexName, rawIndexMetadata);
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
        if (forked == null) return null;
        SyntheticSourceFallbackShardReader wrapper = new SyntheticSourceFallbackShardReader(forked, shardDir, indexName, rawIndexMetadata);
        wrapper.ignoredDocuments = ignoredDocuments;
        wrapper.unmappedValuesLost = unmappedValuesLost;
        wrapper.isFork = true;
        return wrapper;
    }

    @Override
    public de.sloow.gefrierschrank.core.lucene.DocumentMetadata documentMetadata(int docId) throws IOException {
        return delegate.documentMetadata(docId);
    }

    /**
     * Documents of segments without any {@code _source} field go straight to reconstruction; the
     * delegate would only decode their stored fields to throw. The exception path below stays the
     * safety net for readers that cannot tell up front.
     */
    @Override
    public void readSource(int docId, SourceConsumer consumer) throws IOException {
        if (delegate.storedSourceAbsent(docId)) {
            byte[] bytes = batchedSource(docId);
            consumer.accept(bytes, bytes.length);
            return;
        }
        try {
            delegate.readSource(docId, consumer);
        } catch (IOException e) {
            if (!isLikelySyntheticSourceFailure(e)) {
                throw e;
            }
            reconstructedSource(docId, consumer);
        }
    }

    /**
     * Matches the exact wording {@code Lucene10ShardReader} uses for the
     * no-stored-{@code _source} case (see that class) - deliberately narrow, so an unrelated
     * IOException (corrupt segment, truncated file, S3 read error) is never mistaken for synthetic
     * source and silently redirected into reconstruction instead of surfacing as itself.
     */
    private static boolean isLikelySyntheticSourceFailure(IOException e) {
        String message = e.getMessage();
        return message != null && message.contains("synthetic source");
    }

    private void reconstructedSource(int docId, SourceConsumer consumer) throws IOException {
        byte[] bytes = reconstructor().readSourceBytes(docId);
        consumer.accept(bytes, bytes.length);
    }

    /**
     * Serves documents of source-less segments from a batch reconstructed ahead of the caller.
     * Given the ids of a run of documents, Elasticsearch's loaders read single-valued doc values
     * once per distinct value instead of once per document, as in its fetch phase. A batch holds
     * the requested document and the live source-less documents after it, up to the first other
     * document, so it is exactly what the caller asks for next; any other request drops it. When a
     * batch fails, its documents are reconstructed one at a time, so a document that cannot be
     * reconstructed fails alone, with its own exception, as it does without batches.
     */
    private byte[] batchedSource(int docId) throws IOException {
        if (batchNext < batchIds.length && batchIds[batchNext] == docId) {
            byte[] source = batchSources[batchNext];
            batchSources[batchNext++] = null;
            return source;
        }
        batchIds = NO_IDS;
        batchSources = NO_SOURCES;
        batchNext = 0;
        SyntheticSourceReconstructor reconstruction = reconstructor();
        if (docId > perDocumentThrough) {
            int[] ids = batchFrom(docId);
            if (ids.length > 1) {
                byte[][] sources;
                try {
                    sources = reconstruction.readSourcesBytes(ids);
                } catch (IOException | RuntimeException batchFailure) {
                    if (failedBatches++ == 0) firstBatchFailure = batchFailure.getClass().getSimpleName();
                    perDocumentThrough = ids[ids.length - 1];
                    return reconstruction.readSourceBytes(docId);
                }
                if (sources.length < ids.length) {
                    batchDocuments = Math.max(2, sources.length);
                } else if (ids.length == batchDocuments) {
                    batchDocuments = Math.min(BATCH_DOCUMENTS, 2 * batchDocuments);
                }
                batchIds = sources.length == ids.length ? ids : Arrays.copyOf(ids, sources.length);
                batchSources = sources;
                batchNext = 1;
                byte[] source = sources[0];
                sources[0] = null;
                return source;
            }
        }
        return reconstruction.readSourceBytes(docId);
    }

    private int[] batchFrom(int docId) throws IOException {
        int docCount = delegate.docCount();
        int[] ids = new int[Math.min(batchDocuments, docCount - docId)];
        int count = 0;
        ids[count++] = docId;
        for (int d = docId + 1; d < docCount && count < ids.length; d++) {
            if (!delegate.isLive(d)) continue;
            if (!delegate.storedSourceAbsent(d)) break;
            ids[count++] = d;
        }
        return count == ids.length ? ids : Arrays.copyOf(ids, count);
    }

    private SyntheticSourceReconstructor reconstructor() throws IOException {
        if (reconstructor == null) {
            java.util.BitSet documents = ignoredDocuments;
            reconstructor = SyntheticSourceReconstructor.open(shardDir, indexName, rawIndexMetadata, doc -> {
                synchronized (documents) {
                    documents.set(doc);
                }
            });
            if (reconstructor.losesUnmappedValues()) unmappedValuesLost.set(true);
        }
        return reconstructor;
    }

    /**
     * Warns, without any document content, when batches failed and their documents took the slow
     * path; the root wrapper also notes how many reconstructed documents lost values Elasticsearch
     * ignored, unless the caller reports that itself ({@link #deferIgnoredFieldNote}).
     */
    @Override
    public void close() throws IOException {
        if (failedBatches > 0) {
            System.err.println("warning: " + failedBatches + " reconstruction batch(es) of index " + indexName
                + " failed (first: " + firstBatchFailure + "); their documents were reconstructed one at a time");
        }
        Long ignored = ignoredFieldDocuments(this);
        if (ignored != null && !ignoredNoteDeferred) {
            System.err.println(ignoredFieldNote(indexName, ignored));
        }
        try {
            delegate.close();
        } finally {
            if (reconstructor != null) {
                reconstructor.close();
            }
        }
    }

    /**
     * Distinct documents of the shard that were rebuilt without stored {@code _source} through the
     * rewritten mapping and list a field in {@code _ignored}: null unless {@code reader} is a root
     * wrapper (its forks share the count) and the count is above zero.
     */
    static Long ignoredFieldDocuments(ShardReader reader) {
        if (!(reader instanceof SyntheticSourceFallbackShardReader wrapper) || wrapper.isFork) return null;
        long count;
        synchronized (wrapper.ignoredDocuments) {
            count = wrapper.ignoredDocuments.cardinality();
        }
        return count > 0 ? count : null;
    }

    /** The caller reports the {@code _ignored} count itself; a no-op unless {@code reader} is a root wrapper. */
    static void deferIgnoredFieldNote(ShardReader reader) {
        if (reader instanceof SyntheticSourceFallbackShardReader wrapper && !wrapper.isFork) {
            wrapper.ignoredNoteDeferred = true;
        }
    }

    /** Content-free: the index name and a count. */
    static String ignoredFieldNote(String indexName, long documents) {
        return "note: " + documents + " document(s) of index " + indexName + " list fields in _ignored; values Elasticsearch"
            + " ignored (and whole values of text fields whose keyword sub-field ignored them) were kept only in the"
            + " disabled _source and are missing from their reconstructed rows";
    }

    /**
     * {@code TRUE} when documents of this shard were rebuilt without stored {@code _source} although
     * values sent under {@code dynamic: false}/{@code runtime} objects are lost uncounted
     * ({@code --source-disabled-accept unmapped-values}); null otherwise, never false.
     */
    static Boolean unrecoverableUnmappedValues(ShardReader reader) {
        return reader instanceof SyntheticSourceFallbackShardReader wrapper && wrapper.unmappedValuesLost.get() ? Boolean.TRUE : null;
    }
}
