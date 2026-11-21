package de.sloow.gefrierschrank.syntheticsource;

import java.io.IOException;

/**
 * Ambient-side contract for the reconstruction worker that actually lives inside
 * {@link IsolatedEsClassLoader}'s isolated world (see {@code isolated.IsolatedReconstructionWorkerImpl}).
 *
 * <p>This interface itself must NOT be isolated (see {@link IsolatedEsClassLoader}'s Javadoc): it
 * has to be the SAME {@link Class} object on both sides of the classloader boundary for the
 * isolated implementation instance to be castable to it from ambient code. Its methods therefore
 * deliberately touch only plain JDK types (int, byte[]) - no Lucene or Elasticsearch type ever
 * crosses this boundary.
 */
public interface IsolatedReconstructionWorker extends AutoCloseable {

    /**
     * JVM property choosing the directory the worker reads the shard through: {@code mmap} (the
     * default, falling back to {@code niofs} once when mapping fails) or {@code niofs}.
     */
    String DIRECTORY_PROPERTY = "gefrierschrank.reconstruction.directory";

    /**
     * Bytes after which {@link #readSources} stops early: about 8 MiB of documents held at once, which
     * a batch of 1024 documents reaches only at about 8 KB per document.
     */
    long DEFAULT_BATCH_BYTES = 8L << 20;

    /** JVM property overriding {@link #DEFAULT_BATCH_BYTES}, read when a worker opens; for tests and benchmarks. */
    String BATCH_BYTES_PROPERTY = "gefrierschrank.reconstruction.batchBytes";

    int docCount();

    /**
     * Reconstructed {@code _source} of the given document as Elasticsearch renders it (UTF-8 JSON,
     * what ES's own search returns); {@link SyntheticSourceJson} turns it into the export text. Any
     * order of doc ids is correct; ascending ids within a segment are the fast path (loaders are
     * built once per segment run instead of per document). Calls are serialized.
     */
    byte[] readSource(int docId) throws IOException;

    /**
     * Reconstructs a run of documents the way Elasticsearch's fetch phase does: each segment's
     * loaders are built for exactly the ids of this call that fall into it, so single-valued doc
     * values are looked up once per distinct value instead of once per document. Each id is visited
     * once, in order; the loaders are discarded on return and never shared with
     * {@link #readSource}. Element {@code i} of the result belongs to id {@code i}. The result may
     * cover only a prefix of the ids (never fewer than one), ending at the document that brings the
     * returned bytes to the worker's batch limit.
     *
     * @param ascendingGlobalDocIds strictly ascending global doc ids, at least one
     */
    byte[][] readSources(int[] ascendingGlobalDocIds) throws IOException;

    @Override
    void close() throws IOException;
}
