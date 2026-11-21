package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;

/**
 * Lets Elasticsearch's {@code StoredFieldLoader} read stored fields through each segment's merge
 * instance, the way ES's fetch phase and {@code ConcurrentSegmentSourceProvider} do on a real node.
 *
 * <p>{@code StoredFieldLoader.create(..., forceSequentialReader=true)} takes the sequential path only
 * for leaves that are {@link SequentialStoredFieldsLeafReader}s (on a node, every leaf is an
 * {@code ElasticsearchLeafReader}); other leaves get the random-access reader, which decompresses
 * the whole block again for every document - a full 240 KB zstd block per doc for
 * {@code best_compression}/logsdb segments. The merge instance keeps the current block, so reading
 * a segment's documents in ascending order decompresses each block once.
 *
 * <p>Must be the outermost wrapper, over {@link JniZstdStoredFieldsReader}: the merge instance comes
 * from {@code in.getFieldsReader()}, which only the zstd wrapper's leaves replace with a reader that
 * works without ES native access. Wrapped the other way round, the zstd wrapper would no longer see
 * {@code SegmentReader}s and ZSTD segments would fall back to ES's own decompressor.
 */
final class SequentialStoredFieldsDirectoryReader extends FilterDirectoryReader {

    private SequentialStoredFieldsDirectoryReader(DirectoryReader in) throws IOException {
        super(in, new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return new Leaf(reader);
            }
        });
    }

    /** Wraps {@code in}, taking ownership of it: closing the result, or a failed wrap, closes {@code in}. */
    static DirectoryReader wrap(DirectoryReader in) throws IOException {
        try {
            return new SequentialStoredFieldsDirectoryReader(in);
        } catch (IOException | RuntimeException | Error e) {
            IOUtils.closeWhileHandlingException(in);
            throw e;
        }
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
        throw new UnsupportedOperationException("reconstruction readers are never reopened");
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    /** Same shape as ES's {@code ElasticsearchLeafReader}, without the shard id. */
    static final class Leaf extends SequentialStoredFieldsLeafReader {

        Leaf(LeafReader in) {
            super(in);
        }

        @Override
        protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
            return reader;
        }

        // Nested-object loaders key their parent bitsets on the core cache key.
        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

        @Override
        protected void doClose() {
            // The wrapped directory reader owns the segment readers.
        }
    }
}
