package de.sloow.gefrierschrank.syntheticsource.isolated;

import de.sloow.gefrierschrank.core.codec.ZstdFrameDecoder;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CloseableThreadLocal;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads Elasticsearch's ZSTD stored fields without Elasticsearch's native-access layer.
 *
 * <p>ES's own {@code ZstdCompressionMode$ZstdDecompressor} decompresses through
 * {@code NativeAccess.instance()}, which binds {@code libzstd} from an ES distribution's
 * {@code lib/platform} directory. A standalone process has no such directory, so ES falls back to
 * {@code NoopNativeAccess}, whose buffers are {@code null}: every stored-field read of a
 * ZSTD segment - {@code _source}, {@code _ignored_source}, stored text - would throw a
 * {@code NullPointerException} without a message. Synthetic-source indices written with
 * {@code best_compression} or {@code logsdb} (ES 8.14+) would hit this on their first document.
 *
 * <p>The isolated world now substitutes a working {@code NativeAccess}
 * ({@code org.elasticsearch.nativeaccess.IsolatedJniNativeAccess}, needed for ES 9.3's ZSTD-compressed
 * ES819 binary doc values), so ES's own decompressor would run too. This reader stays because it is
 * the proven byte-exact stored-field path and avoids the two heap copies per block that ES's
 * buffer-based decompressor makes.
 *
 * <p>This wrapper leaves every other format (the real ES doc-values/postings decoders this
 * isolated world exists for) untouched and replaces only the stored-fields reader of segments ES
 * tagged with {@value #ZSTD_MODE_ATTRIBUTE}. The replacement is Lucene's own compressing reader for
 * the same {@code ZstdStoredFields814} container with a {@link ZstdFrameDecoder}-backed
 * decompressor (zstd-jni, or pure Java where the native library cannot load), i.e. what
 * {@code gefrierschrank-lucene10}'s {@code EsZstdStoredFieldsFormat} does for the ambient world.
 * That class cannot be reused here: its Lucene type references would resolve to the ambient Lucene
 * classes (see {@code IsolatedEsClassLoader}). Segments ES wrapped in {@code TSDBStoredFieldsFormat}
 * are read correctly too: its bloom-filter sidecar contributes no stored fields.
 */
final class JniZstdStoredFieldsReader extends FilterDirectoryReader {

    /** Segment attribute written by ES's {@code Zstd814StoredFieldsFormat.fieldsWriter}. */
    static final String ZSTD_MODE_ATTRIBUTE = "Zstd814StoredFieldsFormat.mode";

    private static final String ZSTD_FORMAT_NAME = "ZstdStoredFields814";

    private final List<Closeable> resources;

    private JniZstdStoredFieldsReader(DirectoryReader in, Wrapper wrapper) throws IOException {
        super(in, wrapper);
        this.resources = wrapper.resources;
    }

    /** Wraps {@code in}, taking ownership of it: closing the result closes {@code in}. */
    static DirectoryReader wrap(DirectoryReader in) throws IOException {
        Wrapper wrapper = new Wrapper();
        try {
            return new JniZstdStoredFieldsReader(in, wrapper);
        } catch (IOException | RuntimeException | Error e) {
            Throwable failure = e instanceof UncheckedIOException unchecked ? unchecked.getCause() : e;
            IOUtils.closeWhileHandlingException(wrapper.resources);
            IOUtils.closeWhileHandlingException(in);
            if (failure instanceof IOException io) throw io;
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw (Error) failure;
        }
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
        throw new UnsupportedOperationException("reconstruction readers are never reopened");
    }

    @Override
    protected void doClose() throws IOException {
        IOUtils.close(in::close, () -> IOUtils.close(resources));
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    /**
     * The codec check is what decides which decompressor Lucene would actually use; the attribute
     * check additionally covers segments whose codec wraps that format.
     */
    static boolean usesEsZstdStoredFields(SegmentInfo info) {
        return info.getCodec().storedFieldsFormat() instanceof Zstd814StoredFieldsFormat
            || info.getAttribute(ZSTD_MODE_ATTRIBUTE) != null;
    }

    private static final class Wrapper extends SubReaderWrapper {
        final List<Closeable> resources = new ArrayList<>();

        @Override
        public LeafReader wrap(LeafReader reader) {
            if (!(reader instanceof SegmentReader segmentReader)) {
                return reader;
            }
            SegmentInfo info = segmentReader.getSegmentInfo().info;
            if (!usesEsZstdStoredFields(info)) {
                return reader;
            }
            StoredFieldsReader original;
            try {
                original = openStoredFields(segmentReader, info);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            // One clone per thread, like Lucene's SegmentCoreReaders: a reader caches its current
            // decompressed block, so reusing it keeps consecutive documents in a block cheap.
            CloseableThreadLocal<StoredFieldsReader> perThread = new CloseableThreadLocal<>() {
                @Override
                protected StoredFieldsReader initialValue() {
                    return original.clone();
                }
            };
            resources.add(perThread);
            return new Leaf(segmentReader, perThread);
        }

        private StoredFieldsReader openStoredFields(SegmentReader segmentReader, SegmentInfo info) throws IOException {
            Directory segmentDirectory = info.dir;
            if (info.getUseCompoundFile()) {
                segmentDirectory = info.getCodec().compoundFormat().getCompoundReader(info.dir, info);
                resources.add(segmentDirectory);
            }
            StoredFieldsReader storedFields = JniZstdStoredFieldsFormat.INSTANCE.fieldsReader(
                segmentDirectory, info, segmentReader.getFieldInfos(), IOContext.DEFAULT);
            resources.add(storedFields);
            return storedFields;
        }
    }

    private static final class Leaf extends FilterCodecReader {
        private final CloseableThreadLocal<StoredFieldsReader> storedFields;

        Leaf(CodecReader in, CloseableThreadLocal<StoredFieldsReader> storedFields) {
            super(in);
            this.storedFields = storedFields;
        }

        @Override
        public StoredFieldsReader getFieldsReader() {
            return storedFields.get();
        }

        @Override
        protected void doClose() {
            // FilterCodecReader would close the segment reader directly; the enclosing directory
            // reader owns it and the stored-fields resources.
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    /** Read-only: the container settings besides name and compression are writer-side tuning only. */
    private static final class JniZstdStoredFieldsFormat extends Lucene90CompressingStoredFieldsFormat {
        static final JniZstdStoredFieldsFormat INSTANCE = new JniZstdStoredFieldsFormat();

        private JniZstdStoredFieldsFormat() {
            super(ZSTD_FORMAT_NAME, JniZstdCompressionMode.INSTANCE, 14336, 128, 10);
        }

        @Override
        public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) {
            throw new UnsupportedOperationException("read-only format");
        }
    }

    private static final class JniZstdCompressionMode extends CompressionMode {
        static final JniZstdCompressionMode INSTANCE = new JniZstdCompressionMode();

        @Override
        public Compressor newCompressor() {
            throw new UnsupportedOperationException("read-only format");
        }

        @Override
        public Decompressor newDecompressor() {
            return new JniZstdDecompressor();
        }
    }

    /** Same block layout as ES's decompressor: VInt compressed length, then one zstd frame. */
    static final class JniZstdDecompressor extends Decompressor {
        private final ZstdFrameDecoder decoder = new ZstdFrameDecoder();
        private byte[] compressed = new byte[0];

        @Override
        public void decompress(DataInput in, int originalLength, int offset, int length, BytesRef bytes) throws IOException {
            if (originalLength == 0) {
                bytes.offset = 0;
                bytes.length = 0;
                return;
            }
            int compressedLength = in.readVInt();
            compressed = ArrayUtil.growNoCopy(compressed, compressedLength);
            in.readBytes(compressed, 0, compressedLength);

            bytes.bytes = ArrayUtil.growNoCopy(bytes.bytes, originalLength);
            int decompressedLength;
            try {
                decompressedLength = decoder.decompress(compressed, compressedLength, bytes.bytes, originalLength);
            } catch (ZstdFrameDecoder.MalformedFrameException e) {
                throw new CorruptIndexException("zstd decompression failed", in, e);
            }
            if (decompressedLength != originalLength) {
                throw new CorruptIndexException(
                    "Expected " + originalLength + " decompressed bytes but got " + decompressedLength, in);
            }
            bytes.offset = offset;
            bytes.length = length;
        }

        @Override
        public Decompressor clone() {
            return new JniZstdDecompressor();
        }
    }
}
