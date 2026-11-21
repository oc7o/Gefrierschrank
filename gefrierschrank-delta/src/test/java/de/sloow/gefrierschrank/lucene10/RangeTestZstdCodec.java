package de.sloow.gefrierschrank.lucene10;

import com.github.luben.zstd.Zstd;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 * Test codec of the delta module: Elasticsearch's best_compression ZSTD stored-fields layout
 * (240 KB blocks of up to 2048 documents, VInt length plus one ZSTD frame), read through the
 * production {@link EsZstdDecompressor}. It has its own name because on this module's test
 * classpath the Elasticsearch jar shadows the Elasticsearch codec names with ES's native reader.
 * In this package only to reach the package-private decompressor.
 */
public final class RangeTestZstdCodec extends FilterCodec {
    private final StoredFieldsFormat storedFields = new Lucene90CompressingStoredFieldsFormat(
        "GsRangeTestZstdStoredFields", new Mode(), 245760, 2048, 10);

    public RangeTestZstdCodec() {
        super("GsRangeTestZstd", new Lucene103Codec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFields;
    }

    private static final class Mode extends CompressionMode {
        @Override
        public Compressor newCompressor() {
            return new Compressor() {
                @Override
                public void compress(ByteBuffersDataInput in, DataOutput out) throws IOException {
                    byte[] source = new byte[(int) in.length()];
                    in.readBytes(source, 0, source.length);
                    byte[] compressed = Zstd.compress(source, 3);
                    out.writeVInt(compressed.length);
                    out.writeBytes(compressed, 0, compressed.length);
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public Decompressor newDecompressor() {
            return new EsZstdDecompressor();
        }
    }
}
