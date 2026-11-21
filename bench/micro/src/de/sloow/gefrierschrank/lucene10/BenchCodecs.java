package de.sloow.gefrierschrank.lucene10;

import com.github.luben.zstd.Zstd;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 * Writable stand-ins for ES's Zstd814StoredFieldsFormat presets (level/block/docs taken from the
 * real ES class via javap). Reads go through the production EsZstdDecompressor, same as the
 * project's EsZstdStoredFieldsRoundTripTest.
 */
public final class BenchCodecs {
    private BenchCodecs() {}

    public static final class ZstdBestSpeed extends FilterCodec {
        public ZstdBestSpeed() { super("GsBenchZstdBestSpeed", new Lucene103Codec()); }

        @Override
        public StoredFieldsFormat storedFieldsFormat() {
            return new Lucene90CompressingStoredFieldsFormat("ZstdStoredFields814", new Mode(1), 14336, 128, 10);
        }
    }

    public static final class ZstdBestCompression extends FilterCodec {
        public ZstdBestCompression() { super("GsBenchZstdBestCompression", new Lucene103Codec()); }

        @Override
        public StoredFieldsFormat storedFieldsFormat() {
            return new Lucene90CompressingStoredFieldsFormat("ZstdStoredFields814", new Mode(3), 245760, 2048, 10);
        }
    }

    static final class Mode extends CompressionMode {
        private final int level;

        Mode(int level) { this.level = level; }

        @Override
        public Compressor newCompressor() {
            return new Compressor() {
                @Override
                public void compress(ByteBuffersDataInput in, DataOutput out) throws IOException {
                    byte[] src = new byte[(int) in.length()];
                    in.readBytes(src, 0, src.length);
                    byte[] compressed = Zstd.compress(src, level);
                    out.writeVInt(compressed.length);
                    out.writeBytes(compressed, 0, compressed.length);
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public Decompressor newDecompressor() { return new EsZstdDecompressor(); }
    }
}
