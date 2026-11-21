package de.sloow.gefrierschrank.lucene10;

import com.github.luben.zstd.Zstd;
import de.sloow.gefrierschrank.core.codec.ZstdFrameDecoder;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Builds a real Lucene segment whose stored fields are compressed with actual ZSTD frames
 * (via zstd-jni) and reads it back through the unmodified, production {@link EsZstdDecompressor}
 * - proof against real fdt/fdx/fdm files, not just an isolated byte-array round trip.
 *
 * <p>The production {@link EsZstdCompressionMode} deliberately can't write (this tool is
 * read-only), so this test supplies its own working compressor purely to get bytes onto disk;
 * decompression goes through the real production class unchanged. Runs once with the default
 * decoder selection and once with the forced pure-Java decoder.
 */
class EsZstdStoredFieldsRoundTripTest {

    @AfterEach
    void clearDecoderProperty() {
        System.clearProperty(ZstdFrameDecoder.DECODER_PROPERTY);
    }

    @ParameterizedTest
    @ValueSource(strings = {"auto", "java"})
    void readsRealZstdCompressedStoredFieldsThroughTheProductionDecompressor(String decoder) throws IOException {
        if (decoder.equals("java")) {
            System.setProperty(ZstdFrameDecoder.DECODER_PROPERTY, "java");
        }
        int docCount = 60;
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig().setCodec(new TestWritableZstdCodec());
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (int i = 0; i < docCount; i++) {
                    Document doc = new Document();
                    doc.add(new StoredField("_source", source(i).getBytes(StandardCharsets.UTF_8)));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(docCount, reader.maxDoc());
                var storedFields = reader.storedFields();

                // out-of-order access forces the reader to seek into the middle of decompressed
                // blocks, exercising the offset/length windowing in EsZstdDecompressor.
                for (int docId : new int[]{55, 0, 30, 1, 59}) {
                    var value = storedFields.document(docId).getField("_source").binaryValue();
                    String json = new String(value.bytes, value.offset, value.length, StandardCharsets.UTF_8);
                    assertEquals(source(docId), json, "doc " + docId);
                }
            }
        }
    }

    private static String source(int doc) {
        return "{\"doc\":" + doc + ",\"payload\":\"" + "x".repeat(500) + "\"}";
    }

    /** Public with a public no-arg constructor so Lucene's SPI loader can instantiate it; registered in test resources. */
    public static final class TestWritableZstdCodec extends FilterCodec {
        public TestWritableZstdCodec() {
            super("GefrierschrankTestZstdRoundTrip", new Lucene103Codec());
        }

        @Override
        public StoredFieldsFormat storedFieldsFormat() {
            return new Lucene90CompressingStoredFieldsFormat(
                    "ZstdStoredFields814", TestWritableZstdCompressionMode.INSTANCE, 14336, 128, 10);
        }
    }

    /** Real compressor for writing; decompression delegates to the actual production class. */
    private static final class TestWritableZstdCompressionMode extends CompressionMode {
        static final TestWritableZstdCompressionMode INSTANCE = new TestWritableZstdCompressionMode();

        @Override
        public Compressor newCompressor() {
            return new Compressor() {
                @Override
                public void compress(ByteBuffersDataInput in, DataOutput out) throws IOException {
                    byte[] src = new byte[(int) in.length()];
                    in.readBytes(src, 0, src.length);
                    byte[] compressed = Zstd.compress(src);
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
