package de.sloow.gefrierschrank.lucene10;

import com.github.luben.zstd.Zstd;
import de.sloow.gefrierschrank.core.codec.ZstdFrameDecoder;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Round-trips real zstd-compressed bytes (compressed with the same zstd-jni library ES's
 * native layer would produce frames for) through {@link EsZstdDecompressor}, matching the
 * on-disk framing confirmed via javap against ES's real ZstdDecompressor: a VInt compressed
 * length, then that many raw zstd-frame bytes. Runs once with the default decoder selection and
 * once with the forced pure-Java decoder.
 */
class EsZstdDecompressorTest {

    @AfterEach
    void clearDecoderProperty() {
        System.clearProperty(ZstdFrameDecoder.DECODER_PROPERTY);
    }

    private static EsZstdDecompressor decompressor(String decoder) {
        if (decoder.equals("java")) {
            System.setProperty(ZstdFrameDecoder.DECODER_PROPERTY, "java");
        } else {
            System.clearProperty(ZstdFrameDecoder.DECODER_PROPERTY);
        }
        return new EsZstdDecompressor();
    }

    private static byte[] frame(byte[] original) throws java.io.IOException {
        return block(Zstd.compress(original));
    }

    private static byte[] block(byte[] compressed) throws java.io.IOException {
        byte[] out = new byte[5 + compressed.length];
        ByteArrayDataOutput output = new ByteArrayDataOutput(out);
        output.writeVInt(compressed.length);
        output.writeBytes(compressed, 0, compressed.length);
        return Arrays.copyOf(out, output.getPosition());
    }

    @ParameterizedTest
    @ValueSource(strings = {"auto", "java"})
    void decompressesFullBlock(String decoder) throws Exception {
        byte[] original = "the quick brown fox jumps over the lazy dog".repeat(20).getBytes(StandardCharsets.UTF_8);
        byte[] wire = frame(original);

        BytesRef bytes = new BytesRef();
        decompressor(decoder).decompress(new ByteArrayDataInput(wire), original.length, 0, original.length, bytes);

        assertArrayEquals(original, Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length));
    }

    @ParameterizedTest
    @ValueSource(strings = {"auto", "java"})
    void slicesRequestedOffsetAndLengthOutOfTheDecompressedBlock(String decoder) throws Exception {
        byte[] original = "0123456789".repeat(50).getBytes(StandardCharsets.UTF_8);
        byte[] wire = frame(original);

        int offset = 137;
        int length = 42;
        BytesRef bytes = new BytesRef();
        decompressor(decoder).decompress(new ByteArrayDataInput(wire), original.length, offset, length, bytes);

        byte[] expected = Arrays.copyOfRange(original, offset, offset + length);
        byte[] actual = Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length);
        assertArrayEquals(expected, actual);
    }

    @ParameterizedTest
    @ValueSource(strings = {"auto", "java"})
    void zeroLengthOriginalYieldsEmptySlice(String decoder) throws Exception {
        BytesRef bytes = new BytesRef(new byte[]{1, 2, 3});
        decompressor(decoder).decompress(new ByteArrayDataInput(new byte[0]), 0, 0, 0, bytes);

        assertEquals(0, bytes.offset);
        assertEquals(0, bytes.length);
    }

    @ParameterizedTest
    @ValueSource(strings = {"auto", "java"})
    void cloneProducesAnIndependentUsableInstance(String decoder) throws Exception {
        byte[] original = "clone me please".getBytes(StandardCharsets.UTF_8);
        byte[] wire = frame(original);

        Decompressor clone = decompressor(decoder).clone();
        BytesRef bytes = new BytesRef();
        clone.decompress(new ByteArrayDataInput(wire), original.length, 0, original.length, bytes);

        assertArrayEquals(original, Arrays.copyOfRange(bytes.bytes, bytes.offset, bytes.offset + bytes.length));
    }

    @ParameterizedTest
    @ValueSource(strings = {"auto", "java"})
    void truncatedFrameIsCorruptIndex(String decoder) throws Exception {
        byte[] original = "a truncated frame must never pass as a block ".repeat(40).getBytes(StandardCharsets.UTF_8);
        byte[] compressed = Zstd.compress(original);
        byte[] wire = block(Arrays.copyOf(compressed, compressed.length - 3));

        assertThrows(CorruptIndexException.class, () -> decompressor(decoder)
            .decompress(new ByteArrayDataInput(wire), original.length, 0, original.length, new BytesRef()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"auto", "java"})
    void decompressedLengthMismatchIsCorruptIndex(String decoder) throws Exception {
        byte[] original = "length mismatch ".repeat(40).getBytes(StandardCharsets.UTF_8);
        byte[] wire = frame(original);

        // Declared longer than the frame content: decodes, then fails the exact length check.
        assertThrows(CorruptIndexException.class, () -> decompressor(decoder)
            .decompress(new ByteArrayDataInput(wire), original.length + 7, 0, 10, new BytesRef()));
        // Declared shorter: the content does not fit, which the decoder reports as malformed.
        assertThrows(CorruptIndexException.class, () -> decompressor(decoder)
            .decompress(new ByteArrayDataInput(wire), original.length - 7, 0, 10, new BytesRef()));
    }
}
