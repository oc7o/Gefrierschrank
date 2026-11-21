package de.sloow.gefrierschrank.lucene10;

import de.sloow.gefrierschrank.core.codec.ZstdFrameDecoder;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Decompresses the ZSTD blocks that Elasticsearch writes for {@code _source} (and other
 * stored fields), following the same read pattern as Lucene's own
 * {@code CompressionMode$DeflateDecompressor}: read the whole block, then slice the
 * requested [offset, offset+length) window out of it. Confirmed against ES's real
 * ZstdDecompressor via javap - it reads a VInt compressed length, then that many raw
 * zstd-frame bytes, then decompresses the full frame before windowing.
 *
 * <p>Frames are decoded by {@link ZstdFrameDecoder}: zstd-jni where its native library loads,
 * pure Java otherwise (noexec {@code java.io.tmpdir}).
 */
final class EsZstdDecompressor extends Decompressor {

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
        return new EsZstdDecompressor();
    }
}
