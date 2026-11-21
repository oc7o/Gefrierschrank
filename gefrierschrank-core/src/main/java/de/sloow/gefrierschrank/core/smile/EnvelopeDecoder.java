package de.sloow.gefrierschrank.core.smile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.Inflater;

/**
 * Decodes ES snapshot-repository metadata blobs ({@code snap-*.dat}, {@code meta-*.dat},
 * shard {@code index-*} files) written by {@code ChecksumBlobStoreFormat}.
 *
 * <p>Layout, determined empirically against ES 7.17.9 / 8.17.0 / 9.3.0 output and
 * cross-checked with OpenSearch's writer implementation:
 * <pre>
 *   Lucene header   : magic 0x3FD76C17, codec name, codec version (v1)
 *   Compression tag : "DFL\0" — LUCENE_WITH_DEFLATE, followed by a raw-deflate stream
 *   Footer          : 16 bytes — footer magic 0xC02893E8 (inverted 0x3FD76C17),
 *                     algid 0, int64 CRC32 over every preceding byte
 * </pre>
 *
 * <p>ES composes CodecUtil.header(codec, v1) + compress(payload) + its own footer,
 * so the footer magic is bitwise-inverted relative to a plain Lucene footer and the
 * CRC is computed as int32-CRC32 (zero-extended to int64) rather than CRC64.
 */
public final class EnvelopeDecoder {

    private static final int LUCENE_HEADER_MAGIC = 0x3FD76C17;
    private static final int ES_FOOTER_MAGIC = 0xC02893E8;
    private static final int FOOTER_LENGTH = 16;

    private EnvelopeDecoder() {}

    public static Envelope decode(byte[] data) throws IOException {
        requireLength(data, 9 + FOOTER_LENGTH);
        checkHeader(data);

        int nameLen = data[4] & 0xFF;
        String codecName = new String(data, 5, nameLen, StandardCharsets.UTF_8);
        int codecVersion = intBE(data, 5 + nameLen);
        int pos = 5 + nameLen + 4;

        byte compressionMode = 0;
        boolean compressed = startsWithDeflateTag(data, pos);
        if (compressed) {
            compressionMode = data[pos + 3];
            pos += 4;
        }

        int contentEnd = data.length - FOOTER_LENGTH;
        byte[] content = compressed
            ? inflate(data, pos, contentEnd)
            : Arrays.copyOfRange(data, pos, contentEnd);

        verifyFooter(data, contentEnd);

        return new Envelope(codecName, codecVersion, compressionMode, content);
    }

    private static void checkHeader(byte[] data) throws IOException {
        if (intBE(data, 0) != LUCENE_HEADER_MAGIC) {
            throw new IOException("Not a Lucene-header file (magic 0x%08X)".formatted(intBE(data, 0)));
        }
    }

    private static boolean startsWithDeflateTag(byte[] data, int pos) {
        return pos + 4 <= data.length
            && data[pos] == 'D' && data[pos + 1] == 'F' && data[pos + 2] == 'L' && data[pos + 3] == 0;
    }

    private static byte[] inflate(byte[] data, int from, int to) throws IOException {
        Inflater inflater = new Inflater(true);
        try {
            inflater.setInput(data, from, to - from);
            byte[] buffer = new byte[Math.max(64, (to - from) * 4)];
            int total = 0;
            while (!inflater.finished()) {
                if (total == buffer.length) {
                    buffer = Arrays.copyOf(buffer, buffer.length * 2);
                }
                int n = inflater.inflate(buffer, total, buffer.length - total);
                if (n == 0 && inflater.needsInput()) {
                    throw new IOException("Deflate stream truncated");
                }
                total += n;
            }
            return Arrays.copyOf(buffer, total);
        } catch (java.util.zip.DataFormatException e) {
            throw new IOException("Corrupt deflate stream", e);
        } finally {
            inflater.end();
        }
    }

    private static void verifyFooter(byte[] data, int footerStart) throws IOException {
        if (intBE(data, footerStart) != ES_FOOTER_MAGIC) {
            throw new IOException("Missing ES footer magic at offset %d".formatted(footerStart));
        }
        // Unlike standard Lucene (CRC over [0, footerStart)), ES's composed footer
        // checksums the footer prefix (magic + algid) as well: CRC covers [0, len-8).
        CRC32 crc = new CRC32();
        crc.update(data, 0, footerStart + 8);
        long stored = longBE(data, footerStart + 8);
        if (crc.getValue() != stored) {
            throw new IOException("Checksum mismatch: stored=%d computed=%d".formatted(stored, crc.getValue()));
        }
    }

    private static int intBE(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24) | ((b[off + 1] & 0xFF) << 16)
            | ((b[off + 2] & 0xFF) << 8) | (b[off + 3] & 0xFF);
    }

    private static long longBE(byte[] b, int off) {
        return ((long) intBE(b, off) << 32) | (intBE(b, off + 4) & 0xFFFFFFFFL);
    }

    private static void requireLength(byte[] data, int min) throws IOException {
        if (data.length < min) throw new IOException("File too short: %d bytes".formatted(data.length));
    }
}
