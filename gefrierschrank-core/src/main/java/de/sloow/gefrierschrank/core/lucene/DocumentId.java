package de.sloow.gefrierschrank.core.lucene;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/** Decodes ES 7+ stored _id: UTF-8 marker, packed decimal, or URL-safe base64. */
public final class DocumentId {
    private DocumentId() {}
    public static String decode(byte[] bytes, int offset, int length) throws IOException {
        if (length < 1) throw new IOException("Empty stored document ID");
        int marker = bytes[offset] & 255;
        if (marker == 255) return new String(bytes, offset + 1, length - 1, StandardCharsets.UTF_8);
        if (marker == 254) {
            StringBuilder digits = new StringBuilder();
            for (int i = offset + 1; i < offset + length; i++) {
                int high = (bytes[i] & 255) >>> 4, low = bytes[i] & 15;
                if (high > 9 || (low > 9 && !(low == 15 && i == offset + length - 1)))
                    throw new IOException("Invalid packed document ID");
                digits.append((char) ('0' + high));
                if (low != 15) digits.append((char) ('0' + low));
            }
            return digits.toString();
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(
            Arrays.copyOfRange(bytes, offset + (marker == 253 ? 1 : 0), offset + length));
    }
}
