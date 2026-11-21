package de.sloow.gefrierschrank.delta;

import org.apache.avro.util.Utf8;

/** String values for the Parquet writer, handed over as UTF-8 bytes. */
final class AvroStrings {

    private AvroStrings() {}

    /**
     * parquet-avro copies {@link Utf8} bytes as they are but runs a {@code CharsetEncoder} over every
     * {@link String}. {@code new Utf8(String)} would silently turn an unpaired surrogate into '?',
     * where the encoder path failed the shard, so such values are still rejected here. The message
     * is fixed because the value is document content.
     */
    static Utf8 utf8(String value) {
        for (int i = 0, n = value.length(); i < n; i++) {
            char c = value.charAt(i);
            if (!Character.isSurrogate(c)) continue;
            if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(value.charAt(i + 1))) {
                i++;
                continue;
            }
            throw new IllegalArgumentException("string value is not valid Unicode");
        }
        return new Utf8(value);
    }
}
