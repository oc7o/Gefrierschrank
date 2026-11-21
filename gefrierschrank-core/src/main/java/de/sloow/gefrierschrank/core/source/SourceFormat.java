package de.sloow.gefrierschrank.core.source;

/**
 * The content type of a stored {@code _source}, detected as Elasticsearch detects it. Elasticsearch
 * stores the request body in the request's content type, so an index written by a SMILE, CBOR or
 * YAML client holds documents in that format.
 */
public enum SourceFormat {
    JSON("stored"),
    SMILE("stored-smile"),
    CBOR("stored-cbor"),
    YAML("stored-yaml"),
    /** Elasticsearch cannot tell either; the JSON path decides (and fails it). */
    UNKNOWN("stored");

    /** Elasticsearch's stream detection looks at this many bytes after the leading whitespace. */
    private static final int DETECTION_WINDOW = 20;

    private final String storedKind;

    SourceFormat(String storedKind) {
        this.storedKind = storedKind;
    }

    /** The {@code _source_kind} of a row holding a document stored in this format. */
    public String storedKind() {
        return storedKind;
    }

    /** True for the formats that are converted to JSON instead of exported as stored. */
    public boolean converted() {
        return this == SMILE || this == CBOR || this == YAML;
    }

    /**
     * Elasticsearch 9.3's {@code XContentFactory.xContentType(InputStream)} (x-content 9.3.0)
     * over {@code bytes[0, length)}: skip {@code Character.isWhitespace} bytes, then apply
     * {@code xContentType(byte[], int, int)} to at most the next 20 bytes. Its rules, in order:
     * {@code JsonXContentImpl.detectContent} ({@code '{'}), {@code SmileXContentImpl} ({@code ":)\n"}),
     * {@code YamlXContentImpl} ({@code "---"}), {@code CborXContentImpl} (0xBF with a second byte,
     * the self-describe tag D9 D9 F7, any major type 5 map header), then a UTF-8 byte order mark
     * followed by whitespace and {@code '{'} is JSON, anything else is unknown.
     */
    public static SourceFormat detect(byte[] bytes, int length) {
        if (bytes == null) return UNKNOWN;
        length = Math.min(length, bytes.length);
        if (length <= 0) return UNKNOWN;
        if (bytes[0] == '{') return JSON;
        int o = 0;
        while (o < length && Character.isWhitespace((char) (bytes[o] & 0xFF))) o++;
        int n = Math.min(DETECTION_WINDOW, length - o);
        if (n <= 0) return UNKNOWN;
        byte first = bytes[o];
        if (first == '{') return JSON;
        if (n > 2 && first == ':' && bytes[o + 1] == ')' && bytes[o + 2] == '\n') return SMILE;
        if (n > 2 && first == '-' && bytes[o + 1] == '-' && bytes[o + 2] == '-') return YAML;
        if (first == (byte) 0xBF && n > 1) return CBOR;
        if (majorType(first) == 6 && n > 2
            && first == (byte) 0xD9 && bytes[o + 1] == (byte) 0xD9 && bytes[o + 2] == (byte) 0xF7) return CBOR;
        if (majorType(first) == 5) return CBOR;
        int i = n > 3 && first == (byte) 0xEF && bytes[o + 1] == (byte) 0xBB && bytes[o + 2] == (byte) 0xBF ? 3 : 0;
        // Signed bytes, as Elasticsearch's Character.isWhitespace(int) sees them: no byte >= 0x80 is whitespace.
        for (; i < n; i++) {
            byte b = bytes[o + i];
            if (b == '{') return JSON;
            if (!Character.isWhitespace(b)) break;
        }
        return UNKNOWN;
    }

    private static int majorType(byte b) {
        return (b >> 5) & 0x7;
    }
}
