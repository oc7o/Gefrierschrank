package de.sloow.gefrierschrank.delta;

/**
 * Fast acceptance check for a stored {@code _source} on the archive byte row path. It accepts only
 * bytes that the legacy per-document path accepts unaltered: decoding them as UTF-8 and running
 * {@link ArchiveContext#validateSource} succeeds, and the decoded text encodes back to exactly these
 * bytes. Everything else returns false, and the caller runs the legacy path for that one document,
 * so a rejection here never changes an outcome, only its cost.
 *
 * <p>Accepted: one RFC 8259 object with nothing but SP, HT, LF or CR around it, no byte order
 * mark, at most {@link #MAX_DEPTH} nested containers, strict UTF-8 (no overlong forms, no encoded
 * surrogates, nothing above U+10FFFF), the eight single-character escapes and {@code \}{@code uXXXX},
 * the full number grammar without leading zeros, and the literals true, false and null. Field names
 * are capped at {@link #MAX_NAME_BYTES} raw bytes: Jackson's name limit counts UTF-16 units, and a
 * name never has more of those than bytes.
 *
 * <p>Iterative, so deep nesting cannot overflow the stack. Never throws for content and allocates
 * nothing per call. Not thread-safe: one instance per shard or range.
 */
final class SourceValidator {

    /** Jackson's default nesting limit, which DocumentJson keeps. */
    static final int MAX_DEPTH = 1000;
    /** Jackson's default maxNameLength, which DocumentJson keeps. */
    static final int MAX_NAME_BYTES = 50_000;

    private static final byte[] TRUE = {'t', 'r', 'u', 'e'};
    private static final byte[] FALSE = {'f', 'a', 'l', 's', 'e'};
    private static final byte[] NULL = {'n', 'u', 'l', 'l'};

    private final boolean[] inArray = new boolean[MAX_DEPTH];
    private byte[] b;
    private int n;
    private int i;

    /** True only if {@code bytes[0, length)} is a source the legacy path accepts unaltered. */
    boolean accepts(byte[] bytes, int length) {
        if (bytes == null || length < 0 || length > bytes.length) return false;
        b = bytes;
        n = length;
        i = 0;
        try {
            return document();
        } finally {
            b = null;
        }
    }

    private boolean document() {
        skipWhitespace();
        if (i >= n || b[i] != '{') return false;
        int top = -1;
        while (true) {
            skipWhitespace();
            if (i >= n) return false;
            int c = b[i];
            if (c == '{' || c == '[') {
                if (++top == MAX_DEPTH) return false;
                boolean array = c == '[';
                inArray[top] = array;
                i++;
                skipWhitespace();
                if (i >= n) return false;
                if (b[i] == (array ? ']' : '}')) {
                    i++;
                    top--;
                } else {
                    if (!array && !name()) return false;
                    continue;
                }
            } else if (c == '"') {
                i++;
                if (!string()) return false;
            } else if (c == 't') {
                if (!literal(TRUE)) return false;
            } else if (c == 'f') {
                if (!literal(FALSE)) return false;
            } else if (c == 'n') {
                if (!literal(NULL)) return false;
            } else if (!number()) {
                return false;
            }
            // After a value: close containers until a comma leads to the next value.
            while (true) {
                skipWhitespace();
                if (top < 0) return i == n;
                if (i >= n) return false;
                int d = b[i++];
                if (d == ',') {
                    if (!inArray[top]) {
                        skipWhitespace();
                        if (!name()) return false;
                    }
                    break;
                }
                if (d != (inArray[top] ? ']' : '}')) return false;
                top--;
            }
        }
    }

    /** A field name and its colon. */
    private boolean name() {
        if (i >= n || b[i] != '"') return false;
        int start = ++i;
        if (!string()) return false;
        if (i - 1 - start > MAX_NAME_BYTES) return false;
        skipWhitespace();
        if (i >= n || b[i] != ':') return false;
        i++;
        return true;
    }

    private void skipWhitespace() {
        while (i < n) {
            int c = b[i];
            if (c == ' ' || c == '\n' || c == '\r' || c == '\t') i++;
            else return;
        }
    }

    private boolean literal(byte[] word) {
        if (n - i < word.length) return false;
        for (int k = 0; k < word.length; k++) {
            if (b[i + k] != word[k]) return false;
        }
        i += word.length;
        return true;
    }

    private boolean number() {
        if (b[i] == '-' && ++i >= n) return false;
        int c = b[i];
        if (c == '0') {
            i++;
        } else if (c >= '1' && c <= '9') {
            i++;
            skipDigits();
        } else {
            return false;
        }
        if (i < n && b[i] == '.') {
            int start = ++i;
            skipDigits();
            if (i == start) return false;
        }
        if (i < n && (b[i] == 'e' || b[i] == 'E')) {
            i++;
            if (i < n && (b[i] == '+' || b[i] == '-')) i++;
            int start = i;
            skipDigits();
            if (i == start) return false;
        }
        return true;
    }

    private void skipDigits() {
        while (i < n && b[i] >= '0' && b[i] <= '9') i++;
    }

    /** The rest of a string after its opening quote, including the closing quote. */
    private boolean string() {
        while (true) {
            // Bytes >= 0x80 are negative here, so the ASCII run stops at them too.
            while (i < n) {
                int c = b[i];
                if (c >= 0x20 && c != '"' && c != '\\') i++;
                else break;
            }
            if (i >= n) return false;
            int c = b[i] & 0xFF;
            if (c == '"') {
                i++;
                return true;
            }
            if (c == '\\') {
                if (!escape()) return false;
            } else if (c < 0x80 || !multiByte(c)) {
                return false;
            }
        }
    }

    private boolean escape() {
        if (i + 1 >= n) return false;
        switch (b[i + 1]) {
            case '"', '\\', '/', 'b', 'f', 'n', 'r', 't' -> {
                i += 2;
                return true;
            }
            case 'u' -> {
                if (n - i < 6) return false;
                for (int k = i + 2; k < i + 6; k++) {
                    if (!isHexDigit(b[k])) return false;
                }
                i += 6;
                return true;
            }
            default -> {
                return false;
            }
        }
    }

    private static boolean isHexDigit(byte c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    /** One strict UTF-8 sequence starting with lead byte {@code c} at {@code i}. */
    private boolean multiByte(int c) {
        if (c >= 0xC2 && c <= 0xDF) {
            if (!continuation(i + 1)) return false;
            i += 2;
            return true;
        }
        if (c >= 0xE0 && c <= 0xEF) {
            // E0 excludes overlong forms, ED excludes the UTF-16 surrogate range.
            int low = c == 0xE0 ? 0xA0 : 0x80;
            int high = c == 0xED ? 0x9F : 0xBF;
            if (!secondByte(low, high) || !continuation(i + 2)) return false;
            i += 3;
            return true;
        }
        if (c >= 0xF0 && c <= 0xF4) {
            // F0 excludes overlong forms, F4 excludes code points above U+10FFFF.
            int low = c == 0xF0 ? 0x90 : 0x80;
            int high = c == 0xF4 ? 0x8F : 0xBF;
            if (!secondByte(low, high) || !continuation(i + 2) || !continuation(i + 3)) return false;
            i += 4;
            return true;
        }
        return false;
    }

    private boolean secondByte(int low, int high) {
        if (i + 1 >= n) return false;
        int second = b[i + 1] & 0xFF;
        return second >= low && second <= high;
    }

    private boolean continuation(int at) {
        return at < n && (b[at] & 0xC0) == 0x80;
    }
}
