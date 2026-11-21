package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Turns Elasticsearch's rendering of a reconstructed {@code _source} into the exact text 1.0.0
 * exported, without 1.0.0's detour through a {@code Map}.
 *
 * <p>1.0.0 parsed ES's bytes into an ordered map ({@code Source.source()}, i.e.
 * {@code XContentHelper.convertToMap(bytes, true)}) and wrote the map with
 * {@code new ObjectMapper().writeValueAsString}. Copying the parser's tokens straight into the same
 * kind of generator yields the same text: the same Java number types (int, long and BigInteger by
 * the parsed number type, every float as a double), strings decoded and re-escaped by the same
 * generator, the same key order and no whitespace. The generator must write chars: ES escapes
 * characters outside the BMP as surrogate-pair escapes, the map path wrote them as raw characters,
 * and Jackson's UTF-8 generator would escape them again.
 *
 * <p>Duplicate keys failed 1.0.0's map conversion (ES parses with strict duplicate detection), so
 * they fail here too rather than silently keeping one value. Parse errors never quote the document,
 * and no message says "synthetic source": that phrase is the fast path's reconstruction trigger.
 */
final class SyntheticSourceJson {

    /** DocumentJson's limits (only nesting depth bounded, at the legacy writer's 1000). */
    private static final JsonFactory PARSERS = JsonFactory.builder()
        .streamReadConstraints(StreamReadConstraints.builder()
            .maxStringLength(Integer.MAX_VALUE)
            .maxNumberLength(Integer.MAX_VALUE)
            .maxNameLength(Integer.MAX_VALUE)
            .build())
        .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .disable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
        .build();

    /** The factory behind 1.0.0's {@code new ObjectMapper().writeValueAsString}. */
    private static final JsonFactory GENERATORS = new ObjectMapper().getFactory();

    private SyntheticSourceJson() {}

    /** The export text as a Java string (lone surrogates kept, like 1.0.0's string). */
    static String toExportText(byte[] esJson) throws IOException {
        if (esJson.length == 0) {
            return "{}";  // ES's Source.fromBytes treats empty bytes as an empty source
        }
        StringWriter text = new StringWriter(esJson.length + 16);
        try (JsonParser parser = PARSERS.createParser(esJson);
             JsonGenerator generator = GENERATORS.createGenerator(text)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("reconstructed _source is not a JSON object");
            }
            generator.copyCurrentStructure(parser);
            if (parser.nextToken() != null) {
                throw new IOException("reconstructed _source has trailing content");
            }
        }
        return text.toString();
    }

    /** The export text as UTF-8, exactly the bytes 1.0.0 handed to the archive writer. */
    static byte[] toExportJson(byte[] esJson) throws IOException {
        byte[] unchanged = fastPath(esJson);
        return unchanged != null ? unchanged : toExportText(esJson).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Elasticsearch's own bytes when they provably equal the export text, otherwise {@code null}.
     *
     * <p>They do exactly when the char path could change nothing, which one strict pass over the
     * bytes proves: a single object and nothing after it, in JSON's grammar; no whitespace outside
     * strings; strings of well-formed UTF-8 using only the escapes the char generator writes
     * ({@code \" \\ \b \f \n \r \t}, and <code>&#92;u00XX</code> with upper-case hex for the
     * other control characters), so no surrogate escape (ES writes those for characters outside the
     * BMP); no duplicate names in an object (each string has exactly one such encoding, so equal
     * names are equal bytes); no integer {@code -0}; and every number with a fraction or exponent
     * finite and written exactly as {@link Double#toString} writes {@link Double#parseDouble} of it.
     * Such a document parses with {@code PARSERS} and the generator writes every token back as it
     * was. Anything else, including very deep documents, takes the char path, which also reports
     * every failure.
     *
     * <p>A dedicated pass rather than a Jackson parse, which with duplicate detection alone costs about
     * half of what the char path does.
     */
    static byte[] fastPath(byte[] esJson) {
        return esJson.length > 0 && esJson[0] == '{' && new CanonicalText(esJson).document() ? esJson : null;
    }

    /** One strict pass over one document; see {@link #fastPath}. */
    private static final class CanonicalText {

        /** Deeper documents take the char path: the generator's nesting limit then decides, as before. */
        private static final int MAX_DEPTH = 100;
        /** Objects of up to this many names compare each new name with the earlier ones; wider ones hash them. */
        private static final int LINEAR_NAMES = 16;
        private static final boolean[] PLAIN = new boolean[256];

        static {
            for (int c = ' '; c < 0x80; c++) {
                PLAIN[c] = c != '"' && c != '\\';
            }
        }

        private final byte[] json;
        private final int length;
        private int pos;
        private int depth;
        // Names of the objects being checked, innermost last, as (start, end) pairs.
        private int[] names = new int[32];
        private int namesTop;

        CanonicalText(byte[] json) {
            this.json = json;
            this.length = json.length;
        }

        boolean document() {
            return object() && pos == length;
        }

        private boolean object() {
            if (++depth > MAX_DEPTH) return false;
            pos++;
            if (pos < length && json[pos] == '}') {
                pos++;
                depth--;
                return true;
            }
            int first = namesTop;
            WideObjectNames wide = null;
            while (true) {
                if (pos >= length || json[pos] != '"') return false;
                int nameStart = pos + 1;
                if (!string()) return false;
                int nameEnd = pos - 1;
                if (wide == null && namesTop - first == 2 * LINEAR_NAMES) wide = new WideObjectNames(first);
                boolean distinct = wide == null ? newName(first, nameStart, nameEnd) : wide.add(nameStart, nameEnd);
                if (!distinct) return false;
                if (namesTop + 2 > names.length) names = Arrays.copyOf(names, names.length * 2);
                names[namesTop++] = nameStart;
                names[namesTop++] = nameEnd;
                if (pos >= length || json[pos] != ':') return false;
                pos++;
                if (!value() || pos >= length) return false;
                byte next = json[pos++];
                if (next == '}') {
                    namesTop = first;
                    depth--;
                    return true;
                }
                if (next != ',') return false;
            }
        }

        /** Objects hold a few names, so comparing length and last byte first beats hashing every name. */
        private boolean newName(int first, int start, int end) {
            int length = end - start;
            for (int name = first; name < namesTop; name += 2) {
                int otherEnd = names[name + 1];
                if (otherEnd - names[name] == length
                        && (length == 0 || json[otherEnd - 1] == json[end - 1])
                        && Arrays.equals(json, names[name], otherEnd, json, start, end)) {
                    return false;
                }
            }
            return true;
        }

        private int hash(int start, int end) {
            int hash = 0;
            for (int i = start; i < end; i++) hash = 31 * hash + json[i];
            return hash;
        }

        /**
         * The names of one object wider than {@link #LINEAR_NAMES}, in an open-addressing table, so
         * finding a duplicate stays linear in the object's width. Holds indexes of the object's names
         * on the {@code names} stack; each name's hash is kept for probing and growing.
         */
        private final class WideObjectNames {
            /** A longer probe sequence means colliding hashes; the char path then decides. */
            private static final int MAX_PROBES = 128;

            private final int first;
            private int[] hashes = new int[4 * LINEAR_NAMES];
            // Name number + 1 per slot, 0 for an empty slot; at most half full.
            private int[] slots = new int[4 * LINEAR_NAMES];
            private int shift = Integer.numberOfLeadingZeros(slots.length - 1);
            private int count;

            /** Takes over the object's first names, already checked to be distinct. */
            WideObjectNames(int first) {
                this.first = first;
                for (int name = first; name < namesTop; name += 2) {
                    hashes[count] = hash(names[name], names[name + 1]);
                    slots[freeSlot(hashes[count])] = ++count;
                }
            }

            /** Adds the name that is about to be pushed, or returns false for a duplicate or colliding hashes. */
            boolean add(int start, int end) {
                if (2 * (count + 1) > slots.length) grow();
                int hash = hash(start, end);
                int length = end - start;
                int mask = slots.length - 1;
                int slot = (hash * 0x9E3779B9) >>> shift;
                for (int probes = 0; slots[slot] != 0; slot = (slot + 1) & mask) {
                    int other = slots[slot] - 1;
                    if (hashes[other] == hash) {
                        int otherStart = names[first + 2 * other];
                        int otherEnd = names[first + 2 * other + 1];
                        if (otherEnd - otherStart == length && Arrays.equals(json, otherStart, otherEnd, json, start, end)) {
                            return false;
                        }
                    }
                    if (++probes == MAX_PROBES) return false;
                }
                if (count == hashes.length) hashes = Arrays.copyOf(hashes, 2 * count);
                hashes[count] = hash;
                slots[slot] = ++count;
                return true;
            }

            private void grow() {
                slots = new int[2 * slots.length];
                shift--;
                for (int name = 0; name < count; name++) slots[freeSlot(hashes[name])] = name + 1;
            }

            private int freeSlot(int hash) {
                int mask = slots.length - 1;
                int slot = (hash * 0x9E3779B9) >>> shift;
                while (slots[slot] != 0) slot = (slot + 1) & mask;
                return slot;
            }
        }

        private boolean array() {
            if (++depth > MAX_DEPTH) return false;
            pos++;
            if (pos < length && json[pos] == ']') {
                pos++;
                depth--;
                return true;
            }
            while (true) {
                if (!value() || pos >= length) return false;
                byte next = json[pos++];
                if (next == ']') {
                    depth--;
                    return true;
                }
                if (next != ',') return false;
            }
        }

        private boolean value() {
            if (pos >= length) return false;
            return switch (json[pos]) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't' -> literal(TRUE);
                case 'f' -> literal(FALSE);
                case 'n' -> literal(NULL);
                default -> number();
            };
        }

        private static final byte[] TRUE = {'t', 'r', 'u', 'e'};
        private static final byte[] FALSE = {'f', 'a', 'l', 's', 'e'};
        private static final byte[] NULL = {'n', 'u', 'l', 'l'};

        private boolean literal(byte[] word) {
            if (!Arrays.equals(json, pos, Math.min(length, pos + word.length), word, 0, word.length)) return false;
            pos += word.length;
            return true;
        }

        /** JSON's number grammar; integers other than -0 as they are, others as {@link Double#toString} writes them. */
        private boolean number() {
            int start = pos;
            if (json[pos] == '-') pos++;
            if (pos >= length) return false;
            byte first = json[pos];
            if (first == '0') {
                pos++;
            } else if (first >= '1' && first <= '9') {
                pos++;
                skipDigits();
            } else {
                return false;
            }
            boolean integer = true;
            if (pos < length && json[pos] == '.') {
                pos++;
                if (!digits()) return false;
                integer = false;
            }
            if (pos < length && (json[pos] == 'e' || json[pos] == 'E')) {
                pos++;
                if (pos < length && (json[pos] == '+' || json[pos] == '-')) pos++;
                if (!digits()) return false;
                integer = false;
            }
            if (integer) {
                return !(pos - start == 2 && json[start] == '-' && json[start + 1] == '0');
            }
            if (shortPlainDecimal(start, pos)) return true;
            String text = new String(json, start, pos - start, StandardCharsets.ISO_8859_1);
            double value = Double.parseDouble(text);
            return Double.isFinite(value) && Double.toString(value).equals(text);
        }

        /**
         * A number {@link Double#toString} writes exactly as it is, decided without parsing it: no
         * exponent, a magnitude in [10^-3, 10^7) (where it writes plain notation), at most 15
         * significant digits and no trailing zero in the fraction other than a lone "0". Decimals of
         * at most 15 significant digits round-trip through a double, so no other decimal that short
         * rounds to the same double, and these digits are the shortest ones {@link Double#toString}
         * picks. Everything else takes the exact check.
         */
        private boolean shortPlainDecimal(int start, int end) {
            int integerStart = json[start] == '-' ? start + 1 : start;
            int point = integerStart;
            while (point < end && json[point] != '.') point++;
            if (point == end) return false;
            for (int i = point + 1; i < end; i++) {
                if (json[i] == 'e' || json[i] == 'E') return false;
            }
            int integerDigits = point - integerStart;
            int fractionDigits = end - point - 1;
            boolean trailingZero = json[end - 1] == '0';
            if (json[integerStart] != '0') {
                return integerDigits <= 7 && (trailingZero ? fractionDigits == 1 : integerDigits + fractionDigits <= 15);
            }
            if (fractionDigits == 1 && trailingZero) return true;  // 0.0 and -0.0
            int leadingZeros = 0;
            while (leadingZeros < fractionDigits && json[point + 1 + leadingZeros] == '0') leadingZeros++;
            return leadingZeros <= 2 && !trailingZero && fractionDigits - leadingZeros <= 15;
        }

        private boolean digits() {
            int start = pos;
            skipDigits();
            return pos > start;
        }

        private void skipDigits() {
            while (pos < length && json[pos] >= '0' && json[pos] <= '9') pos++;
        }

        /** From the opening quote to after the closing one. */
        private boolean string() {
            int i = pos + 1;
            while (true) {
                while (i < length && PLAIN[json[i] & 0xFF]) i++;
                if (i >= length) return false;
                int c = json[i++];
                if (c == '"') {
                    pos = i;
                    return true;
                }
                if (c == '\\') {
                    i = afterEscape(i);
                } else if (c < 0) {
                    i = afterUtf8(i - 1);
                } else {
                    return false;  // a raw control character
                }
                if (i < 0) return false;
            }
        }

        private int afterEscape(int i) {
            if (i >= length) return -1;
            switch (json[i]) {
                case '"', '\\', 'b', 'f', 'n', 'r', 't':
                    return i + 1;
                case 'u':
                    if (i + 4 >= length || json[i + 1] != '0' || json[i + 2] != '0') return -1;
                    int high = json[i + 3];
                    int low = json[i + 4];
                    int lowValue = low >= '0' && low <= '9' ? low - '0' : low >= 'A' && low <= 'F' ? low - 'A' + 10 : -1;
                    if ((high != '0' && high != '1') || lowValue < 0) return -1;
                    int value = (high - '0') << 4 | lowValue;
                    boolean hasShortEscape = value == '\b' || value == '\t' || value == '\n' || value == '\f' || value == '\r';
                    return hasShortEscape ? -1 : i + 5;
                default:
                    return -1;
            }
        }

        /** After the well-formed UTF-8 sequence at {@code i}, or -1: overlong, surrogate, beyond U+10FFFF, cut short. */
        private int afterUtf8(int i) {
            int lead = json[i] & 0xFF;
            if (lead >= 0xC2 && lead <= 0xDF) {
                return i + 1 < length && continuation(json[i + 1]) ? i + 2 : -1;
            }
            if (lead >= 0xE0 && lead <= 0xEF) {
                if (i + 2 >= length) return -1;
                int second = json[i + 1] & 0xFF;
                if ((lead == 0xE0 && second < 0xA0) || (lead == 0xED && second > 0x9F)) return -1;
                return continuation(json[i + 1]) && continuation(json[i + 2]) ? i + 3 : -1;
            }
            if (lead >= 0xF0 && lead <= 0xF4) {
                if (i + 3 >= length) return -1;
                int second = json[i + 1] & 0xFF;
                if ((lead == 0xF0 && second < 0x90) || (lead == 0xF4 && second > 0x8F)) return -1;
                return continuation(json[i + 1]) && continuation(json[i + 2]) && continuation(json[i + 3]) ? i + 4 : -1;
            }
            return -1;
        }

        private static boolean continuation(byte b) {
            return (b & 0xC0) == 0x80;
        }
    }
}
