package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The byte validator may only accept what the legacy per-document path accepts unaltered (decode
 * as UTF-8, {@link ArchiveContext#validateSource}, encode back to the same bytes), must never
 * throw, and must accept nearly every real document, or the per-document path becomes the hot path.
 */
class SourceValidatorTest {

    private final SourceValidator validator = new SourceValidator();

    /** Today's decision: accepted unaltered, or why not. */
    static String legacy(byte[] bytes, int length) {
        String text = new String(bytes, 0, length, StandardCharsets.UTF_8);
        try {
            ArchiveContext.validateSource(text);
        } catch (Exception e) {
            return "REJECT(" + e.getClass().getSimpleName() + ")";
        }
        return Arrays.equals(text.getBytes(StandardCharsets.UTF_8), Arrays.copyOf(bytes, length)) ? "ACCEPT" : "ALTERED";
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[]... parts) {
        var out = new java.io.ByteArrayOutputStream();
        for (byte[] part : parts) out.writeBytes(part);
        return out.toByteArray();
    }

    private static byte[] inString(int... bytes) {
        byte[] middle = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) middle[i] = (byte) bytes[i];
        return concat(utf8("{\"a\":\""), middle, utf8("\"}"));
    }

    /** Probe cases, with today's decision: the validator accepts exactly the ACCEPT ones. */
    static Map<String, byte[]> probeCases() {
        Map<String, byte[]> cases = new LinkedHashMap<>();
        cases.put("plain", utf8("{\"a\":1}"));
        cases.put("bom", concat(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, utf8("{\"a\":1}")));
        cases.put("name-50000", utf8("{\"" + "x".repeat(50_000) + "\":1}"));
        cases.put("name-50001", utf8("{\"" + "x".repeat(50_001) + "\":1}"));
        cases.put("name-60000-nested", utf8("{\"a\":{\"" + "x".repeat(60_000) + "\":1}}"));
        cases.put("depth-1000", utf8("{\"a\":" + "[".repeat(999) + "]".repeat(999) + "}"));
        cases.put("depth-1001", utf8("{\"a\":" + "[".repeat(1000) + "]".repeat(1000) + "}"));
        cases.put("value-str-2M", utf8("{\"a\":\"" + "y".repeat(2_000_000) + "\"}"));
        cases.put("num-40digits", utf8("{\"a\":1234567890123456789012345678901234567890.5e-300}"));
        cases.put("leading-zero", utf8("{\"a\":01}"));
        cases.put("neg-zero", utf8("{\"a\":-0}"));
        cases.put("lone-surrogate-escape", utf8("{\"a\":\"\\ud800\"}"));
        cases.put("raw-DEL", utf8("{\"a\":\"\u007f\"}"));
        cases.put("raw-tab-in-string", utf8("{\"a\":\"\t\"}"));
        cases.put("NaN", utf8("{\"a\":NaN}"));
        cases.put("comment", utf8("{/*c*/\"a\":1}"));
        cases.put("single-quote", utf8("{'a':1}"));
        cases.put("trailing-comma", utf8("{\"a\":1,}"));
        cases.put("dup-key", utf8("{\"a\":1,\"a\":2}"));
        cases.put("trailing-ws", utf8("{\"a\":1} \n\t\r"));
        cases.put("trailing-nul", concat(utf8("{\"a\":1}"), new byte[]{0}));
        cases.put("trailing-obj", utf8("{\"a\":1}{}"));
        cases.put("leading-ws", utf8("  \n{\"a\":1}"));
        cases.put("empty", new byte[0]);
        cases.put("blank", utf8("   "));
        cases.put("array-root", utf8("[1]"));
        cases.put("overlong-C0-80", inString(0xC0, 0x80));
        cases.put("overlong-in-name", concat(utf8("{\""), new byte[]{(byte) 0xC0, (byte) 0xAF}, utf8("\":1}")));
        cases.put("cesu-surrogate", inString(0xED, 0xA0, 0x80));
        cases.put("bad-cont", inString(0xC3, 0x28));
        cases.put("F4-90", inString(0xF4, 0x90, 0x80, 0x80));
        cases.put("lone-FF", inString(0xFF));
        cases.put("emoji", utf8("{\"a\":\"\uD83D\uDE00\"}"));
        cases.put("U+2028", utf8("{\"a\":\"\u2028\"}"));
        cases.put("nonchar-FFFF", utf8("{\"a\":\"\uFFFF\"}"));
        cases.put("escape-slash", utf8("{\"a\":\"\\/\"}"));
        cases.put("bad-escape", utf8("{\"a\":\"\\x\"}"));
        cases.put("unquoted-key", utf8("{a:1}"));
        cases.put("true-x", utf8("{\"a\":truex}"));
        cases.put("plus-num", utf8("{\"a\":+1}"));
        cases.put("dot-num", utf8("{\"a\":.5}"));
        cases.put("exp-noDigits", utf8("{\"a\":1e}"));
        cases.put("NUL-escape", utf8("{\"a\":\"\\u0000\"}"));
        cases.put("ctrl-ws-before", concat(new byte[]{0x0B}, utf8("{\"a\":1}")));
        cases.put("nbsp-ws", concat(utf8("{\"a\":1"), new byte[]{(byte) 0xC2, (byte) 0xA0}, utf8("}")));
        cases.put("cjk", utf8("{\"名前\":\"日本語のテキスト\",\"中\":[\"文字\"]}"));
        cases.put("max-code-point", inString(0xF4, 0x8F, 0xBF, 0xBF));
        cases.put("truncated-3-byte", inString(0xE2, 0x82));
        cases.put("truncated-at-end", concat(utf8("{\"a\":\""), new byte[]{(byte) 0xE2}));
        cases.put("literals", utf8("{\"t\":true,\"f\":false,\"n\":null,\"x\":[true,false,null]}"));
        cases.put("uppercase-hex-escape", utf8("{\"a\":\"\\u00E9\\uD83D\\uDE00\"}"));
        cases.put("short-u-escape", utf8("{\"a\":\"\\u12\"}"));
        cases.put("exp-forms", utf8("{\"a\":[1E+2,1e-3,-0.0e0,0.5E7]}"));
        cases.put("missing-colon", utf8("{\"a\" 1}"));
        cases.put("missing-value", utf8("{\"a\":}"));
        cases.put("unclosed", utf8("{\"a\":[1,2}"));
        cases.put("mismatched", utf8("{\"a\":[1,2}]"));
        return cases;
    }

    @Test
    void probeCasesAcceptExactlyWhatTodayAcceptsUnaltered() {
        var mismatches = new ArrayList<String>();
        for (var entry : probeCases().entrySet()) {
            byte[] bytes = entry.getValue();
            String today = legacy(bytes, bytes.length);
            boolean accepted = validator.accepts(bytes, bytes.length);
            if (accepted != today.equals("ACCEPT")) mismatches.add(entry.getKey() + ": today " + today + ", validator " + accepted);
        }
        assertEquals(List.of(), mismatches);
    }

    @Test
    void probeExpectationsHold() {
        var cases = probeCases();
        for (String rejected : List.of("bom", "name-50001", "name-60000-nested", "depth-1001", "overlong-C0-80",
                "cesu-surrogate", "F4-90", "bad-cont", "lone-FF", "leading-zero", "NaN", "comment", "single-quote",
                "trailing-comma", "raw-tab-in-string", "empty", "blank", "array-root", "trailing-obj", "trailing-nul"))
            assertFalse(validator.accepts(cases.get(rejected), cases.get(rejected).length), rejected);
        for (String accepted : List.of("plain", "name-50000", "depth-1000", "value-str-2M", "num-40digits", "dup-key",
                "lone-surrogate-escape", "emoji", "trailing-ws", "leading-ws", "raw-DEL", "neg-zero"))
            assertTrue(validator.accepts(cases.get(accepted), cases.get(accepted).length), accepted);
    }

    @Test
    void lengthBoundsTheInput() {
        byte[] bytes = utf8("{\"a\":1}garbage");
        assertTrue(validator.accepts(bytes, 7));
        assertFalse(validator.accepts(bytes, bytes.length));
        assertFalse(validator.accepts(bytes, 6));
        assertFalse(validator.accepts(bytes, bytes.length + 1));
        assertFalse(validator.accepts(bytes, -1));
        assertFalse(validator.accepts(null, 0));
    }

    @Test
    void deepNestingIsRejectedWithoutRecursion() throws Exception {
        byte[] deep = utf8("{\"a\":" + "[".repeat(1_000_000) + "]".repeat(1_000_000) + "}");
        byte[] deepObjects = utf8("{\"a\":" + "{\"a\":".repeat(500_000) + "1" + "}".repeat(500_001));
        var results = new boolean[3];
        var failure = new Throwable[1];
        // A small stack: any recursion per nesting level would overflow it.
        Thread thread = new Thread(null, () -> {
            try {
                var local = new SourceValidator();
                results[0] = local.accepts(deep, deep.length);
                results[1] = local.accepts(deepObjects, deepObjects.length);
                byte[] ok = utf8("{\"a\":" + "[".repeat(999) + "]".repeat(999) + "}");
                results[2] = local.accepts(ok, ok.length);
            } catch (Throwable t) {
                failure[0] = t;
            }
        }, "small-stack", 64 * 1024);
        thread.start();
        thread.join();
        assertNull(failure[0]);
        assertFalse(results[0]);
        assertFalse(results[1]);
        assertTrue(results[2]);
    }

    /**
     * Over a million inputs: well-formed documents from a JSON grammar, the same with random byte
     * mutations, and random byte strings. Accepting implies today's path accepts unaltered; for
     * unmutated grammar documents the two decisions are equal, so the validator is not stricter
     * than needed either.
     */
    @Test
    void differentialFuzzAgainstTheLegacyPath() {
        var random = new SplittableRandom(20260928);
        var grammar = new JsonGrammar(random);
        long inputs = 0, accepted = 0, grammarDocs = 0;
        var violations = new ArrayList<String>();
        while (inputs < 1_050_000) {
            byte[] bytes;
            boolean unmutated = false;
            int kind = random.nextInt(10);
            if (kind < 4) {
                bytes = utf8(grammar.document());
                unmutated = true;
                grammarDocs++;
            } else if (kind < 9) {
                bytes = mutate(utf8(grammar.document()), random);
            } else {
                bytes = new byte[random.nextInt(24)];
                random.nextBytes(bytes);
                if (bytes.length > 0 && random.nextBoolean()) bytes[0] = '{';
            }
            inputs++;
            boolean accepts;
            try {
                accepts = validator.accepts(bytes, bytes.length);
            } catch (Throwable t) {
                fail("validator threw " + t + " for " + java.util.HexFormat.of().formatHex(bytes));
                return;
            }
            String today = legacy(bytes, bytes.length);
            if (accepts) accepted++;
            if (accepts && !today.equals("ACCEPT") || unmutated && !accepts && today.equals("ACCEPT")) {
                if (violations.size() < 20) violations.add(today + " vs " + accepts + ": " + new String(bytes, StandardCharsets.UTF_8));
            }
        }
        assertEquals(List.of(), violations);
        assertTrue(grammarDocs > 400_000, "grammar documents " + grammarDocs);
        assertTrue(accepted > grammarDocs, "mutations keep some documents valid: " + accepted);
    }

    private static byte[] mutate(byte[] bytes, SplittableRandom random) {
        byte[] out = bytes;
        for (int m = 1 + random.nextInt(3); m > 0; m--) {
            int at = out.length == 0 ? 0 : random.nextInt(out.length);
            switch (random.nextInt(6)) {
                case 0 -> { if (out.length > 0) out[at] = (byte) random.nextInt(256); }
                case 1 -> { if (out.length > 0) out = concat(Arrays.copyOf(out, at), Arrays.copyOfRange(out, at + 1, out.length)); }
                case 2 -> out = concat(Arrays.copyOf(out, at), new byte[]{(byte) random.nextInt(256)}, Arrays.copyOfRange(out, at, out.length));
                case 3 -> {
                    byte[][] specials = {{(byte) 0xC0, (byte) 0x80}, {(byte) 0xED, (byte) 0xA0, (byte) 0x80},
                        {(byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80}, {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF},
                        {(byte) 0xE2, (byte) 0x82}, {'\\', 'u', 'd', '8'}, {'\\'}, {'"'}, {0x01}, {(byte) 0xFF},
                        {',', '}'}, {']'}, {'-', '0', '1'}, {'1', 'e'}, {(byte) 0xF0, (byte) 0x8F, (byte) 0xBF, (byte) 0xBF}};
                    out = concat(Arrays.copyOf(out, at), specials[random.nextInt(specials.length)], Arrays.copyOfRange(out, at, out.length));
                }
                case 4 -> { if (out.length > 1) out = Arrays.copyOf(out, at); }
                default -> { if (out.length > 0) out[at] = (byte) (out[at] ^ (1 << random.nextInt(8))); }
            }
        }
        return out;
    }

    @Test
    void acceptsNearlyEveryBenchDocument() {
        var generator = new EcsDocuments(42);
        int total = 100_000, accepted = 0;
        for (int i = 0; i < total; i++) {
            byte[] bytes = utf8(generator.next());
            if (validator.accepts(bytes, bytes.length)) accepted++;
        }
        assertTrue(accepted >= total * 0.999, accepted + " of " + total);
    }

    @Test
    void acceptsNearlyEveryGsbenchStyleDocument() throws IOException {
        var mapper = de.sloow.gefrierschrank.core.DocumentJson.newMapper();
        int total = 0, accepted = 0;
        var styles = new java.util.HashSet<String>();
        try (var in = new BufferedReader(new InputStreamReader(new GZIPInputStream(
                getClass().getResourceAsStream("/source-validator/gsbench-sources.ndjson.gz")), StandardCharsets.UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
                byte[] bytes = utf8(mapper.readValue(line, String.class));
                total++;
                if (validator.accepts(bytes, bytes.length)) accepted++;
                else fail("rejected: " + line);
                if (bytes[0] == ' ') styles.add("spaced");
                if (line.contains("\\n  ")) styles.add("pretty");
                if (line.contains("\\\\u")) styles.add("ascii");
            }
        }
        assertTrue(total > 2000);
        assertTrue(accepted >= total * 0.999, accepted + " of " + total);
        assertEquals(java.util.Set.of("spaced", "pretty", "ascii"), styles, "the corpus covers every source style");
    }

    /** Random well-formed JSON objects: all value kinds, escapes, non-ASCII text, odd whitespace. */
    static final class JsonGrammar {
        private static final String[] WORDS = {"a", "id", "日本語", "Grüße", "🚀", "𝔘𝔫", "Ω≈ç√", "tab\t", "q\"", "b\\s",
            "", "x y", "\u2028", "\uFEFF", "\u007F", "\u0001", "</script>"};
        private final SplittableRandom random;

        JsonGrammar(SplittableRandom random) {
            this.random = random;
        }

        String document() {
            var out = new StringBuilder();
            whitespace(out);
            object(out, 0);
            whitespace(out);
            return out.toString();
        }

        private void whitespace(StringBuilder out) {
            if (random.nextInt(4) != 0) return;
            String[] ws = {" ", "\n", "\r", "\t", "  \n  "};
            out.append(ws[random.nextInt(ws.length)]);
        }

        private void object(StringBuilder out, int depth) {
            out.append('{');
            int fields = random.nextInt(depth == 0 ? 6 : 4);
            for (int f = 0; f < fields; f++) {
                if (f > 0) out.append(',');
                whitespace(out);
                string(out);
                whitespace(out);
                out.append(':');
                value(out, depth + 1);
            }
            whitespace(out);
            out.append('}');
        }

        private void value(StringBuilder out, int depth) {
            whitespace(out);
            int kind = random.nextInt(depth > 4 ? 6 : 8);
            switch (kind) {
                case 0, 1 -> string(out);
                case 2 -> number(out);
                case 3 -> out.append("true");
                case 4 -> out.append("false");
                case 5 -> out.append("null");
                case 6 -> object(out, depth);
                default -> {
                    out.append('[');
                    int items = random.nextInt(4);
                    for (int i = 0; i < items; i++) {
                        if (i > 0) out.append(',');
                        value(out, depth + 1);
                    }
                    whitespace(out);
                    out.append(']');
                }
            }
            whitespace(out);
        }

        private void string(StringBuilder out) {
            out.append('"');
            for (int parts = random.nextInt(4); parts > 0; parts--) {
                switch (random.nextInt(6)) {
                    case 0 -> {
                        String[] escapes = {"\\\"", "\\\\", "\\/", "\\b", "\\f", "\\n", "\\r", "\\t", "\\u00e9",
                            "\\uD83D\\uDE00", "\\ud800", "\\u0000", "\\uFFFF"};
                        out.append(escapes[random.nextInt(escapes.length)]);
                    }
                    case 1 -> out.appendCodePoint(0x80 + random.nextInt(0x800 - 0x80));
                    case 2 -> {
                        int cp = random.nextInt(0x10FFFF);
                        if (cp >= 0xD800 && cp <= 0xDFFF || cp < 0x20 || cp == '"' || cp == '\\') cp = 'z';
                        out.appendCodePoint(cp);
                    }
                    default -> {
                        String word = WORDS[random.nextInt(WORDS.length)];
                        for (char c : word.toCharArray()) {
                            if (c == '"' || c == '\\') out.append('\\').append(c);
                            else if (c < 0x20) out.append(String.format("\\u%04x", (int) c));
                            else out.append(c);
                        }
                    }
                }
            }
            out.append('"');
        }

        private void number(StringBuilder out) {
            if (random.nextBoolean()) out.append('-');
            if (random.nextInt(5) == 0) out.append('0');
            else {
                out.append((char) ('1' + random.nextInt(9)));
                for (int d = random.nextInt(random.nextInt(8) == 0 ? 40 : 6); d > 0; d--) out.append((char) ('0' + random.nextInt(10)));
            }
            if (random.nextInt(3) == 0) {
                out.append('.');
                for (int d = 1 + random.nextInt(5); d > 0; d--) out.append((char) ('0' + random.nextInt(10)));
            }
            if (random.nextInt(5) == 0) {
                out.append(random.nextBoolean() ? 'e' : 'E');
                if (random.nextBoolean()) out.append(random.nextBoolean() ? '+' : '-');
                for (int d = 1 + random.nextInt(3); d > 0; d--) out.append((char) ('0' + random.nextInt(10)));
            }
        }
    }

    /** The ECS-style documents of bench/micro's Bench generator (copied, not shared). */
    static final class EcsDocuments {
        private final SplittableRandom random;
        private final String[] words = new String[2000];
        private long timestamp = 1_767_225_600_000L;
        private static final String[] SERVICES = {"checkout", "cart", "catalog", "payment", "shipping", "auth",
            "search", "recommendation", "inventory", "pricing", "notification", "gateway"};
        private static final String[] LEVELS = {"INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "INFO", "DEBUG",
            "DEBUG", "WARN", "WARN", "ERROR"};
        private static final String[] METHODS = {"GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"};
        private static final int[] STATUSES = {200, 200, 200, 200, 200, 200, 201, 204, 301, 400, 401, 403, 404, 500, 503};
        private static final String[] AGENTS = {
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "okhttp/4.12.0", "curl/8.9.1", "python-requests/2.32.3", "Go-http-client/2.0"};
        private static final String[] COUNTRIES = {"DE", "DE", "DE", "AT", "CH", "FR", "NL", "US", "GB", "PL"};
        private static final String[] CITIES = {"Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt", "Vienna",
            "Zurich", "Paris", "Amsterdam", "London"};

        EcsDocuments(long seed) {
            random = new SplittableRandom(seed);
            String[] syllables = {"ka", "lo", "mi", "ne", "ru", "sa", "te", "vo", "zi", "pa", "ro", "de", "an",
                "el", "is", "or", "un", "ba", "ce", "fu"};
            for (int i = 0; i < words.length; i++) {
                StringBuilder w = new StringBuilder();
                int n = 2 + random.nextInt(3);
                for (int s = 0; s < n; s++) w.append(syllables[random.nextInt(syllables.length)]);
                words[i] = w.toString();
            }
        }

        private String word() { return words[Math.min(words.length - 1, (int) Math.abs(random.nextGaussian() * 300))]; }

        private String hex(int length) {
            StringBuilder sb = new StringBuilder(length);
            for (int i = 0; i < length; i++) sb.append(Character.forDigit(random.nextInt(16), 16));
            return sb.toString();
        }

        String next() {
            timestamp += random.nextInt(40);
            String service = SERVICES[random.nextInt(SERVICES.length)];
            int status = STATUSES[random.nextInt(STATUSES.length)];
            int host = random.nextInt(120);
            int country = random.nextInt(COUNTRIES.length);
            StringBuilder message = new StringBuilder();
            switch (random.nextInt(4)) {
                case 0 -> message.append("Processed order ").append(random.nextInt(10_000_000)).append(" in ")
                    .append(random.nextInt(2000)).append(" ms (user=u-").append(random.nextInt(1_000_000)).append(")");
                case 1 -> message.append("Cache miss for key ").append(word()).append(':').append(random.nextInt(100_000));
                case 2 -> message.append("Request to /api/v2/").append(word()).append(" failed with status ").append(status);
                default -> { }
            }
            for (int i = 6 + random.nextInt(18); i > 0; i--) message.append(message.isEmpty() ? "" : " ").append(word());
            return "{\"@timestamp\":\"" + Instant.ofEpochMilli(timestamp) + "\","
                + "\"host\":{\"name\":\"" + service + "-" + host + "\",\"ip\":\"10.1." + (host / 50) + "." + host + "\"},"
                + "\"service\":{\"name\":\"" + service + "\",\"environment\":\"production\"},"
                + "\"log\":{\"level\":\"" + LEVELS[random.nextInt(LEVELS.length)] + "\"},"
                + "\"http\":{\"request\":{\"method\":\"" + METHODS[random.nextInt(METHODS.length)] + "\",\"bytes\":"
                + random.nextInt(20_000) + "},\"response\":{\"status_code\":" + status + "}},"
                + "\"user_agent\":{\"original\":\"" + AGENTS[random.nextInt(AGENTS.length)] + "\"},"
                + "\"client\":{\"geo\":{\"country_iso_code\":\"" + COUNTRIES[country] + "\",\"city_name\":\""
                + CITIES[country] + "\"}},"
                + "\"trace\":{\"id\":\"" + hex(32) + "\"},"
                + "\"event\":{\"duration\":" + random.nextInt(2_000_000_000) + ",\"outcome\":\""
                + (status < 400 ? "success" : "failure") + "\"},"
                + "\"message\":\"" + message + "\"}";
        }
    }
}
