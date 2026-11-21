package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential against 1.0.0's exact computation: ES's {@code Source.source()} (an ordered
 * {@code XContentHelper.convertToMap}) written by {@code new ObjectMapper().writeValueAsString},
 * then {@code getBytes(UTF_8)} in the pipeline. ES runs on this module's test classpath.
 */
class SyntheticSourceJsonTest {

    @BeforeAll
    static void elasticsearchLogging() {
        if (org.elasticsearch.logging.internal.spi.LoggerFactory.provider() == null) {
            org.elasticsearch.logging.internal.spi.LoggerFactory.setInstance(
                new org.elasticsearch.common.logging.internal.LoggerFactoryImpl());
        }
    }

    @Test
    void elasticsearchRenderedValuesExportExactlyAsBefore() throws Exception {
        XContentBuilder b = new XContentBuilder(JsonXContent.jsonXContent, new ByteArrayOutputStream());
        b.startObject();
        b.field("emoji", "\uD83D\uDE00 grinning \uD83D\uDE80");
        b.field("bmp", "caf\u00e9 \u65e5\u672c\u8a9e \u00ff\u0100\uffff");
        b.field("control", "\u0000\u0001\u001f\u007f\t\n\r\b\f");
        b.field("quotes", "\"quoted\" back\\slash /slash </script>");
        b.field("int_max", Integer.MAX_VALUE).field("int_min", Integer.MIN_VALUE);
        b.field("int_max_plus_one", Integer.MAX_VALUE + 1L).field("long_max", Long.MAX_VALUE).field("long_min", Long.MIN_VALUE);
        b.field("unsigned_long_max", new BigInteger("18446744073709551615"));
        b.field("two_pow_63", new BigInteger("9223372036854775808"));
        b.field("d1", 1.0E10).field("d2", 1.0E-7).field("neg_zero", -0.0).field("d3", 0.1).field("d4", 4.9E-324);
        b.field("d5", 1.7976931348623157E308).field("d6", 1.0).field("d7", 100.0).field("d8", 1.0E21).field("d9", 123456.789);
        b.field("f1", 0.1f).field("f2", 3.4028235E38f).field("f3", 1.1f).field("f4", -2.5E-3f);
        b.field("scaled", 12.34).field("scaled_whole", 5.0);
        b.startArray("nested").startObject().field("a", 1).startArray("b").value(true).value(false).nullValue().endArray()
            .endObject().startArray().value("x").startObject().endObject().startArray().endArray().endArray().endArray();
        b.startObject("empty").endObject();
        b.startArray("empty_array").endArray();
        b.nullField("null_value").field("t", true).field("f", false);
        b.endObject();
        assertSameAsLegacy(BytesReference.toBytes(BytesReference.bytes(b)));
    }

    @Test
    void handWrittenEdgeCasesExportExactlyAsBefore() throws Exception {
        for (String json : List.of(
                "{\"lone_high\":\"\\ud800\",\"lone_low\":\"x\\udc00y\",\"pair\":\"\\ud83d\\ude00\"}",
                "{\"raw_pair\":\"\uD83D\uDE00\",\"escaped_ascii\":\"\\u0041\\u00e9\"}",
                "{ \"spaced\" : [ 1 , 2 ] ,\n\"x\" : { } }",
                "{\"n1\":1.50,\"n2\":1e2,\"n3\":-0,\"n4\":1E+2,\"n5\":0.0000001,\"n6\":12345678901234567890123}",
                "{\"big\":123456789012345678901234567890.5,\"tiny\":1e-400,\"huge\":1e400}",
                "{\"key with \\\"quote\\\"\":1,\"\\u00e9\":2,\"\":3}",
                "{}")) {
            assertSameAsLegacy(json.getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void deepNestingAndHugeStringsExportExactlyAsBefore() throws Exception {
        // Depth 999: the object plus 998 arrays, just inside both parsers' and the writer's limit.
        assertSameAsLegacy(("{\"a\":" + "[".repeat(998) + "]".repeat(998) + "}").getBytes(StandardCharsets.UTF_8));
        // A string value beyond Jackson's default limit of 20M characters.
        assertSameAsLegacy(("{\"s\":\"" + "abcdefghij".repeat(2_500_000) + "\"}").getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void whatFailedBeforeStillFails() {
        for (String json : List.of(
                "{\"a\":1,\"a\":2}",
                "{\"x\":{\"b\":1,\"b\":[2]}}",
                "{\"a\":" + "[".repeat(1000) + "]".repeat(1000) + "}",
                "{\"a\":")) {
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            assertThrows(Exception.class, () -> legacy(bytes), json);
            assertThrows(IOException.class, () -> SyntheticSourceJson.toExportJson(bytes), json);
        }
    }

    /**
     * ES's builder writes exactly one object, so trailing content cannot come from it. The map path
     * silently dropped whatever followed the first object; exporting part of a document is worse
     * than failing it.
     */
    @Test
    void trailingContentFailsInsteadOfBeingDropped() throws Exception {
        byte[] bytes = "{\"a\":1} {\"b\":2}".getBytes(StandardCharsets.UTF_8);
        assertEquals("{\"a\":1}", legacy(bytes));
        assertThrows(IOException.class, () -> SyntheticSourceJson.toExportJson(bytes));
    }

    @Test
    void parseErrorsNeverQuoteTheDocument() {
        for (String json : List.of(
                "{\"id\":\"topsecret-value\",\"id\":\"topsecret-value\"}",
                "{\"id\":\"topsecret-value\",",
                "{\"id\":\"topsecret-value\"} trailing",
                "[\"topsecret-value\"]")) {
            var failure = assertThrows(IOException.class,
                () -> SyntheticSourceJson.toExportJson(json.getBytes(StandardCharsets.UTF_8)));
            for (Throwable t = failure; t != null; t = t.getCause()) {
                assertFalse(String.valueOf(t.getMessage()).contains("topsecret"), t.getMessage());
                assertFalse(String.valueOf(t.getMessage()).contains("synthetic source"), t.getMessage());
            }
        }
    }

    @Test
    void emptyBytesAreAnEmptySourceLikeElasticsearchSays() throws Exception {
        assertSameAsLegacy(new byte[0]);
    }

    /**
     * The fast path returns Elasticsearch's bytes only where they equal the char path's output: over
     * every document of every synthetic fixture (as the real ES mappers render them), and each
     * result also equals the 1.0.0 golden text.
     */
    @ParameterizedTest
    @MethodSource("renderedFixtures")
    void fastPathIsUnusedOrIdenticalOnEveryFixtureDocument(String fixtureIndex, @TempDir Path tmp)
            throws Exception {
        List<String> golden = new ArrayList<>();
        new ObjectMapper().readTree(SyntheticSourceReconstructorTest.GOLDEN.toFile()).path("reconstructed")
            .path(fixtureIndex).forEach(doc -> golden.add(doc.asText()));
        int hits = 0;
        try (var reconstructor = SyntheticSourceReconstructorTest.Fixture.materialize(fixtureIndex, tmp).open()) {
            for (int doc = 0; doc < reconstructor.docCount(); doc++) {
                byte[] esJson = reconstructor.elasticsearchSource(doc);
                if (assertFastPathUnusedOrIdentical(esJson)) hits++;
                if (!golden.isEmpty()) {
                    assertArrayEquals(golden.get(doc).getBytes(StandardCharsets.UTF_8),
                        SyntheticSourceJson.toExportJson(esJson), fixtureIndex + " doc " + doc);
                }
            }
            assertTrue(hits > 0, fixtureIndex + ": the fast path never applied");
        }
    }

    static List<String> renderedFixtures() {
        List<String> fixtures = new ArrayList<>(SyntheticSourceReconstructorTest.GOLDEN_INDICES);
        fixtures.remove("synth-deletes-repo/test_deletes_nested");
        fixtures.add("es93-repo/test_es93_logsdb_binary");
        fixtures.add("es93-repo/test_es93_tsdb_hist");
        return fixtures;
    }

    /**
     * Raw Elasticsearch renderings captured from the benchmark's logs-logsdb (ES 9.3) and synth8
     * (ES 8.17) shards, every 200th document. At least 80% must take the fast path; every document
     * that does not contains a surrogate escape, which ES writes for characters outside the BMP and
     * the char path writes as the raw character.
     */
    @ParameterizedTest
    @ValueSource(strings = {"gsbench-logs-logsdb.jsonl.gz", "gsbench-synth8.jsonl.gz"})
    void fastPathIsUnusedOrIdenticalOnElasticsearchRenderedCorpora(String corpus) throws Exception {
        List<byte[]> documents = corpus(corpus);
        int hits = 0;
        for (byte[] esJson : documents) {
            if (assertFastPathUnusedOrIdentical(esJson)) {
                hits++;
            } else {
                String text = new String(esJson, StandardCharsets.UTF_8);
                assertTrue(Pattern.compile("\\\\u[dD][89abAB]").matcher(text).find(),
                    "a rejected document without a surrogate escape: " + text);
            }
        }
        assertTrue(hits >= 0.8 * documents.size(), corpus + ": " + hits + " of " + documents.size());
    }

    /**
     * Generated documents around every rejection rule: characters outside the BMP raw and escaped,
     * lone surrogates, raw CESU-8 and overlong bytes, float, half_float and scaled_float renderings,
     * big integers, -0, control characters, insignificant whitespace, escapes the generator writes
     * differently (slash, A, lower-case hex, non-ASCII), a BOM, duplicate keys and trailing content.
     */
    @Test
    void fastPathIsUnusedOrIdenticalOnAGeneratedCorpus() throws Exception {
        List<byte[]> corpus = new ArrayList<>();
        for (String json : List.of(
                "{\"a\":\"\uD83D\uDE00\"}", "{\"a\":\"\\ud83d\\ude00\"}", "{\"a\":\"\\uD83D\\uDE00\"}",
                "{\"a\":\"x\\ud800\"}", "{\"a\":\"\\uDC00y\"}", "{\"\\uD83D\\uDE00\":1}",
                "{\"a\":1.5,\"b\":1.0E10,\"c\":1.0E-7,\"d\":-0.0,\"e\":0.1,\"f\":4.9E-324}",
                "{\"a\":1.50}", "{\"a\":1e2}", "{\"a\":1E+2}", "{\"a\":1.0e10}", "{\"a\":0.00001}", "{\"a\":1e400}",
                "{\"a\":-1e400}", "{\"a\":1.0E400}", "{\"a\":-0}", "{\"a\":[-0,0,-1]}", "{\"a\":-0.0}", "{\"a\":-0e0}",
                "{\"a\":12345678901234567890123}", "{\"a\":-9223372036854775809}", "{\"a\":9223372036854775807}",
                "{\"a\":\"\\u0000\\u001F\\u0007\"}", "{\"a\":\"\\u001f\"}", "{\"a\":\"\\u0008\"}", "{\"a\":\"\\u000a\"}",
                "{\"a\":\"\\u000A\"}", "{\"a\":\"\\b\\f\\n\\r\\t\\\"\\\\\"}", "{\"a\":\"\\/\"}", "{\"a\":\"\\u0041\"}",
                "{\"a\":\"\\u00e9\"}", "{\"a\":\"\\u00E9\"}", "{\"a\":\"\\u0022\"}", "{\"a\":\"\\u005C\"}", "{\"a\":\"\\u007F\"}",
                "{\"a\":\"\u007f caf\u00e9 \u65e5 \uffff \u2028\"}", "{ \"a\":1}", "{\"a\" :1}", "{\"a\":1 }", "{\"a\":1}\n", "\n{\"a\":1}",
                "{\"a\":\t1}", "{\"a\":1,\"a\":2}", "{\"a\":{\"b\":1,\"b\":1}}", "{\"a\":1}{\"b\":2}", "{\"a\":1} x",
                "[1,2]", "\"a\"", "{\"a\":", "{\"a\":tru}", "{\"a\":true,\"b\":false,\"c\":null,\"d\":[],\"e\":{}}",
                "{}", "{\"\":\"\"}", "{\"a\":\"" + "[".repeat(3) + "\"}", "{\"a\":" + "[".repeat(150) + "]".repeat(150) + "}")) {
            corpus.add(json.getBytes(StandardCharsets.UTF_8));
        }
        corpus.add(new byte[]{'{', '"', 'a', '"', ':', '"', (byte) 0xED, (byte) 0xA0, (byte) 0x80, '"', '}'});
        corpus.add(new byte[]{'{', '"', 'a', '"', ':', '"', (byte) 0xC0, (byte) 0x80, '"', '}'});
        corpus.add(new byte[]{'{', '"', 'a', '"', ':', '"', (byte) 0xE0, (byte) 0x80, (byte) 0x80, '"', '}'});
        corpus.add(new byte[]{'{', '"', 'a', '"', ':', '"', (byte) 0xF4, (byte) 0x90, (byte) 0x80, (byte) 0x80, '"', '}'});
        corpus.add(new byte[]{'{', '"', 'a', '"', ':', '"', (byte) 0xF0, (byte) 0x9F, (byte) 0x98, (byte) 0x80, '"', '}'});
        corpus.add(new byte[]{'{', '"', 'a', '"', ':', '"', (byte) 0x80, '"', '}'});
        corpus.add(new byte[]{'{', '"', 'a', '"', ':', '"', (byte) 0xE2, (byte) 0x82, '"', '}'});
        corpus.add(new byte[]{'{', '"', 'a', '"', ':', '"', 0x01, '"', '}'});
        corpus.add(new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF, '{', '"', 'a', '"', ':', '1', '}'});
        for (byte[] esJson : corpus) {
            assertFastPathUnusedOrIdentical(esJson);
        }

        int hits = 0;
        int canonical = 0;
        for (byte[] esJson : generatedDocuments(new Random(42), 3000)) {
            if (assertFastPathUnusedOrIdentical(esJson)) hits++;
            if (Arrays.equals(esJson, SyntheticSourceJson.toExportText(esJson).getBytes(StandardCharsets.UTF_8))) canonical++;
        }
        assertTrue(hits > 1000, "the generated canonical documents take the fast path: " + hits);
        assertEquals(canonical, hits, "every generated document already in export form takes the fast path");
    }

    /**
     * Random single-byte edits (insert, delete, replace with a JSON-significant or non-ASCII byte) of
     * canonical documents: whatever the fast path still accepts must parse and render to itself.
     */
    @Test
    void fastPathIsUnusedOrIdenticalOnMutatedDocuments() throws Exception {
        var random = new Random(7);
        byte[] alphabet = "{}[]:,\"\\/ \t\n-+.0123456789eEtrufalsnxuDAF".getBytes(StandardCharsets.US_ASCII);
        List<byte[]> seeds = new ArrayList<>(generatedDocuments(new Random(11), 200));
        seeds.add("{\"a\":[1,-2,3.5,1.0E10,true,false,null,{\"b\":\"\\u001F\\n\"}],\"c\":\"caf\u00e9\uD83D\uDE00\"}"
            .getBytes(StandardCharsets.UTF_8));
        int accepted = 0;
        for (int round = 0; round < 60_000; round++) {
            byte[] seed = seeds.get(random.nextInt(seeds.size()));
            byte[] mutated = mutate(seed, random, alphabet);
            if (random.nextInt(4) == 0) mutated = mutate(mutated, random, alphabet);
            if (assertFastPathUnusedOrIdentical(mutated)) accepted++;
        }
        assertTrue(accepted > 1000, "some mutations stay canonical: " + accepted);
    }

    private static byte[] mutate(byte[] document, Random random, byte[] alphabet) {
        int at = random.nextInt(document.length);
        byte replacement = random.nextInt(8) == 0 ? (byte) (0x80 + random.nextInt(0x80)) : alphabet[random.nextInt(alphabet.length)];
        return switch (random.nextInt(3)) {
            case 0 -> {
                byte[] inserted = new byte[document.length + 1];
                System.arraycopy(document, 0, inserted, 0, at);
                inserted[at] = replacement;
                System.arraycopy(document, at, inserted, at + 1, document.length - at);
                yield inserted;
            }
            case 1 -> {
                byte[] deleted = new byte[document.length - 1];
                System.arraycopy(document, 0, deleted, 0, at);
                System.arraycopy(document, at + 1, deleted, at, document.length - at - 1);
                yield deleted;
            }
            default -> {
                byte[] replaced = document.clone();
                replaced[at] = replacement;
                yield replaced;
            }
        };
    }

    /** The rules themselves: canonical text passes through as the same array, each deviation does not. */
    @Test
    void fastPathAcceptsCanonicalTextAndRejectsEachDeviation() {
        String canonical = "{\"a\":\"\\u0000\\u001F\\u0007\",\"b\":\"\\b\\f\\n\\r\\t\\\"\\\\\",\"c\":1.5,\"d\":-1,"
            + "\"e\":12345678901234567890123,\"f\":\"caf\u00e9\u007f/\uD83D\uDE00\",\"g\":[true,false,null,{},-0.0,1.0E10]}";
        byte[] bytes = canonical.getBytes(StandardCharsets.UTF_8);
        assertTrue(assertFastPathUnusedOrIdentical(bytes), canonical);
        for (String deviation : List.of(
                "{\"a\":\"\\uD83D\\uDE00\"}", "{\"a\":\"\\u001f\"}", "{\"a\":\"\\u000A\"}", "{\"a\":\"\\/\"}",
                "{\"a\":\"\\u0041\"}", "{\"a\":\"\\u00E9\"}", "{\"a\":-0}", "{\"a\":1.50}", "{\"a\":1e2}", "{\"a\":1e400}",
                "{\"a\":1 }", "{\"a\":1}\n", "{\"a\":1,\"a\":1}", "{\"a\":1}{}", "[1]", "")) {
            assertEquals(null, SyntheticSourceJson.fastPath(deviation.getBytes(StandardCharsets.UTF_8)), deviation);
        }
        assertEquals(null, SyntheticSourceJson.fastPath(new byte[]{'{', '"', 'a', '"', ':', '"', (byte) 0xED, (byte) 0xA0, (byte) 0x80, '"', '}'}));
        assertEquals(null, SyntheticSourceJson.fastPath(new byte[]{'{', '"', 'a', '"', ':', '"', (byte) 0xC0, (byte) 0x80, '"', '}'}));
    }

    /**
     * A duplicate name never takes the fast path, wherever it sits in an object, however wide the
     * object and however many names of the same length and last byte come before it: around the
     * switch from comparing names to hashing them (16 names) and the hash table's growth steps,
     * with nested wide objects whose names repeat the outer ones.
     */
    @Test
    void fastPathFindsDuplicateNamesInWideObjects() throws Exception {
        for (int width : new int[]{1, 2, 15, 16, 17, 18, 31, 32, 33, 34, 64, 65, 127, 128, 129, 200, 1000, 5000}) {
            StringBuilder members = new StringBuilder();
            for (int i = 0; i < width; i++) {
                members.append(i == 0 ? "" : ",").append(String.format("\"%04dz\":%d", i, i));
            }
            byte[] distinct = ("{" + members + "}").getBytes(StandardCharsets.UTF_8);
            assertTrue(assertFastPathUnusedOrIdentical(distinct), "width " + width);
            byte[] nested = ("{\"a\":{" + members + "},\"b\":{" + members + "}}").getBytes(StandardCharsets.UTF_8);
            assertTrue(assertFastPathUnusedOrIdentical(nested), "width " + width);
            byte[] inside = ("{" + members + ",\"x\":{" + members + "},\"y\":1}").getBytes(StandardCharsets.UTF_8);
            assertTrue(assertFastPathUnusedOrIdentical(inside), "width " + width);
            for (String duplicate : List.of(
                    "{" + members + ",\"0000z\":0}",
                    "{" + members + String.format(",\"%04dz\":0}", width - 1),
                    "{" + members + String.format(",\"%04dz\":0}", Math.min(width - 1, 16)),
                    "{" + members + ",\"x\":{" + members + "}" + String.format(",\"%04dz\":0}", width / 2),
                    "{\"a\":{" + members + String.format(",\"%04dz\":0},\"b\":1}", width / 2))) {
                byte[] bytes = duplicate.getBytes(StandardCharsets.UTF_8);
                assertEquals(null, SyntheticSourceJson.fastPath(bytes), duplicate);
                assertThrows(IOException.class, () -> SyntheticSourceJson.toExportJson(bytes), duplicate);
            }
        }
        for (String json : List.of("{\"\":1,\"\":2}", "{\"\":1,\"a\":{\"\":1},\"\":2}")) {
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            assertEquals(null, SyntheticSourceJson.fastPath(bytes), json);
            assertThrows(IOException.class, () -> SyntheticSourceJson.toExportJson(bytes), json);
        }
        assertTrue(assertFastPathUnusedOrIdentical("{\"\":1,\"a\":{\"\":1}}".getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Names whose hashes are all equal ("Aa" and "BB" hash alike, and so does every concatenation of
     * them) are still told apart by their bytes; once they would make the probing quadratic the
     * document takes the char path, which renders it unchanged.
     */
    @Test
    void fastPathTellsNamesWithEqualHashesApart() throws Exception {
        for (int blocks : new int[]{5, 6}) {
            List<String> names = collidingNames(blocks);
            String members = String.join(",", names.stream().map(name -> "\"" + name + "\":1").toList());
            assertTrue(assertFastPathUnusedOrIdentical(("{" + members + "}").getBytes(StandardCharsets.UTF_8)), "blocks " + blocks);
            for (String name : List.of(names.get(0), names.get(names.size() / 2), names.get(names.size() - 1))) {
                byte[] duplicate = ("{" + members + ",\"" + name + "\":2}").getBytes(StandardCharsets.UTF_8);
                assertEquals(null, SyntheticSourceJson.fastPath(duplicate), name);
                assertThrows(IOException.class, () -> SyntheticSourceJson.toExportJson(duplicate), name);
            }
        }
        String members = String.join(",", collidingNames(8).stream().map(name -> "\"" + name + "\":1").toList());
        byte[] colliding = ("{" + members + "}").getBytes(StandardCharsets.UTF_8);
        assertEquals(null, SyntheticSourceJson.fastPath(colliding));
        assertArrayEquals(colliding, SyntheticSourceJson.toExportJson(colliding));
    }

    private static List<String> collidingNames(int blocks) {
        List<String> names = new ArrayList<>(List.of(""));
        for (int i = 0; i < blocks; i++) {
            List<String> longer = new ArrayList<>();
            for (String name : names) {
                longer.add(name + "Aa");
                longer.add(name + "BB");
            }
            names = longer;
        }
        assertEquals(1, names.stream().map(String::hashCode).distinct().count());
        return names;
    }

    /**
     * A number with a fraction or an exponent takes the fast path exactly when {@link Double#toString}
     * writes its value back as the same text, whether the fast path decides that by parsing or by
     * the digits alone: boundaries of the plain range and of 15 significant digits, trailing zeros,
     * random short and long decimals, and doubles' and floats' own renderings.
     */
    @Test
    void fastPathAcceptsExactlyTheFloatsDoubleToStringWritesUnchanged() throws Exception {
        var random = new Random(99);
        List<String> numbers = new ArrayList<>(List.of("0.0", "-0.0", "0.00", "00.0", "1.0", "-1.0", "10.0", "10.00",
            "100.0", "1.5", "1.50", "0.1", "0.3", "0.30000000000000004", "0.001", "-0.001", "0.0010", "0.000999",
            "0.0009999999999999", "5.0E-4", "1.0E-3", "9999999.0", "9999999.5", "9999999.99999999", "9999999.999999999",
            "99999999.0", "10000000.0", "1.0E7", "0.00123456789012345", "0.001234567890123456", "1234567.12345678",
            "1234567.123456789", "123456.789012345", "123456.7890123456", "4.9E-324", "1.7976931348623157E308"));
        for (int i = 0; i < 200_000; i++) {
            numbers.add(switch (random.nextInt(5)) {
                case 0 -> Double.toString(Double.longBitsToDouble(random.nextLong()));
                case 1 -> Double.toString(random.nextDouble() * Math.pow(10, random.nextInt(14) - 5));
                case 2 -> Double.toString((double) (random.nextFloat() * (float) Math.pow(10, random.nextInt(12) - 4)));
                case 3 -> Double.toString(Math.round(random.nextDouble() * 1e9) / Math.pow(10, random.nextInt(13)));
                default -> randomPlainDecimal(random);
            });
        }
        int accepted = 0;
        for (String number : numbers) {
            if (!number.matches("-?(0|[1-9][0-9]*)(\\.[0-9]+)?([eE][+-]?[0-9]+)?") || !number.matches(".*[.eE].*")) continue;
            byte[] json = ("{\"a\":" + number + "}").getBytes(StandardCharsets.US_ASCII);
            double value = Double.parseDouble(number);
            boolean writtenUnchanged = Double.isFinite(value) && Double.toString(value).equals(number);
            assertEquals(writtenUnchanged, SyntheticSourceJson.fastPath(json) != null, number);
            if (writtenUnchanged) {
                assertFastPathUnusedOrIdentical(json);
                accepted++;
            }
        }
        assertTrue(accepted > 50_000, "accepted " + accepted);
    }

    private static String randomPlainDecimal(Random random) {
        StringBuilder number = new StringBuilder(random.nextBoolean() ? "-" : "");
        if (random.nextInt(5) < 2) {
            number.append('0');
        } else {
            number.append((char) ('1' + random.nextInt(9)));
            for (int i = random.nextInt(9); i > 0; i--) number.append((char) ('0' + random.nextInt(10)));
        }
        number.append('.');
        if (random.nextInt(10) == 0) return number.append('0').toString();
        for (int i = random.nextInt(5); i > 0; i--) number.append('0');
        for (int i = 1 + random.nextInt(17); i > 0; i--) number.append((char) ('0' + random.nextInt(10)));
        if (random.nextInt(8) == 0) number.append("0".repeat(1 + random.nextInt(2)));
        return number.toString();
    }

    /** Documents rendered like ES's builder (floats as Float or Double, scaled values, big numbers, any character). */
    private static List<byte[]> generatedDocuments(Random random, int count) throws Exception {
        List<byte[]> documents = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            XContentBuilder b = new XContentBuilder(JsonXContent.jsonXContent, new ByteArrayOutputStream());
            b.startObject();
            int fields = 1 + random.nextInt(8);
            for (int f = 0; f < fields; f++) {
                String name = "f" + f + (random.nextInt(10) == 0 ? randomText(random, 3) : "");
                switch (random.nextInt(9)) {
                    case 0 -> b.field(name, random.nextFloat() * (float) Math.pow(10, random.nextInt(20) - 10));
                    case 1 -> b.field(name, random.nextDouble() * Math.pow(10, random.nextInt(40) - 20));
                    case 2 -> b.field(name, Math.round(random.nextDouble() * 1e4) / 100.0);
                    case 3 -> b.field(name, (double) Float.float16ToFloat(Float.floatToFloat16(random.nextFloat() * 1000)));
                    case 4 -> b.field(name, random.nextLong());
                    case 5 -> b.field(name, new BigInteger(64, random));
                    case 6 -> b.field(name, randomText(random, random.nextInt(12)));
                    case 7 -> b.startArray(name).value(random.nextInt()).value(random.nextBoolean()).nullValue().endArray();
                    default -> b.startObject(name).field("x", randomText(random, 4)).field("y", -random.nextInt(3)).endObject();
                }
            }
            b.endObject();
            documents.add(BytesReference.toBytes(BytesReference.bytes(b)));
        }
        return documents;
    }

    private static String randomText(Random random, int length) {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < length; i++) {
            switch (random.nextInt(6)) {
                case 0 -> text.append((char) random.nextInt(0x20));
                case 1 -> text.append("\"\\/");
                case 2 -> text.appendCodePoint(0x10000 + random.nextInt(0x10FFFF - 0x10000));
                case 3 -> text.append((char) (0x80 + random.nextInt(0xD800 - 0x80)));
                case 4 -> text.append(random.nextInt(50) == 0 ? '\uD800' : '\u2028');
                default -> text.append((char) ('a' + random.nextInt(26)));
            }
        }
        return text.toString();
    }

    /** True when the fast path applied, after checking its bytes equal the char path's. */
    private static boolean assertFastPathUnusedOrIdentical(byte[] esJson) {
        byte[] fast = SyntheticSourceJson.fastPath(esJson);
        if (fast == null) return false;
        assertSame(esJson, fast);
        String charPath;
        try {
            charPath = SyntheticSourceJson.toExportText(esJson);
        } catch (IOException e) {
            throw new AssertionError("the fast path accepted a document the char path rejects: "
                + new String(esJson, StandardCharsets.UTF_8), e);
        }
        assertArrayEquals(charPath.getBytes(StandardCharsets.UTF_8), fast, new String(esJson, StandardCharsets.UTF_8));
        return true;
    }

    private static List<byte[]> corpus(String name) throws IOException {
        byte[] all;
        try (var in = new GZIPInputStream(Files.newInputStream(
                Path.of("src/test/resources/fixtures/es-rendered-sources", name)))) {
            all = in.readAllBytes();
        }
        List<byte[]> documents = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < all.length; i++) {
            if (all[i] == '\n') {
                documents.add(Arrays.copyOfRange(all, start, i));
                start = i + 1;
            }
        }
        return documents;
    }

    private static void assertSameAsLegacy(byte[] esJson) throws Exception {
        String legacy = legacy(esJson);
        assertEquals(legacy, SyntheticSourceJson.toExportText(esJson));
        assertArrayEquals(legacy.getBytes(StandardCharsets.UTF_8), SyntheticSourceJson.toExportJson(esJson));
    }

    /** 1.0.0: {@code Source.fromBytes(bytes, JSON).source()} then {@code ObjectMapper.writeValueAsString}. */
    private static String legacy(byte[] esJson) throws Exception {
        Map<String, Object> map = esJson.length == 0 ? Map.of()
            : XContentHelper.convertToMap(new BytesArray(esJson), true).v2();
        return new ObjectMapper().writeValueAsString(map);
    }
}
