package de.sloow.gefrierschrank.core.source;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import de.sloow.gefrierschrank.core.DocumentJson;
import de.sloow.gefrierschrank.core.FailureDiagnostics;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SourceJsonConverterTest {

    private static final String MARKER = "SECRET-DOCUMENT-TEXT";

    interface Body {
        void write(JsonGenerator generator) throws IOException;
    }

    private static byte[] encode(JsonFactory factory, Body body) throws IOException {
        var out = new ByteArrayOutputStream();
        try (JsonGenerator generator = factory.createGenerator(out)) {
            body.write(generator);
        }
        return out.toByteArray();
    }

    private static byte[] smile(Body body) throws IOException {
        return encode(SmileFactory.builder().build(), body);
    }

    private static byte[] cbor(Body body) throws IOException {
        return encode(CBORFactory.builder().build(), body);
    }

    private static byte[] bytes(int... values) {
        byte[] array = new byte[values.length];
        for (int i = 0; i < values.length; i++) array[i] = (byte) values[i];
        return array;
    }

    private static String json(byte[] bytes, SourceFormat format) throws SourceConversionException {
        assertEquals(format, SourceFormat.detect(bytes, bytes.length), "test input detected as " + format);
        var json = SourceJsonConverter.toJson(bytes, bytes.length, format);
        assertTrue(json.length() <= json.bytes().length);
        return new String(json.bytes(), 0, json.length(), StandardCharsets.UTF_8);
    }

    private static String yaml(String text) throws SourceConversionException {
        return json(text.getBytes(StandardCharsets.UTF_8), SourceFormat.YAML);
    }

    private static SourceConversionException fails(byte[] bytes, SourceFormat format) {
        var failure = assertThrows(SourceConversionException.class, () -> SourceJsonConverter.toJson(bytes, bytes.length, format));
        assertTrue(failure.getMessage().startsWith("stored _source in " + format + " format could not be converted to JSON ("),
            failure.getMessage());
        assertFalse(failure.getMessage().contains(MARKER), failure.getMessage());
        assertFalse(failure.getMessage().contains("synthetic source"), failure.getMessage());
        assertNull(failure.getCause());
        assertInstanceOf(FailureDiagnostics.SafeMessage.class, failure);
        return failure;
    }

    /** Jackson 2.17's UTF-8 generator writes a character outside the BMP as two escaped surrogates. */
    @Test
    void smileScalarsAsElasticsearchWritesThem() throws IOException {
        byte[] doc = smile(g -> {
            g.writeStartObject();
            g.writeNumberField("int", 42);
            g.writeNumberField("negative", -7);
            g.writeNumberField("long", 1L << 40);
            g.writeFieldName("big");
            g.writeNumber(new BigInteger("123456789012345678901234567890"));
            g.writeNumberField("float", 1.1f);
            g.writeNumberField("double", 0.1);
            g.writeFieldName("decimal");
            g.writeNumber(new BigDecimal("1E+3"));
            g.writeBooleanField("t", true);
            g.writeBooleanField("f", false);
            g.writeNullField("n");
            g.writeStringField("s", "x\"\\/\n\t\u0001é😀");
            g.writeEndObject();
        });
        assertEquals("{\"int\":42,\"negative\":-7,\"long\":1099511627776,\"big\":123456789012345678901234567890,"
            + "\"float\":1.1,\"double\":0.1,\"decimal\":1E+3,\"t\":true,\"f\":false,\"n\":null,"
            + "\"s\":\"x\\\"\\\\/\\n\\t\\u0001é\\uD83D\\uDE00\"}", json(doc, SourceFormat.SMILE));
    }

    @Test
    void binaryBecomesBase64() throws IOException {
        byte[] payload = {0, 1, 2, (byte) 0xFF, (byte) 0xFE, 10, 13};
        byte[] doc = smile(g -> {
            g.writeStartObject();
            g.writeBinaryField("bin", payload);
            g.writeEndObject();
        });
        assertEquals("{\"bin\":\"" + Base64.getEncoder().encodeToString(payload) + "\"}", json(doc, SourceFormat.SMILE));
        byte[] cborDoc = cbor(g -> {
            g.writeStartObject();
            g.writeBinaryField("bin", payload);
            g.writeEndObject();
        });
        assertEquals("{\"bin\":\"" + Base64.getEncoder().encodeToString(payload) + "\"}", json(cborDoc, SourceFormat.CBOR));
    }

    @Test
    void sharedNamesAndValuesExpand() throws IOException {
        var factory = SmileFactory.builder().enable(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES).build();
        byte[] doc = encode(factory, g -> {
            g.writeStartObject();
            g.writeArrayFieldStart("items");
            for (int i = 0; i < 50; i++) {
                g.writeStartObject();
                g.writeStringField("name", "shared-value");
                g.writeStringField("other", "v" + (i % 3));
                g.writeEndObject();
            }
            g.writeEndArray();
            g.writeEndObject();
        });
        var expected = new StringBuilder("{\"items\":[");
        for (int i = 0; i < 50; i++) {
            if (i > 0) expected.append(',');
            expected.append("{\"name\":\"shared-value\",\"other\":\"v").append(i % 3).append("\"}");
        }
        expected.append("]}");
        String converted = json(doc, SourceFormat.SMILE);
        assertEquals(expected.toString(), converted);
        assertTrue(converted.length() > doc.length * 3, "back-references expand");
    }

    @Test
    void nonFiniteNumbersBecomeQuotedStrings() throws IOException {
        byte[] doc = smile(g -> {
            g.writeStartObject();
            g.writeNumberField("fnan", Float.NaN);
            g.writeNumberField("dnan", Double.NaN);
            g.writeNumberField("pinf", Double.POSITIVE_INFINITY);
            g.writeNumberField("ninf", Float.NEGATIVE_INFINITY);
            g.writeEndObject();
        });
        String json = json(doc, SourceFormat.SMILE);
        assertEquals("{\"fnan\":\"NaN\",\"dnan\":\"NaN\",\"pinf\":\"Infinity\",\"ninf\":\"-Infinity\"}", json);
        assertTrue(DocumentJson.newMapper().readTree(json).isObject());
    }

    @Test
    void emptyContainersAndKeyOrder() throws IOException {
        byte[] doc = smile(g -> {
            g.writeStartObject();
            g.writeObjectFieldStart("z");
            g.writeEndObject();
            g.writeArrayFieldStart("a");
            g.writeEndArray();
            g.writeArrayFieldStart("m");
            g.writeStartArray();
            g.writeNumber(1);
            g.writeEndArray();
            g.writeEndArray();
            g.writeEndObject();
        });
        assertEquals("{\"z\":{},\"a\":[],\"m\":[[1]]}", json(doc, SourceFormat.SMILE));
        assertEquals("{}", json(smile(g -> {
            g.writeStartObject();
            g.writeEndObject();
        }), SourceFormat.SMILE));
    }

    @Test
    void duplicateKeysAreKept() throws IOException {
        byte[] doc = smile(g -> {
            g.writeStartObject();
            g.writeNumberField("a", 1);
            g.writeNumberField("a", 2);
            g.writeEndObject();
        });
        assertEquals("{\"a\":1,\"a\":2}", json(doc, SourceFormat.SMILE));
    }

    @Test
    void nestingIsLimitedLikeTheJsonPath() throws IOException {
        assertTrue(json(nested(1000), SourceFormat.SMILE).startsWith("{\"a\":{\"a\":"));
        assertEquals("stored _source in SMILE format could not be converted to JSON (StreamConstraintsException)",
            fails(nested(1001), SourceFormat.SMILE).getMessage());
    }

    private static byte[] nested(int depth) throws IOException {
        // The generator has the same limit as the parser, so the deepest document is written without checks.
        var factory = SmileFactory.builder()
            .streamWriteConstraints(com.fasterxml.jackson.core.StreamWriteConstraints.builder().maxNestingDepth(5000).build())
            .build();
        return encode(factory, g -> {
            g.writeStartObject();
            for (int i = 1; i < depth; i++) g.writeObjectFieldStart("a");
            for (int i = 0; i < depth; i++) g.writeEndObject();
        });
    }

    @Test
    void longNamesFailAndLongStringsDoNot() throws IOException {
        String longName = "n".repeat(50_001);
        byte[] doc = smile(g -> {
            g.writeStartObject();
            g.writeNumberField(longName, 1);
            g.writeEndObject();
        });
        assertEquals("stored _source in SMILE format could not be converted to JSON (name too long)",
            fails(doc, SourceFormat.SMILE).getMessage());
        String longestName = "n".repeat(50_000);
        assertTrue(json(smile(g -> {
            g.writeStartObject();
            g.writeNumberField(longestName, 1);
            g.writeEndObject();
        }), SourceFormat.SMILE).startsWith("{\"nnn"));
        char[] chars = new char[21_000_000];
        Arrays.fill(chars, 'x');
        String longValue = new String(chars);
        byte[] large = smile(g -> {
            g.writeStartObject();
            g.writeStringField("v", longValue);
            g.writeEndObject();
        });
        var converted = SourceJsonConverter.toJson(large, large.length, SourceFormat.SMILE);
        assertEquals(longValue.length() + 8, converted.length());
    }

    @Test
    void cborMapEncodings() throws IOException {
        byte[] indefinite = cbor(g -> {
            g.writeStartObject();
            g.writeStringField("k", "v");
            g.writeEndObject();
        });
        assertEquals((byte) 0xBF, indefinite[0]);
        assertEquals("{\"k\":\"v\"}", json(indefinite, SourceFormat.CBOR));

        byte[] definite = cbor(g -> {
            g.writeStartObject(null, 2);
            g.writeStringField("k", "v");
            g.writeFieldName("n");
            g.writeStartArray(null, 2);
            g.writeNumber(1);
            g.writeNumber(2.5);
            g.writeEndArray();
            g.writeEndObject();
        });
        assertEquals((byte) 0xA2, definite[0]);
        assertEquals("{\"k\":\"v\",\"n\":[1,2.5]}", json(definite, SourceFormat.CBOR));

        byte[] described = encode(CBORFactory.builder().enable(CBORGenerator.Feature.WRITE_TYPE_HEADER).build(), g -> {
            g.writeStartObject();
            g.writeNumberField("k", 1);
            g.writeEndObject();
        });
        assertEquals((byte) 0xD9, described[0]);
        assertEquals("{\"k\":1}", json(described, SourceFormat.CBOR));
    }

    @Test
    void cborHalfFloatsBignumsAndIntegerKeys() throws IOException {
        // {"h": 1.5 (half), "nan": NaN (half), "inf": +Infinity (half), "ninf": -Infinity (half)}
        byte[] halves = bytes(0xA4,
            0x61, 'h', 0xF9, 0x3E, 0x00,
            0x63, 'n', 'a', 'n', 0xF9, 0x7E, 0x00,
            0x63, 'i', 'n', 'f', 0xF9, 0x7C, 0x00,
            0x64, 'n', 'i', 'n', 'f', 0xF9, 0xFC, 0x00);
        assertEquals("{\"h\":1.5,\"nan\":\"NaN\",\"inf\":\"Infinity\",\"ninf\":\"-Infinity\"}", json(halves, SourceFormat.CBOR));
        // Tag 2 (positive bignum) 2^64 and tag 3 (negative bignum, -1 - n) with n = 2^64. Jackson 2.17's
        // CBOR parser reads tag 3 as -n; Elasticsearch 9.3 reads it with the same parser (2.17.2), and
        // the fixture's golden pins what it returns.
        byte[] bignums = bytes(0xA2,
            0x61, 'p', 0xC2, 0x49, 0x01, 0, 0, 0, 0, 0, 0, 0, 0,
            0x61, 'n', 0xC3, 0x49, 0x01, 0, 0, 0, 0, 0, 0, 0, 0);
        assertEquals("{\"p\":18446744073709551616,\"n\":-18446744073709551616}", json(bignums, SourceFormat.CBOR));
        // Tag 4 (decimal fraction) 27315 x 10^-2 reads as a BigDecimal, which Elasticsearch drops and fails on.
        assertEquals("{\"d\":273.15}", json(bytes(0xA1, 0x61, 'd', 0xC4, 0x82, 0x21, 0x19, 0x6A, 0xB3), SourceFormat.CBOR));
        // Integer keys are read as names.
        assertEquals("{\"1\":2}", json(bytes(0xA1, 0x01, 0x02), SourceFormat.CBOR));
        // A byte string value and a 64-bit double.
        assertEquals("{\"b\":\"AQI=\",\"d\":0.1}", json(bytes(0xA2, 0x61, 'b', 0x42, 0x01, 0x02,
            0x61, 'd', 0xFB, 0x3F, 0xB9, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9A), SourceFormat.CBOR));
    }

    /**
     * Jackson's YAML parser, as Elasticsearch configures it: YAML 1.1 booleans, empty plain scalars
     * as null, aliases as the anchor's name, timestamps as text. Its parser cannot read .nan and
     * .inf, which the converter writes as the strings the generator writes for non-finite doubles.
     */
    @Test
    void yamlAsJacksonReadsIt() throws IOException {
        String text = """
            ---
            # a comment
            plain: hello
            quoted_number: "12"
            int: 12
            hex: 0x1F
            float: 1.10
            exp: 1e3
            yes_word: yes
            bool: true
            tilde: ~
            null_word: null
            empty:
            empty_quoted: ""
            date: 2024-01-02
            stamp: 2024-01-02T03:04:05Z
            block: |
              line one
              line two
            folded: >
              one
              two
            anchor: &a anchored
            alias: *a
            list:
              - 1
              - two
            flow: {x: [1, 2]}
            nan: .nan
            inf: .inf
            ninf: -.inf
            overflow: 1.0e+400
            """;
        assertEquals("{\"plain\":\"hello\",\"quoted_number\":\"12\",\"int\":12,\"hex\":31,\"float\":1.1,\"exp\":1000.0,"
            + "\"yes_word\":true,\"bool\":true,\"tilde\":null,\"null_word\":null,\"empty\":null,\"empty_quoted\":\"\","
            + "\"date\":\"2024-01-02\",\"stamp\":\"2024-01-02T03:04:05Z\",\"block\":\"line one\\nline two\\n\","
            + "\"folded\":\"one two\\n\",\"anchor\":\"anchored\",\"alias\":\"a\",\"list\":[1,\"two\"],\"flow\":{\"x\":[1,2]},"
            + "\"nan\":\"NaN\",\"inf\":\"Infinity\",\"ninf\":\"-Infinity\",\"overflow\":\"Infinity\"}", yaml(text));
    }

    @Test
    void yamlStructureFailures() {
        fails("---\na: 1\n---\nb: 2\n".getBytes(StandardCharsets.UTF_8), SourceFormat.YAML);
        fails("---\n- 1\n- 2\n".getBytes(StandardCharsets.UTF_8), SourceFormat.YAML);
        fails(("---\n" + MARKER + "\n").getBytes(StandardCharsets.UTF_8), SourceFormat.YAML);
        fails(("---\na: [" + MARKER + "\n").getBytes(StandardCharsets.UTF_8), SourceFormat.YAML);
    }

    @Test
    void trailingAndTruncatedContentFails() throws IOException {
        byte[] doc = smile(g -> {
            g.writeStartObject();
            g.writeStringField("secret", MARKER);
            g.writeEndObject();
        });
        assertEquals("stored _source in SMILE format could not be converted to JSON (JsonEOFException)",
            fails(Arrays.copyOf(doc, doc.length - 1), SourceFormat.SMILE).getMessage());
        fails(Arrays.copyOf(doc, doc.length - 5), SourceFormat.SMILE);

        // A second root value: the SMILE token for null.
        byte[] twoRoots = Arrays.copyOf(doc, doc.length + 1);
        twoRoots[doc.length] = 0x21;
        assertEquals("stored _source in SMILE format could not be converted to JSON (content after the root object)",
            fails(twoRoots, SourceFormat.SMILE).getMessage());

        byte[] cborDoc = cbor(g -> {
            g.writeStartObject();
            g.writeStringField("secret", MARKER);
            g.writeEndObject();
        });
        fails(Arrays.copyOf(cborDoc, cborDoc.length - 3), SourceFormat.CBOR);
        byte[] cborTrailing = Arrays.copyOf(cborDoc, cborDoc.length + 1);
        cborTrailing[cborDoc.length] = 0x01;
        fails(cborTrailing, SourceFormat.CBOR);
        // A CBOR map header followed by garbage.
        fails(bytes(0xBF, 0xFF, 0xFF), SourceFormat.CBOR);
        fails(bytes(0xBF), SourceFormat.CBOR);
    }

    @Test
    void rootMustBeAnObject() throws IOException {
        byte[] array = smile(g -> {
            g.writeStartArray();
            g.writeEndArray();
        });
        assertEquals("stored _source in SMILE format could not be converted to JSON (root is not an object)",
            assertThrows(SourceConversionException.class,
                () -> SourceJsonConverter.toJson(array, array.length, SourceFormat.SMILE)).getMessage());
    }

    @Test
    void onlyConvertedFormatsAreAccepted() {
        byte[] doc = "{}".getBytes(StandardCharsets.UTF_8);
        assertThrows(IllegalArgumentException.class, () -> SourceJsonConverter.toJson(doc, doc.length, SourceFormat.JSON));
        assertThrows(IllegalArgumentException.class, () -> SourceJsonConverter.toJson(doc, doc.length, SourceFormat.UNKNOWN));
    }

    @Test
    void onlyTheGivenLengthIsRead() throws IOException {
        byte[] doc = smile(g -> {
            g.writeStartObject();
            g.writeNumberField("a", 1);
            g.writeEndObject();
        });
        byte[] padded = Arrays.copyOf(doc, doc.length + 16);
        Arrays.fill(padded, doc.length, padded.length, (byte) 0x7F);
        var json = SourceJsonConverter.toJson(padded, doc.length, SourceFormat.SMILE);
        assertEquals("{\"a\":1}", new String(json.bytes(), 0, json.length(), StandardCharsets.UTF_8));
    }

    /**
     * Elasticsearch's SMILE and CBOR factories disable {@code FAIL_ON_SYMBOL_HASH_OVERFLOW}, so a
     * document with many keys whose symbol-table hashes collide is indexed; it converts here too.
     * The keys collide whatever the factory's hash seed (Jackson mixes the seed in after the two
     * quads of an 8-byte name are combined), and a default factory refuses the same document.
     */
    @Test
    void manyCollidingKeysConvertAsElasticsearchReadsThem() throws IOException {
        java.util.List<String> names = collidingNames(1000);
        var root = com.fasterxml.jackson.core.sym.ByteQuadsCanonicalizer.createRoot();
        assertEquals(1, names.stream().mapToInt(n -> root.calcHash(quad(n, 0), quad(n, 4))).distinct().count());
        Body body = g -> {
            g.writeStartObject();
            for (int i = 0; i < names.size(); i++) g.writeNumberField(names.get(i), i);
            g.writeEndObject();
        };
        var expected = new StringBuilder("{");
        for (int i = 0; i < names.size(); i++) {
            expected.append(i == 0 ? "" : ",").append('"').append(names.get(i)).append("\":").append(i);
        }
        expected.append('}');
        for (JsonFactory factory : new JsonFactory[] {SmileFactory.builder().build(), CBORFactory.builder().build()}) {
            byte[] doc = encode(factory, body);
            assertThrows(com.fasterxml.jackson.core.exc.StreamConstraintsException.class, () -> {
                try (var parser = factory.createParser(doc)) {
                    while (parser.nextToken() != null) { }
                }
            }, factory.getFormatName() + ": the keys overflow a default factory's symbol table");
            SourceFormat format = factory instanceof SmileFactory ? SourceFormat.SMILE : SourceFormat.CBOR;
            assertEquals(expected.toString(), json(doc, format), factory.getFormatName());
        }
    }

    /** Distinct 8-byte printable names {@code q1 q2} whose pre-seed two-quad hash {@code mix(q1) + 33 * q2} is equal. */
    private static java.util.List<String> collidingNames(int count) {
        int inverse = 33;
        for (int i = 0; i < 5; i++) inverse *= 2 - 33 * inverse;
        java.util.List<String> names = new java.util.ArrayList<>();
        for (int q1 = 0x41414141; names.size() < count; q1++) {
            if (!printable(q1)) continue;
            int mixed = q1;
            mixed += mixed >>> 15;
            mixed ^= mixed >>> 9;
            int q2 = (0x5EED - mixed) * inverse;
            if (printable(q2)) names.add(ascii(q1) + ascii(q2));
        }
        return names;
    }

    private static boolean printable(int quad) {
        for (int shift = 0; shift < 32; shift += 8) {
            int b = (quad >>> shift) & 0xFF;
            if (b < 0x21 || b > 0x7E || b == '"' || b == '\\') return false;
        }
        return true;
    }

    private static String ascii(int quad) {
        return new String(new byte[] {(byte) (quad >>> 24), (byte) (quad >>> 16), (byte) (quad >>> 8), (byte) quad},
            StandardCharsets.US_ASCII);
    }

    private static int quad(String name, int from) {
        byte[] b = name.getBytes(StandardCharsets.US_ASCII);
        return (b[from] << 24) | ((b[from + 1] & 0xFF) << 16) | ((b[from + 2] & 0xFF) << 8) | (b[from + 3] & 0xFF);
    }
}
