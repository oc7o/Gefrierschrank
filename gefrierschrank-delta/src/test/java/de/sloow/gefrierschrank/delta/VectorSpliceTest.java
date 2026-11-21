package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VectorSpliceTest {

    private static final String[] EMBEDDING = {"embedding"};
    private static final String MEMBER = "\"embedding\":[0.5,-0.38599998]";

    private final VectorSplice splice = new VectorSplice();

    @Test
    void membersGoBeforeTheFinalBraceAfterEveryStoredByte() throws Exception {
        assertSpliced("{\"title\":\"a\",\"n\":1.50}", "{\"title\":\"a\",\"n\":1.50," + MEMBER + "}");
        assertSpliced("{ \"title\" : \"a\" }", "{ \"title\" : \"a\" ," + MEMBER + "}");
    }

    @Test
    void anEmptyObjectGetsNoComma() throws Exception {
        assertSpliced("{}", "{" + MEMBER + "}");
        assertSpliced("{ \n }", "{ \n " + MEMBER + "}");
    }

    @Test
    void whitespaceAroundTheObjectStaysWhereItWas() throws Exception {
        assertSpliced("\n  {\"a\":1}\r\n\t", "\n  {\"a\":1," + MEMBER + "}\r\n\t");
    }

    @Test
    void escapedKeysAndSeveralMembersKeepTheirOrder() throws Exception {
        byte[] members = "\"b\\u00e9\":[1],\"z\":{\"x\":0.25}".getBytes(StandardCharsets.UTF_8);
        byte[] out = splice.splice(bytes("{\"k\\\"ey\":\"v\"}"), bytes("{\"k\\\"ey\":\"v\"}").length,
            new String[] {"bé", "z"}, members);
        assertEquals("{\"k\\\"ey\":\"v\",\"b\\u00e9\":[1],\"z\":{\"x\":0.25}}", new String(out, StandardCharsets.UTF_8));
    }

    @Test
    void aRootKeyCollisionIsLeftToElasticsearch() {
        assertNull(spliced("{\"embedding\":null,\"a\":1}"));
        assertNull(spliced("{\"a\":{\"x\":1},\"embedding\":[1]}"));
        String nested = "{\"a\":{\"embedding\":[1]}}";
        assertEquals("{\"a\":{\"embedding\":[1]}," + MEMBER + "}", spliced(nested), "only root keys collide");
    }

    @ParameterizedTest
    @ValueSource(strings = {"[1]", ":)\nú", "", "   ", "\"text\"", "{\"a\":1}{\"b\":2}", "{\"a\":", "{\"a\":1,}",
        "{\"a\":1} x", "{\"a\" 1}", "---\na: 1\n", "{a:1}"})
    void anythingButOneJsonObjectIsLeftToElasticsearch(String stored) {
        assertNull(spliced(stored), stored);
    }

    @Test
    void onlyTheGivenLengthCounts() {
        byte[] stored = bytes("{\"a\":1}garbage");
        byte[] out = splice.splice(stored, 7, EMBEDDING, bytes(MEMBER));
        assertEquals("{\"a\":1," + MEMBER + "}", new String(out, StandardCharsets.UTF_8));
    }

    private void assertSpliced(String stored, String expected) throws Exception {
        String out = spliced(stored);
        assertEquals(expected, out);
        // Removing the inserted bytes gives the stored bytes back, byte for byte.
        int at = out.indexOf(MEMBER);
        String without = out.substring(0, at) + out.substring(at + MEMBER.length());
        assertTrue(without.equals(stored) || without.equals(stored.substring(0, stored.lastIndexOf('}')) + ","
            + stored.substring(stored.lastIndexOf('}'))), without);
        assertArrayEquals(bytes(stored.substring(0, stored.lastIndexOf('}'))),
            bytes(out.substring(0, stored.lastIndexOf('}'))));
        assertTrue(new SourceValidator().accepts(bytes(out), bytes(out).length), out);
        ArchiveContext.validateSource(out);
    }

    private String spliced(String stored) {
        byte[] out = splice.splice(bytes(stored), bytes(stored).length, EMBEDDING, bytes(MEMBER));
        return out == null ? null : new String(out, StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
