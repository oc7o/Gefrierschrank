package de.sloow.gefrierschrank.core.source;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The rules of x-content 9.3.0's {@code XContentFactory.xContentType(InputStream)} and the
 * {@code detectContent} methods of its JSON, SMILE, YAML and CBOR implementations, rule by rule.
 */
class SourceFormatTest {

    private static SourceFormat detect(int... bytes) {
        byte[] array = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) array[i] = (byte) bytes[i];
        return SourceFormat.detect(array, array.length);
    }

    private static SourceFormat detect(String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        return SourceFormat.detect(bytes, bytes.length);
    }

    @Test
    void jsonStartsWithABrace() {
        assertEquals(SourceFormat.JSON, detect("{}"));
        assertEquals(SourceFormat.JSON, detect("{"));
        assertEquals(SourceFormat.JSON, detect(" \t\r\n{\"a\":1}"));
    }

    @Test
    void smileNeedsItsThreeByteHeader() {
        assertEquals(SourceFormat.SMILE, detect(':', ')', '\n', 0x00, 0xFA));
        assertEquals(SourceFormat.SMILE, detect(':', ')', '\n'));
        assertEquals(SourceFormat.UNKNOWN, detect(':', ')'));
        assertEquals(SourceFormat.UNKNOWN, detect(':', ')', ' '));
    }

    @Test
    void yamlNeedsTheDocumentMarker() {
        assertEquals(SourceFormat.YAML, detect("---\na: 1\n"));
        assertEquals(SourceFormat.YAML, detect("---"));
        assertEquals(SourceFormat.UNKNOWN, detect("--"));
        assertEquals(SourceFormat.UNKNOWN, detect("a: 1\n"));
        assertEquals(SourceFormat.UNKNOWN, detect("- 1\n"));
    }

    @Test
    void cborRules() {
        // Indefinite-length map with at least one more byte.
        assertEquals(SourceFormat.CBOR, detect(0xBF, 0xFF));
        // 0xBF alone fails the first rule, but its major type is 5 (map), which the last rule accepts.
        assertEquals(SourceFormat.CBOR, detect(0xBF));
        // Self-describe tag 55799.
        assertEquals(SourceFormat.CBOR, detect(0xD9, 0xD9, 0xF7, 0xA0));
        assertEquals(SourceFormat.CBOR, detect(0xD9, 0xD9, 0xF7));
        assertEquals(SourceFormat.UNKNOWN, detect(0xD9, 0xD9));
        assertEquals(SourceFormat.UNKNOWN, detect(0xD9, 0xD9, 0xF6, 0xA0));
        // Another tag (major type 6) is not a map.
        assertEquals(SourceFormat.UNKNOWN, detect(0xC0, 0xA0));
        assertEquals(SourceFormat.UNKNOWN, detect(0xC2, 0x41, 0x01));
        for (int header = 0xA0; header <= 0xBF; header++) {
            assertEquals(SourceFormat.CBOR, detect(header, 0x00), "map header " + Integer.toHexString(header));
        }
        // Arrays (major type 4) and text strings (major type 3) are not.
        assertEquals(SourceFormat.UNKNOWN, detect(0x80));
        assertEquals(SourceFormat.UNKNOWN, detect(0x9F, 0xFF));
        assertEquals(SourceFormat.UNKNOWN, detect(0x61, 'a'));
    }

    @Test
    void byteOrderMarkThenWhitespaceThenBraceIsJson() {
        assertEquals(SourceFormat.JSON, detect(0xEF, 0xBB, 0xBF, '{'));
        assertEquals(SourceFormat.JSON, detect(0xEF, 0xBB, 0xBF, ' ', '\n', '{', '}'));
        // The mark counts only with more than three bytes.
        assertEquals(SourceFormat.UNKNOWN, detect(0xEF, 0xBB, 0xBF));
        assertEquals(SourceFormat.UNKNOWN, detect(0xEF, 0xBB, 0xBF, 'x'));
        // The mark must come first after the stream's whitespace skip.
        assertEquals(SourceFormat.JSON, detect(' ', 0xEF, 0xBB, 0xBF, '{'));
    }

    @Test
    void leadingWhitespaceIsSkippedBeforeEveryRule() {
        assertEquals(SourceFormat.SMILE, detect(' ', '\n', ':', ')', '\n', 0xFA));
        assertEquals(SourceFormat.YAML, detect("\n---\na: 1"));
        assertEquals(SourceFormat.CBOR, detect('\t', 0xBF, 0xFF));
        // Character.isWhitespace on the unsigned byte: the information separators count, NEL (0x85) does not.
        assertEquals(SourceFormat.JSON, detect(0x1C, 0x1F, 0x0B, 0x0C, '{'));
        assertEquals(SourceFormat.UNKNOWN, detect(0x85, '{'));
    }

    @Test
    void theStreamVariantLooksAtTwentyBytesAfterTheWhitespace() {
        byte[] bomThenSpaces = new byte[30];
        bomThenSpaces[0] = (byte) 0xEF;
        bomThenSpaces[1] = (byte) 0xBB;
        bomThenSpaces[2] = (byte) 0xBF;
        for (int i = 3; i < 29; i++) bomThenSpaces[i] = ' ';
        bomThenSpaces[29] = '{';
        assertEquals(SourceFormat.UNKNOWN, SourceFormat.detect(bomThenSpaces, bomThenSpaces.length));
        bomThenSpaces[19] = '{';
        assertEquals(SourceFormat.JSON, SourceFormat.detect(bomThenSpaces, bomThenSpaces.length));
    }

    @Test
    void emptyNullAndOtherContentAreUnknown() {
        assertEquals(SourceFormat.UNKNOWN, SourceFormat.detect(null, 0));
        assertEquals(SourceFormat.UNKNOWN, SourceFormat.detect(new byte[0], 0));
        assertEquals(SourceFormat.UNKNOWN, SourceFormat.detect(new byte[] {'{'}, 0));
        assertEquals(SourceFormat.UNKNOWN, detect("   \n"));
        assertEquals(SourceFormat.UNKNOWN, detect("[1]"));
        assertEquals(SourceFormat.UNKNOWN, detect("\"x\""));
        assertEquals(SourceFormat.UNKNOWN, detect("plain text"));
        assertEquals(SourceFormat.UNKNOWN, detect(0x00, 0x01));
    }

    @Test
    void onlyTheLengthIsInspected() {
        byte[] bytes = {':', ')', '\n', '{'};
        assertEquals(SourceFormat.UNKNOWN, SourceFormat.detect(bytes, 2));
        assertEquals(SourceFormat.SMILE, SourceFormat.detect(bytes, 3));
        assertEquals(SourceFormat.SMILE, SourceFormat.detect(bytes, 99));
    }

    @Test
    void kinds() {
        assertEquals("stored", SourceFormat.JSON.storedKind());
        assertEquals("stored", SourceFormat.UNKNOWN.storedKind());
        assertEquals("stored-smile", SourceFormat.SMILE.storedKind());
        assertEquals("stored-cbor", SourceFormat.CBOR.storedKind());
        assertEquals("stored-yaml", SourceFormat.YAML.storedKind());
        assertTrue(SourceFormat.SMILE.converted());
        assertTrue(SourceFormat.CBOR.converted());
        assertTrue(SourceFormat.YAML.converted());
        assertFalse(SourceFormat.JSON.converted());
        assertFalse(SourceFormat.UNKNOWN.converted());
    }
}
