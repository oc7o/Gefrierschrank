package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class AvroStringsTest {

    @Test
    void validUnicodeKeepsItsExactUtf8Bytes() {
        for (String value : new String[]{"", "plain ascii {\"a\":1}", "Grüße, Ærø", "日本語のテキスト",
                "emoji 😀 pair", "😀", "mixed ü 中 🎉 end\n\t\\\""}) {
            var utf8 = AvroStrings.utf8(value);
            assertArrayEquals(value.getBytes(StandardCharsets.UTF_8),
                Arrays.copyOf(utf8.getBytes(), utf8.getByteLength()), value);
            assertEquals(value, utf8.toString());
        }
    }

    @Test
    void unpairedSurrogatesAreRejectedWithoutEchoingTheValue() {
        for (String value : new String[]{"secret\uD800", "secret\uD800x", "secret\uDC00x", "\uDC00secret",
                "secret\uDE00\uD83D", "secret\uD83D"}) {
            var failure = assertThrows(IllegalArgumentException.class, () -> AvroStrings.utf8(value));
            assertFalse(failure.getMessage().contains("secret"), failure.getMessage());
        }
    }
}
