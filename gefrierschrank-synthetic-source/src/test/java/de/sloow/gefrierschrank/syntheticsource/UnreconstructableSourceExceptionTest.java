package de.sloow.gefrierschrank.syntheticsource;

import de.sloow.gefrierschrank.core.FailureDiagnostics;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnreconstructableSourceExceptionTest {

    @Test
    void theMessageIsFixedTextAndSortedDistinctTokens() {
        var failure = new UnreconstructableSourceException(List.of("text:no-native-source", "dynamic:false",
            "binary:not-indexed", "dynamic:false"));
        assertEquals("documents without stored _source cannot be reconstructed from this index's doc values and stored "
            + "fields: binary:not-indexed dynamic:false text:no-native-source", failure.getMessage());
        assertEquals("binary:not-indexed dynamic:false text:no-native-source", failure.tokens());
        assertNull(failure.getCause());
    }

    /** The fallback reader treats "synthetic source" in a delegate's message as its trigger. */
    @Test
    void itIsASafeIoExceptionWithoutTheFallbackTrigger() {
        var failure = new UnreconstructableSourceException(List.of("unmapped"));
        assertInstanceOf(IOException.class, failure);
        assertInstanceOf(FailureDiagnostics.SafeMessage.class, failure);
        assertFalse(failure.getMessage().contains("synthetic source"));
        assertTrue(ReconstructionCoverageTest.inVocabulary("unmapped"));
    }

    @Test
    void theVocabularyAcceptsTypeTokensOnly() {
        for (String token : List.of("object:enabled-false", "dynamic:false", "dynamic:runtime", "binary:not-indexed",
                "nested:keep-all", "runtime:shadowing", "flattened:ignore_above", "histogram:diverted", "geo_shape:fallback",
                "keyword:copy_to", "text:no-native-source", "unmapped", "mapping-rejected:MapperParsingException")) {
            assertTrue(ReconstructionCoverageTest.inVocabulary(token), token);
        }
        for (String token : List.of("message:no-native-source", "host.name:fallback", "keyword", "text:anything")) {
            assertFalse(ReconstructionCoverageTest.inVocabulary(token), token);
        }
    }
}
