package de.sloow.gefrierschrank.core;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FailureDiagnosticsTest {
    @Test
    void includesCallerOfRequireNonNullWithoutMessages() {
        try {
            java.util.Objects.requireNonNull(null, "SENSITIVE document and credentials");
            fail("expected failure");
        } catch (NullPointerException e) {
            String diagnostic = FailureDiagnostics.describe(new IllegalStateException("SENSITIVE wrapper", e));
            assertTrue(diagnostic.contains("Objects.requireNonNull"));
            assertTrue(diagnostic.contains("FailureDiagnosticsTest.includesCaller"));
            assertFalse(diagnostic.contains("SENSITIVE"));
        }
    }

    @Test
    void onlySafeMessageRootsHaveTheirMessageAppended() {
        String safe = FailureDiagnostics.describe(
            new IllegalStateException("SENSITIVE wrapper", new InvalidSetting("some.property must be 'a' or 'b'")));
        assertTrue(safe.startsWith(InvalidSetting.class.getName() + ": some.property must be 'a' or 'b' at "), safe);
        assertFalse(safe.contains("SENSITIVE"), safe);

        String plain = FailureDiagnostics.describe(new IllegalArgumentException("SENSITIVE value"));
        assertTrue(plain.startsWith(IllegalArgumentException.class.getName() + " at "), plain);
        assertFalse(plain.contains("SENSITIVE"), plain);

        String wrappedByMarker = FailureDiagnostics.describe(
            new InvalidSetting("fixed text", new java.io.IOException("SENSITIVE cause")));
        assertFalse(wrappedByMarker.contains("SENSITIVE"), "only the root's own marker counts: " + wrappedByMarker);
        assertFalse(wrappedByMarker.contains("fixed text"), wrappedByMarker);

        assertTrue(FailureDiagnostics.describe(new InvalidSetting(null)).startsWith(InvalidSetting.class.getName() + " at "));
    }

    @Test
    void findLooksThroughCausesAndSuppressedFailuresBreadthFirst() {
        var target = new java.io.UncheckedIOException(new java.io.IOException("disk"));
        var first = new IllegalStateException("first shard", new RuntimeException("refused"));
        var second = new IllegalStateException("second shard", target);
        first.addSuppressed(second);
        assertSame(target, FailureDiagnostics.find(first, java.io.UncheckedIOException.class));
        assertSame(first, FailureDiagnostics.find(first, IllegalStateException.class), "the failure itself comes first");
        assertNull(FailureDiagnostics.find(first, ArithmeticException.class));

        var cyclic = new RuntimeException("a");
        var other = new RuntimeException("b", cyclic);
        cyclic.addSuppressed(other);
        assertNull(FailureDiagnostics.find(cyclic, ArithmeticException.class), "a cycle ends the search");
    }

    private static final class InvalidSetting extends IllegalArgumentException implements FailureDiagnostics.SafeMessage {
        InvalidSetting(String message) {
            super(message);
        }

        InvalidSetting(String message, Throwable cause) {
            super(message, cause);
        }
    }

    @Test
    void cyclicCausesDoNotHangReporting() {
        var first = new Exception();
        var second = new Exception(first);
        first.initCause(second);
        assertSame(second, FailureDiagnostics.rootCause(first));
        assertNotNull(FailureDiagnostics.describe(first));
    }
}
