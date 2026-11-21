package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@code dump-to-delta --source-disabled-accept}: an opt-in, set while parsing. */
class SourceDisabledAcceptOptionTest {

    private String previous;

    @BeforeEach
    void saveProperty() {
        previous = System.getProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY);
        System.clearProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY);
    }

    @AfterEach
    void restoreProperty() {
        if (previous == null) System.clearProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY);
        else System.setProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY, previous);
    }

    @Test
    void theValidValueSetsTheProperty() {
        new CommandLine(new GefrierschrankCommand()).parseArgs("dump-to-delta", "repo", "--index", "i", "-o", "out",
            "--source-disabled-accept", "unmapped-values");
        assertEquals(SyntheticCoverage.ACCEPT_UNMAPPED_VALUES, System.getProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY));
        assertTrue(SyntheticCoverage.acceptsUnmappedValues());
    }

    @Test
    void anyOtherValueIsAUsageErrorBeforeAnyRepositoryRead(@TempDir Path tmp) {
        Path repo = tmp.resolve("no-such-repository");
        StringWriter err = new StringWriter();
        int exit = new CommandLine(new GefrierschrankCommand()).setErr(new PrintWriter(err)).execute("dump-to-delta", repo.toString(),
            "--index", "i", "-o", tmp.resolve("out").toString(), "--source-disabled-accept", "everything");
        assertEquals(2, exit, err.toString());
        assertTrue(err.toString().contains("--source-disabled-accept accepts only unmapped-values"), err.toString());
        assertNull(System.getProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY));
        assertFalse(Files.exists(tmp.resolve("out")), "nothing was read or written");
    }

    @Test
    void withoutTheOptionThePropertyIsUntouched() {
        System.setProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY, "set-by-the-jvm");
        new CommandLine(new GefrierschrankCommand()).parseArgs("dump-to-delta", "repo", "--index", "i", "-o", "out");
        assertEquals("set-by-the-jvm", System.getProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY));
        assertFalse(SyntheticCoverage.acceptsUnmappedValues());
    }
}
