package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/** {@code --selective-fetch} and its property on dump-to-delta. */
class SelectiveFetchOptionTest {

    static final Path DEMO = Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");

    private String saved;
    private String savedMinBytes;

    @BeforeEach
    void save() {
        saved = System.getProperty(SelectiveFetch.PROPERTY);
        savedMinBytes = System.getProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY);
    }

    @AfterEach
    void restore() {
        if (saved == null) System.clearProperty(SelectiveFetch.PROPERTY);
        else System.setProperty(SelectiveFetch.PROPERTY, saved);
        if (savedMinBytes == null) System.clearProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY);
        else System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, savedMinBytes);
    }

    static int dumpToDelta(String... args) {
        return new CommandLine(new DumpToDeltaCommand()).execute(args);
    }

    @Test
    void theOptionTurnsSelectiveFetchOnForTheRun(@TempDir Path tmp) {
        System.clearProperty(SelectiveFetch.PROPERTY);
        assertEquals(1, dumpToDelta(DEMO.toString(), "--index", "no-such-index", "-o", tmp.resolve("out").toString()));
        assertNull(System.getProperty(SelectiveFetch.PROPERTY), "off by default");
        assertEquals(1, dumpToDelta(DEMO.toString(), "--index", "no-such-index", "-o", tmp.resolve("out").toString(),
            "--selective-fetch"));
        assertEquals("true", System.getProperty(SelectiveFetch.PROPERTY));
    }

    @Test
    void anInvalidPropertyIsAUsageErrorBeforeAnyRepositoryRead(@TempDir Path tmp) {
        for (String invalid : List.of("yes", "1", "")) {
            System.setProperty(SelectiveFetch.PROPERTY, invalid);
            assertEquals(2, dumpToDelta(DEMO.toString(), "--index", "test_data_9_3_0", "-o", tmp.resolve("out").toString()), invalid);
            assertFalse(Files.exists(tmp.resolve("out")));
        }
        System.setProperty(SelectiveFetch.PROPERTY, "FALSE");
        assertEquals(1, dumpToDelta(DEMO.toString(), "--index", "no-such-index", "-o", tmp.resolve("out").toString()));
    }

    /**
     * An invalid probe threshold is a usage error before any repository read, with the flag or
     * without it, never a quarantined shard.
     */
    @Test
    void anInvalidSyntheticMinBytesIsAUsageErrorBeforeAnyRepositoryRead(@TempDir Path tmp) {
        System.clearProperty(SelectiveFetch.PROPERTY);
        for (String invalid : List.of("-1", "16MB", "", "1.5")) {
            System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, invalid);
            assertEquals(2, dumpToDelta(DEMO.toString(), "--index", "test_data_9_3_0", "-o", tmp.resolve("out").toString()), invalid);
            assertEquals(2, dumpToDelta(DEMO.toString(), "--index", "test_data_9_3_0", "-o", tmp.resolve("out").toString(),
                "--selective-fetch"), invalid);
            assertFalse(Files.exists(tmp.resolve("out")));
        }
        System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, " 0 ");
        assertEquals(1, dumpToDelta(DEMO.toString(), "--index", "no-such-index", "-o", tmp.resolve("out").toString(),
            "--selective-fetch"));
    }
}
