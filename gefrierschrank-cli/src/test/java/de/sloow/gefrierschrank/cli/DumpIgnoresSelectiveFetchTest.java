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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** dump keeps the full fetch in 1.2.0: no option, and the property changes nothing it writes. */
class DumpIgnoresSelectiveFetchTest {

    static final Path CORE = Path.of("../gefrierschrank-core/src/test/resources/fixtures");

    private String saved;

    @BeforeEach
    void save() {
        saved = System.getProperty(SelectiveFetch.PROPERTY);
    }

    @AfterEach
    void restore() {
        if (saved == null) System.clearProperty(SelectiveFetch.PROPERTY);
        else System.setProperty(SelectiveFetch.PROPERTY, saved);
    }

    static int dump(String... args) {
        return new CommandLine(new DumpCommand()).execute(args);
    }

    @Test
    void dumpHasNoSelectiveFetchOption(@TempDir Path tmp) {
        assertEquals(2, dump(CORE.resolve("demo-repo").toString(), "--index", "test_data_9_3_0",
            "-o", tmp.resolve("out.jsonl").toString(), "--selective-fetch"));
        assertFalse(Files.exists(tmp.resolve("out.jsonl")));
    }

    @Test
    void thePropertyLeavesTheDumpByteIdentical(@TempDir Path tmp) throws Exception {
        for (String[] export : List.of(new String[] {"demo-repo", "test_data_8_17_0"}, new String[] {"frozen-repo", "frozen-docs-f5ffca09"},
                new String[] {"es7-features-repo", "es7_hard_deletes"})) {
            System.clearProperty(SelectiveFetch.PROPERTY);
            Path full = tmp.resolve(export[1] + "-full.jsonl");
            assertEquals(0, dump(CORE.resolve(export[0]).toString(), "--index", export[1], "-o", full.toString()));
            System.setProperty(SelectiveFetch.PROPERTY, "true");
            Path flagged = tmp.resolve(export[1] + "-flagged.jsonl");
            assertEquals(0, dump(CORE.resolve(export[0]).toString(), "--index", export[1], "-o", flagged.toString()));
            assertTrue(Files.size(full) > 0);
            assertArrayEquals(Files.readAllBytes(full), Files.readAllBytes(flagged), export[1]);
        }
    }
}
