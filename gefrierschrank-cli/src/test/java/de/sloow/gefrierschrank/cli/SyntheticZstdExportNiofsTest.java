package de.sloow.gefrierschrank.cli;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/** Every case of {@link SyntheticZstdExportTest} again, reconstructing through NIOFS instead of memory-mapped files. */
class SyntheticZstdExportNiofsTest extends SyntheticZstdExportTest {

    private static final String DIRECTORY_PROPERTY = "gefrierschrank.reconstruction.directory";

    @BeforeAll
    static void readThroughNiofs() {
        System.setProperty(DIRECTORY_PROPERTY, "niofs");
    }

    @AfterAll
    static void restoreDefaultDirectory() {
        System.clearProperty(DIRECTORY_PROPERTY);
    }
}
