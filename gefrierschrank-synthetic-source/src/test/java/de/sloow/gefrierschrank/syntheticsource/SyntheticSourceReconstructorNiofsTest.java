package de.sloow.gefrierschrank.syntheticsource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/** Every case of {@link SyntheticSourceReconstructorTest} again, reconstructing through NIOFS instead of memory-mapped files. */
class SyntheticSourceReconstructorNiofsTest extends SyntheticSourceReconstructorTest {

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
