package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/** Every case of {@link ParallelSyntheticReconstructionTest} again, reconstructing through NIOFS instead of memory-mapped files. */
class ParallelSyntheticReconstructionNiofsTest extends ParallelSyntheticReconstructionTest {

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
