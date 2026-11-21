package de.sloow.gefrierschrank.syntheticsource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

/** Every case of {@link SourceVectorsRehydratorTest} again, reading the shard through NIOFS instead of memory-mapped files. */
class SourceVectorsRehydratorNiofsTest extends SourceVectorsRehydratorTest {

    @BeforeAll
    static void readThroughNiofs() {
        System.setProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY, "niofs");
    }

    @AfterAll
    static void restoreDefaultDirectory() {
        System.clearProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY);
    }
}
