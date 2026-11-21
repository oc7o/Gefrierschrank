package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class LuceneRuntimeSelectorTest {

    private ShardSnapshot shardWithVersions(String... writtenBy) {
        var files = Arrays.stream(writtenBy)
            .map(v -> new RepoFileInfo("x", "y", 1, "c", v, null))
            .toList();
        return new ShardSnapshot("u", "p/", "s", 0, 1, files);
    }

    /** Lucene 8 (ES 7), 9 (ES 8), 10 (ES 9) and mixed shards all open in-process; Lucene 7 fails at the open. */
    @Test
    void everyShardWithAWrittenByVersionIsOpenedInProcess() {
        for (var versions : new String[][]{{"7.7.3"}, {"8.11.1"}, {"8.11.1", "9.12.0"}, {"9.12.0"}, {"9.12.0", "10.3.2"},
            {"10.3.2"}, {null, "9.12.0"}}) {
            assertDoesNotThrow(() -> LuceneRuntimeSelector.requireWrittenBy(shardWithVersions(versions)), Arrays.toString(versions));
        }
    }

    @Test
    void unknownVersionFails() {
        assertThrows(IllegalArgumentException.class,
            () -> LuceneRuntimeSelector.requireWrittenBy(shardWithVersions((String) null)));
        assertThrows(IllegalArgumentException.class, () -> LuceneRuntimeSelector.requireWrittenBy(shardWithVersions()));
    }

    @Test
    void theRemovedWorkerOptOutIsRefusedWithAFixedMessage() throws Exception {
        String property = LuceneRuntimeSelector.LUCENE8_RUNTIME_PROPERTY;
        for (String accepted : new String[]{"in-process", "", " "}) {
            Es7InProcessExportTest.withProperties(new String[]{property, accepted}, () -> {
                assertNull(LuceneRuntimeSelector.lucene8RuntimeRefusal(), "'" + accepted + "'");
                return null;
            });
        }
        String previous = System.getProperty(property);
        System.clearProperty(property);
        try {
            assertNull(LuceneRuntimeSelector.lucene8RuntimeRefusal());
        } finally {
            if (previous != null) System.setProperty(property, previous);
        }
        Es7InProcessExportTest.withProperties(new String[]{property, "worker"}, () -> {
            assertEquals("the Lucene 9 worker was removed in 1.2.0", LuceneRuntimeSelector.lucene8RuntimeRefusal());
            return null;
        });
        Es7InProcessExportTest.withProperties(new String[]{property, "Worker"}, () -> {
            assertEquals("gefrierschrank.lucene8.runtime must be 'in-process' (the Lucene 9 worker was removed in 1.2.0)",
                LuceneRuntimeSelector.lucene8RuntimeRefusal());
            return null;
        });
    }
}
