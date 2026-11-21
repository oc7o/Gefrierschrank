package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Values sent under {@code dynamic: false}/{@code dynamic: runtime} objects of a document
 * without stored {@code _source} left no trace. By default its shard quarantines naming both tokens;
 * with {@code --source-disabled-accept unmapped-values} the shard commits without those values,
 * the index prints one content-free warning and the shard reports the loss.
 */
class SourceDisabledAcceptTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String INDEX = "cov_srcoff_dynamic";
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
    void withoutTheOptInTheSourcelessShardQuarantines(@TempDir Path tmp) throws Exception {
        var export = SynthCovFixtures.export(INDEX, tmp.resolve("table"), new Configuration());
        assertEquals(RunReport.Outcome.COMMITTED, export.shard(0).outcome(), export.report().shards().toString());
        assertEquals(RunReport.Outcome.QUARANTINED, export.shard(1).outcome());
        assertTrue(SynthCovFixtures.message(export.shard(1)).endsWith(": dynamic:false dynamic:runtime"), export.shard(1).errorMessage());
        assertFalse(export.stderr().contains("--source-disabled-accept"), "no warning without the opt-in");
        assertNull(SynthCovFixtures.readShard(INDEX, 0, tmp).unrecoverableUnmappedValues());
    }

    @Test
    void withTheOptInEveryShardCommitsWithoutTheUnmappedValues(@TempDir Path tmp) throws Exception {
        System.setProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY, SyntheticCoverage.ACCEPT_UNMAPPED_VALUES);
        JsonNode expected = SynthCovFixtures.expected(INDEX);
        var export = SynthCovFixtures.export(INDEX, tmp.resolve("table"), new Configuration());
        assertEquals(2, export.report().committedCount(), export.report().shards().toString());
        Set<String> reconstructed = new HashSet<>();
        expected.path("reconstructed").forEach(id -> reconstructed.add(id.asText()));
        assertEquals(expected.path("shards").size(), export.rows().size());
        for (var entry : export.rows().entrySet()) {
            String id = entry.getKey();
            if (reconstructed.contains(id)) {
                assertEquals("synthetic", entry.getValue().kind());
                OrderedJsonAssert.assertOrderedEquals(expected.path("sources_optin").path(id), JSON.readTree(entry.getValue().source()), id);
            } else {
                assertEquals(expected.path("input").path(id).asText(), entry.getValue().source(), id + " byte-exact");
            }
        }
        String warning = "warning: index " + INDEX + ": --source-disabled-accept unmapped-values is set; values sent under"
            + " dynamic:false or dynamic:runtime objects were kept only in the disabled _source and are missing, uncounted,"
            + " from documents reconstructed without stored _source (their shard reports carry unrecoverableUnmappedValues: true)";
        assertEquals(1, export.stderr().lines().filter(warning::equals).count(), export.stderr());
        assertFalse(export.stderr().contains("lost-") || export.stderr().contains("shadowed-"), "content-free");

        assertEquals(Boolean.TRUE, SynthCovFixtures.readShard(INDEX, 1, tmp).unrecoverableUnmappedValues());
        assertNull(SynthCovFixtures.readShard(INDEX, 0, tmp.resolve("stored-only")).unrecoverableUnmappedValues(),
            "a shard that reconstructed nothing reports nothing");
    }
}
