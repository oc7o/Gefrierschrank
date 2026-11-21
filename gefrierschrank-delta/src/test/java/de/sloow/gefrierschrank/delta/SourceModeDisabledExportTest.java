package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage;
import de.sloow.gefrierschrank.syntheticsource.SyntheticSourceReconstructor;
import de.sloow.gefrierschrank.syntheticsource.UnreconstructableSourceException;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ES 8+ indices created with {@code index.mapping.source.mode: disabled} never stored a
 * {@code _source}; they are reconstructed like documents whose source was disabled later. Every row
 * is {@code synthetic} and equals the native-synthetic twin's rendering; a mapping-level refusal
 * fails the index before any shard blob is read, with exactly what the worker would throw; a
 * per-segment refusal quarantines its shard after the download; the kill switch restores 1.1.0's
 * refusal before download.
 */
class SourceModeDisabledExportTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private String previousKillSwitch;

    @BeforeEach
    void saveKillSwitch() {
        previousKillSwitch = System.getProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY);
    }

    @AfterEach
    void restoreKillSwitch() {
        if (previousKillSwitch == null) System.clearProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY);
        else System.setProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, previousKillSwitch);
    }

    @ParameterizedTest
    @ValueSource(strings = {"cov_srcmode_native", "cov8_srcmode_native"})
    void everyDocumentIsReconstructedAndEqualsTheTwin(String index, @TempDir Path tmp) throws Exception {
        JsonNode expected = SynthCovFixtures.expected(index);
        var export = SynthCovFixtures.export(index, tmp.resolve("table"), new Configuration());
        assertEquals(2, export.report().committedCount(), export.report().shards().toString());
        assertEquals(expected.path("sources").size(), export.rows().size());
        for (var entry : export.rows().entrySet()) {
            assertEquals("synthetic", entry.getValue().kind(), entry.getKey());
            OrderedJsonAssert.assertOrderedEquals(expected.path("sources").path(entry.getKey()),
                JSON.readTree(entry.getValue().source()), entry.getKey());
        }
        long withConstant = export.rows().values().stream().filter(row -> row.source().contains("\"dataset\":\"coverage\"")).count();
        assertEquals(export.rows().size(), withConstant, "the constant_keyword value is in every reconstructed row");
    }

    @Test
    void aMappingLevelRefusalFailsTheIndexBeforeAnyShardBlobIsRead(@TempDir Path tmp) throws Exception {
        String index = "cov_srcmode_mapping";
        Path table = tmp.resolve("table");
        var store = new SynthCovFixtures.RecordingStore(
            new de.sloow.gefrierschrank.core.repo.FileSystemBlobStore(SynthCovFixtures.repository(index)));
        var sources = new de.sloow.gefrierschrank.core.repo.RepositorySources(
            new de.sloow.gefrierschrank.core.repo.RepositorySources.Source("primary", store));
        var failure = assertThrows(UnreconstructableSourceException.class, () -> new FullIndexExporter(sources, new Configuration(),
            (shard, dir) -> new de.sloow.gefrierschrank.lucene10.Lucene10ShardReader(dir)).run(index, table.toString(), RunReportListener.NO_OP));
        var mapping = assertThrows(UnreconstructableSourceException.class,
            () -> SyntheticCoverage.requireMappingReconstructable(index, SynthCovFixtures.metadata(index)));
        assertEquals(mapping.getMessage(), failure.getMessage());
        assertEquals("object:enabled-false", failure.tokens());
        assertFalse(Files.exists(table.resolve("_delta_log")), "no table");
        assertFalse(Files.exists(table.resolve("_export_metadata")), "no metadata row");
        String indexId = new de.sloow.gefrierschrank.core.repo.RepositoryInspector(
            new de.sloow.gefrierschrank.core.repo.FileSystemBlobStore(SynthCovFixtures.repository(index))).index(index).repoUuid();
        assertTrue(store.read.stream().noneMatch(key -> key.startsWith("indices/" + indexId + "/0/")
            || key.startsWith("indices/" + indexId + "/1/")), "no shard blob read: " + store.read);
    }

    @Test
    void aPerSegmentRefusalQuarantinesItsShardAfterTheDownload(@TempDir Path tmp) throws Exception {
        String index = "cov_srcmode_text";
        JsonNode expected = SynthCovFixtures.expected(index);
        var export = SynthCovFixtures.export(index, tmp.resolve("table"), new Configuration());
        assertEquals(RunReport.Outcome.COMMITTED, export.shard(0).outcome(), export.report().shards().toString());
        assertEquals(RunReport.Outcome.QUARANTINED, export.shard(1).outcome());
        assertEquals("UnreconstructableSourceException", export.shard(1).errorClass());
        assertTrue(SynthCovFixtures.message(export.shard(1)).endsWith(": text:no-native-source"), export.shard(1).errorMessage());
        assertEquals(SynthCovFixtures.idsOnShard(expected, 0), export.rows().keySet().stream().sorted().toList());
        for (var entry : export.rows().entrySet()) {
            OrderedJsonAssert.assertOrderedEquals(expected.path("sources").path(entry.getKey()),
                JSON.readTree(entry.getValue().source()), entry.getKey());
        }
    }

    @Test
    void theKillSwitchRefusesBeforeDownloadAsIn110(@TempDir Path tmp) throws IOException {
        System.setProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, "false");
        assertThrows(ExportPreflight.SourceDisabledException.class,
            () -> SynthCovFixtures.export("cov_srcmode_native", tmp.resolve("table"), new Configuration()));
        assertFalse(Files.exists(tmp.resolve("table/_delta_log")));
    }
}
