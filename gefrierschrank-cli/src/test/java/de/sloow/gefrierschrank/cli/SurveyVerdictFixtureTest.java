package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Survey must say what dump-to-delta does. The expected verdicts were checked against real
 * {@code dump-to-delta --all-indices} runs over each fixture repository
 * (outcome and row kinds in the comments). The two indices refused by ExportPreflight now FAIL
 * before download instead of downloading and quarantining.
 */
class SurveyVerdictFixtureTest {

    private static final Path CORE = SurveyCommandTest.CORE_FIXTURES;
    private static final Path SYNTHETIC = SurveyCommandTest.SYNTHETIC_FIXTURES;

    private static final Map<Path, Map<String, String>> EXPECTED = new LinkedHashMap<>();

    static {
        EXPECTED.put(CORE.resolve("es7-features-repo"), ordered(
            "es7_completion", "exportable",                         // COMPLETED, 12 stored rows
            "es7_hard_deletes", "exportable",                       // COMPLETED, 27 stored rows
            "es7_features", "exportable",                           // COMPLETED, 61 stored rows (nested under stored source)
            "es7_no_source", "not_exportable(source_disabled)"));   // was QUARANTINED IndexFormatTooOldException; now FAILED SourceDisabledException
        EXPECTED.put(CORE.resolve("frozen-repo"), ordered(
            "frozen-docs-f5ffca09", "exportable",                   // COMPLETED, 3 stored rows
            "partial-frozen-docs-f5ffca09", "mounted(stored)"));    // COMPLETED from backing, 2 stored rows
        EXPECTED.put(SYNTHETIC.resolve("es93-repo"), ordered(
            "test_es93_tsdb_hist", "exportable_synthetic",          // COMPLETED, 400 synthetic rows
            "test_es93_logsdb_binary", "exportable_synthetic",      // COMPLETED, 1300 synthetic rows
            "test_es93_vectors_default", "exportable",              // COMPLETED, 20 stored+vectors rows
            "test_es93_vectors_included", "exportable"));           // COMPLETED, 20 stored rows
        EXPECTED.put(SYNTHETIC.resolve("synth-repo"), ordered(
            "test_plain_synth", "exportable_synthetic",             // COMPLETED, 50 synthetic rows
            "test_tsdb_synth", "exportable_synthetic"));            // COMPLETED, 60 synthetic rows
        EXPECTED.put(SYNTHETIC.resolve("synth-deletes-repo"), ordered(
            "test_deletes_zstd", "exportable_synthetic",            // COMPLETED, 108 synthetic rows
            "test_deletes_nested", "exportable_synthetic",          // COMPLETED, 54 synthetic rows
            "test_deletes_lz4", "exportable_synthetic"));           // COMPLETED, 108 synthetic rows
        EXPECTED.put(SYNTHETIC.resolve("zstd-synth-repo"), ordered(
            "test_zstd_synth", "exportable_synthetic",              // COMPLETED, 40 synthetic rows
            "test_zstd_logsdb", "exportable_synthetic"));           // COMPLETED, 20 synthetic rows
    }

    @Test
    void everyFixtureIndexGetsTheVerdictOfItsDumpToDeltaOutcome() {
        EXPECTED.forEach((repository, expected) -> {
            var result = SurveyCommandTest.run(new FileSystemBlobStore(repository), 16, "--csv");
            assertEquals(0, result.exitCode(), result.err());
            assertEquals(expected, SurveyCommandTest.verdicts(result.out()), repository.toString());
            for (String line : result.out().lines().skip(1).toList()) {
                assertTrue(Long.parseLong(line.split(",", -1)[4]) > 0, "sized from shard metadata: " + line);
                // Elasticsearch's own mapper code (the isolated probe) named the excluded field.
                if (line.startsWith("test_es93_vectors_default,"))
                    assertTrue(line.endsWith(",1 vector field(s) rehydrated from the vector files (index.mapping.exclude_source_vectors)"), line);
            }
        });
    }

    @Test
    void outputIsIdenticalWithOneAndSixteenFetchThreads() {
        for (Path repository : EXPECTED.keySet()) {
            for (String[] options : List.of(new String[] {"--csv"}, new String[0], new String[] {"--no-sizes"})) {
                var serial = SurveyCommandTest.run(new FileSystemBlobStore(repository), 1, options);
                var concurrent = SurveyCommandTest.run(new FileSystemBlobStore(repository), 16, options);
                assertEquals(serial.out(), concurrent.out(), repository + " " + String.join(" ", options));
            }
        }
    }

    @Test
    void anUnreadableMetadataBlobIsUnknownAndLeavesTheOtherIndicesAlone(@TempDir Path repo) throws IOException {
        SurveyCommandTest.copy(SYNTHETIC.resolve("zstd-synth-repo"), repo);
        try (var metaBlobs = Files.walk(repo.resolve("indices/r5kN-uVUS3CMOFJQ6jJfmw"), 1)) {
            for (Path meta : metaBlobs.filter(p -> p.getFileName().toString().startsWith("meta-")).toList()) Files.delete(meta);
        }

        var result = SurveyCommandTest.run(new FileSystemBlobStore(repo), 16, "--csv");

        assertEquals(0, result.exitCode(), result.err());
        assertEquals(ordered("test_zstd_synth", "exportable_synthetic", "test_zstd_logsdb", "unknown(metadata_unreadable)"),
            SurveyCommandTest.verdicts(result.out()));
        assertTrue(result.out().contains("NoSuchFileException"), result.out());
    }

    @Test
    void aFailingVectorProbeIsUnknownNeverExportable() {
        var command = new SurveyCommand() {
            @Override
            BlobStore openStore() {
                return new FileSystemBlobStore(SYNTHETIC.resolve("es93-repo"));
            }
        };
        command.vectorProbe = (index, metadata) -> {
            throw new IOException("isolated world unavailable");
        };
        var result = SurveyCommandTest.capture(command, "--csv");

        assertEquals(0, result.exitCode(), result.err());
        var verdicts = SurveyCommandTest.verdicts(result.out());
        assertEquals("unknown(probe_failed)", verdicts.get("test_es93_vectors_default"));
        // exclude_source_vectors: false settles it without the probe.
        assertEquals("exportable", verdicts.get("test_es93_vectors_included"));
    }

    private static Map<String, String> ordered(String... pairs) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) map.put(pairs[i], pairs[i + 1]);
        return map;
    }
}
