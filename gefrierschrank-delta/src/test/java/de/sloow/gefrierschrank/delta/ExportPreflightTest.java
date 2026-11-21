package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.syntheticsource.SourceVectors;
import de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.IndicesModule;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The index-level preflight predicates. A false positive would refuse an exportable index, so
 * every "passes" row matters as much as every "fails" row.
 */
class ExportPreflightTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final SourceVectorsPlan.Prober NEVER = (index, metadata) -> fail("the isolated probe must not run");

    /** A repository whose catalog holds one index in an ES 7.17.9 and an ES 8.17 ("8.11.0") snapshot. */
    private static final RepositorySources.Source SOURCE = new RepositorySources.Source("primary", new MemoryStore(Map.of(
        "index.latest", new byte[] {0, 0, 0, 0, 0, 0, 0, 0},
        "index-0", """
            {"snapshots":[
              {"name":"es7","uuid":"u7","state":1,"version":"7.17.9","start_time_millis":1},
              {"name":"es8","uuid":"u8","state":1,"version":"8.11.0","index_version":8521000,"start_time_millis":2},
              {"name":"legacy","uuid":"u0","state":1}],
             "indices":{"idx":{"id":"idx-id","snapshots":["u7","u8","u0"],"shard_generations":["g"]}}}
            """.getBytes(StandardCharsets.UTF_8))));

    private static final String ES7 = "u7";
    private static final String ES8 = "u8";
    private static final String NO_VERSION = "u0";

    @Test
    void sourceEnabledFalseFailsOnlyOnSnapshotsWrittenByEs7OrOlder() throws Exception {
        JsonNode disabled = metadata("{}", "[{\"_doc\":{\"_source\":{\"enabled\":false},\"properties\":{\"name\":{\"type\":\"keyword\"}}}}]");

        assertThrows(ExportPreflight.SourceDisabledException.class, () -> ExportPreflight.require("idx", resolved(disabled, ES7)));
        // ES 8.17 and 9.3 accept the true->false flip: older segments may still hold _source.
        assertDoesNotThrow(() -> ExportPreflight.require("idx", resolved(disabled, ES8)));
        assertDoesNotThrow(() -> ExportPreflight.require("idx", resolved(disabled, NO_VERSION)));
    }

    @Test
    void theMappingIsReadInTheArrayDocAndFlatForms() throws Exception {
        for (String mappings : List.of(
                "[{\"_doc\":{\"_source\":{\"enabled\":false}}}]",
                "{\"_doc\":{\"_source\":{\"enabled\":false}}}",
                "[{\"_source\":{\"enabled\":false}}]",
                "{\"_source\":{\"enabled\":false}}",
                "{\"_source\":{\"enabled\":\"false\"}}")) {
            assertThrows(ExportPreflight.SourceDisabledException.class,
                () -> ExportPreflight.require("idx", resolved(metadata("{}", mappings), ES7)), mappings);
        }
    }

    /**
     * The Final setting {@code index.mapping.source.mode: disabled} fails before download where no
     * segment can be reconstructed: on an ES 7 snapshot, on an index created before 8.0, or with the
     * reconstruction kill switch off. On ES 8+ it is DISABLED_FROM_CREATION, whose mapping-level
     * check needs the isolated world (SourceModeDisabledPreflightTest, child JVM).
     */
    @Test
    void theFinalSourceModeSettingFailsBeforeDownloadOnlyWhereNothingCanBeReconstructed() throws Exception {
        for (String settings : List.of("{\"index.mapping.source.mode\":\"disabled\"}",
                "{\"index\":{\"mapping\":{\"source\":{\"mode\":\"DISABLED\"}}}}")) {
            assertThrows(ExportPreflight.SourceDisabledException.class,
                () -> ExportPreflight.require("idx", resolved(metadata(settings, "[{\"_doc\":{}}]"), ES7)));
            JsonNode before8 = metadata("{\"index.mapping.source.mode\":\"disabled\",\"index.version.created\":\"7170999\"}",
                "[{\"_doc\":{}}]");
            for (String snapshot : List.of(ES8, NO_VERSION)) {
                assertThrows(ExportPreflight.SourceDisabledException.class, () -> ExportPreflight.require("idx", resolved(before8, snapshot)));
                assertEquals(ExportPreflight.SourceRetention.DISABLED_FROM_CREATION,
                    ExportPreflight.SourceRetention.of(metadata(settings, "[{\"_doc\":{}}]"), snapshot.equals(ES8) ? 8 : -1));
            }
            String previous = System.getProperty(de.sloow.gefrierschrank.syntheticsource.SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY);
            System.setProperty(de.sloow.gefrierschrank.syntheticsource.SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, "false");
            try {
                for (String snapshot : List.of(ES8, NO_VERSION)) {
                    assertThrows(ExportPreflight.SourceDisabledException.class,
                        () -> ExportPreflight.require("idx", resolved(metadata(settings, "[{\"_doc\":{}}]"), snapshot)));
                }
            } finally {
                if (previous == null) System.clearProperty(de.sloow.gefrierschrank.syntheticsource.SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY);
                else System.setProperty(de.sloow.gefrierschrank.syntheticsource.SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, previous);
            }
        }
    }

    @Test
    void retainedPrunedAndSyntheticSourcesPass() throws Exception {
        for (String[] row : new String[][] {
                {"{}", "[{\"_doc\":{\"_source\":{\"includes\":[\"a\"],\"excludes\":[\"b\"]}}}]"},
                {"{}", "[{\"_doc\":{\"_source\":{\"enabled\":true}}}]"},
                {"{\"index.mapping.source.mode\":\"synthetic\"}", "[{\"_doc\":{}}]"},
                {"{\"index.mapping.source.mode\":\"stored\"}", "[{\"_doc\":{}}]"},
                {"{\"index.mode\":\"logsdb\"}", "[{\"_doc\":{}}]"},
                {"{\"index\":{\"mode\":\"time_series\"}}", "[{\"_doc\":{}}]"},
                {"{}", "[{\"_doc\":{\"_source\":{\"mode\":\"synthetic\"}}}]"},
                {"{}", "[{\"_doc\":{\"_source\":{\"mode\":\"disabled\"}}}]"},
                {"{}", "[]"},
                {"{}", "\"not a mapping\""}}) {
            for (String snapshot : List.of(ES7, ES8)) {
                assertDoesNotThrow(() -> ExportPreflight.require("idx", resolved(metadata(row[0], row[1]), snapshot)),
                    String.join(" ", row) + " @" + snapshot);
            }
        }
    }

    @Test
    void indicesCreatedByEs6OrOlderFailFirst() throws Exception {
        for (String created : List.of("6080099", "5061299", "2040699")) {
            JsonNode old = metadata("{\"index.version.created\":\"" + created + "\"}", "[{\"_doc\":{\"_source\":{\"enabled\":false}}}]");
            assertThrows(ExportPreflight.IndexCreatedTooOldException.class, () -> ExportPreflight.require("idx", resolved(old, ES7)));
        }
        for (String created : List.of("7000001", "7000099", "7170099", "8521000", "9060000", "", "abc", "-1")) {
            JsonNode current = metadata("{\"index\":{\"version\":{\"created\":\"" + created + "\"}}}", "[{\"_doc\":{}}]");
            assertDoesNotThrow(() -> ExportPreflight.require("idx", resolved(current, ES7)), created);
        }
        assertDoesNotThrow(() -> ExportPreflight.require("idx", resolved(metadata("{}", "[{\"_doc\":{}}]"), ES7)));
    }

    @Test
    void rehydratedVectorsPassAndOnlyUnrehydratableOnesFailAfterTheOtherChecks() throws Exception {
        JsonNode vectors = metadata("{\"index.version.created\":\"9060000\",\"index.number_of_shards\":\"1\","
                + "\"index.number_of_replicas\":\"0\"}",
            "[{\"_doc\":{\"properties\":{\"embedding\":{\"type\":\"dense_vector\",\"dims\":8,\"index\":true}}}}]");
        assertDoesNotThrow(() -> ExportPreflight.require("idx", resolved(vectors, ES8)));
        JsonNode rankVectors = metadata("{\"index.version.created\":\"9060000\"}",
            "[{\"_doc\":{\"properties\":{\"v\":{\"type\":\"rank_vectors\"}}}}]");
        assertThrows(SourceVectorsPlan.ExcludedSourceVectorsException.class,
            () -> ExportPreflight.require("idx", resolved(rankVectors, ES8)));
        String previous = System.getProperty(SourceVectorsPlan.REHYDRATE_PROPERTY);
        System.setProperty(SourceVectorsPlan.REHYDRATE_PROPERTY, "false");
        try {
            assertThrows(SourceVectorsPlan.ExcludedSourceVectorsException.class,
                () -> ExportPreflight.require("idx", resolved(vectors, ES8)));
        } finally {
            if (previous == null) System.clearProperty(SourceVectorsPlan.REHYDRATE_PROPERTY);
            else System.setProperty(SourceVectorsPlan.REHYDRATE_PROPERTY, previous);
        }
    }

    @Test
    void failuresCarryFixedMessagesOnly() {
        for (IOException failure : List.of(new ExportPreflight.SourceDisabledException(),
                new ExportPreflight.IndexCreatedTooOldException())) {
            assertFalse(failure.getMessage().contains("synthetic source"), failure.getMessage());
        }
    }

    @Test
    void classifyMirrorsTheExportOutcomes() throws Exception {
        String keyword = "\"name\":{\"type\":\"keyword\"}";
        assertVerdict(ExportPreflight.Kind.EXPORTABLE, null, "{}", "[{\"_doc\":{\"properties\":{" + keyword + "}}}]", ES8);
        assertVerdict(ExportPreflight.Kind.EXPORTABLE, null, "{}",
            "[{\"_doc\":{\"properties\":{\"items\":{\"type\":\"nested\",\"properties\":{" + keyword + "}}}}}]", ES8);
        assertVerdict(ExportPreflight.Kind.NOT_EXPORTABLE, "too_old", "{\"index.version.created\":\"6080099\"}", "[{\"_doc\":{}}]", ES7);
        assertVerdict(ExportPreflight.Kind.NOT_EXPORTABLE, "source_disabled", "{}",
            "[{\"_doc\":{\"_source\":{\"enabled\":false}}}]", ES7);
        assertVerdict(ExportPreflight.Kind.WARN_SOURCE_DISABLED_ES8, null, "{}",
            "[{\"_doc\":{\"_source\":{\"enabled\":false}}}]", ES8);
        assertVerdict(ExportPreflight.Kind.WARN_SOURCE_DISABLED_ES8, null, "{}",
            "[{\"_doc\":{\"_source\":{\"mode\":\"disabled\"}}}]", ES8);

        // The mapping-level _source.mode sits inside the real array form; 1.0's survey missed it.
        assertVerdict(ExportPreflight.Kind.EXPORTABLE_SYNTHETIC, null, "{}",
            "[{\"_doc\":{\"_source\":{\"mode\":\"synthetic\"},\"properties\":{" + keyword + "}}}]", ES8);
        assertVerdict(ExportPreflight.Kind.EXPORTABLE_SYNTHETIC, null, "{\"index.mode\":\"logsdb\"}", "[{\"_doc\":{}}]", ES8);
        assertVerdict(ExportPreflight.Kind.EXPORTABLE_SYNTHETIC, null, "{\"index\":{\"mode\":\"time_series\"}}", "[{\"_doc\":{}}]", ES8);
        assertVerdict(ExportPreflight.Kind.EXPORTABLE, null, "{\"index.mode\":\"logsdb\",\"index.mapping.source.mode\":\"stored\"}",
            "[{\"_doc\":{}}]", ES8);
        assertVerdict(ExportPreflight.Kind.EXPORTABLE, null, "{\"index.mode\":\"logsdb\"}",
            "[{\"_doc\":{\"_source\":{\"mode\":\"stored\"}}}]", ES8);
        assertVerdict(ExportPreflight.Kind.EXPORTABLE_SYNTHETIC, null,
            "{\"index.mapping.source.mode\":\"synthetic\"}", "[{\"_doc\":{\"_source\":{\"mode\":\"stored\"}}}]", ES8);

        assertVerdict(ExportPreflight.Kind.EXPORTABLE_SYNTHETIC, null, "{\"index.mode\":\"logsdb\"}",
            "[{\"_doc\":{\"properties\":{\"items\":{\"type\":\"nested\"}}}}]", ES8);
        assertVerdict(ExportPreflight.Kind.NOT_EXPORTABLE, "semantic_text_synthetic",
            "{\"index.mapping.source.mode\":\"synthetic\",\"index.version.created\":\"8521000\"}",
            "[{\"_doc\":{\"properties\":{\"t\":{\"type\":\"text\",\"fields\":{\"s\":{\"type\":\"semantic_text\"}}}}}}]", ES8);
        assertVerdict(ExportPreflight.Kind.NOT_EXPORTABLE, "unsupported_synthetic_type", "{\"index.mode\":\"logsdb\"}",
            "[{\"_doc\":{\"properties\":{\"a\":{\"properties\":{\"b\":{\"type\":\"annotated_text\"}}}}}}]", ES8);
        assertVerdict(ExportPreflight.Kind.EXPORTABLE, null, "{}",
            "[{\"_doc\":{\"properties\":{\"a\":{\"type\":\"annotated_text\"}}}}]", ES8);
        assertVerdict(ExportPreflight.Kind.UNKNOWN, "metadata_unreadable", null, null, ES8);
    }

    @Test
    void classifyUsesTheVectorProbeOnlyForCandidatesAndIsUnknownWhenItFails() throws Exception {
        JsonNode vectors = metadata("{\"index.version.created\":\"9060000\"}",
            "[{\"_doc\":{\"properties\":{\"embedding\":{\"type\":\"dense_vector\",\"dims\":8,\"index\":true}}}}]");
        assertEquals(new ExportPreflight.Verdict(ExportPreflight.Kind.EXPORTABLE, null,
                "1 vector field(s) rehydrated from the vector files (index.mapping.exclude_source_vectors)"),
            ExportPreflight.classify("idx", resolved(vectors, ES8),
                (index, metadata) -> new SourceVectors.Result(SourceVectors.Verdict.EXCLUDED, Set.of("embedding"))));
        assertEquals(new ExportPreflight.Verdict(ExportPreflight.Kind.NOT_EXPORTABLE, "excluded_vectors",
                "rank_vectors or semantic_text kept out of _source, or the exclusion could not be evaluated"),
            ExportPreflight.classify("idx", resolved(vectors, ES8),
                (index, metadata) -> new SourceVectors.Result(SourceVectors.Verdict.UNSUPPORTED, Set.of())));
        assertEquals(ExportPreflight.Kind.EXPORTABLE, ExportPreflight.classify("idx", resolved(vectors, ES8),
            (index, metadata) -> new SourceVectors.Result(SourceVectors.Verdict.NONE, Set.of())).kind());
        var failed = ExportPreflight.classify("idx", resolved(vectors, ES8), (index, metadata) -> {
            throw new IOException("isolated world unavailable");
        });
        assertEquals(new ExportPreflight.Verdict(ExportPreflight.Kind.UNKNOWN, "probe_failed", "vector probe failed"), failed);
        var linkage = ExportPreflight.classify("idx", resolved(vectors, ES8), (index, metadata) -> {
            throw new NoClassDefFoundError("org/elasticsearch/Missing");
        });
        assertEquals(ExportPreflight.Kind.UNKNOWN, linkage.kind());
    }

    /**
     * Rehydration puts vectors back into stored _source only: an excluding index whose _source was
     * disabled after creation exports like any such index, so survey must not promise more.
     */
    @Test
    void rehydratingIndicesWithSourceDisabledAfterCreationKeepTheSourceDisabledVerdict() throws Exception {
        SourceVectorsPlan.Prober excluded = (index, metadata) -> new SourceVectors.Result(SourceVectors.Verdict.EXCLUDED, Set.of("embedding"));
        String vector = "\"properties\":{\"embedding\":{\"type\":\"dense_vector\",\"dims\":3,\"similarity\":\"cosine\"}}";
        for (String source : List.of("{\"enabled\":false}", "{\"mode\":\"disabled\"}")) {
            JsonNode metadata = metadata("{\"index.version.created\":\"9060000\"}",
                "[{\"_doc\":{\"_source\":" + source + "," + vector + "}}]");
            assertEquals(ExportPreflight.SourceRetention.DISABLED_SINCE_UNKNOWN, ExportPreflight.SourceRetention.of(metadata, 9));
            var verdict = ExportPreflight.classify("idx", metadata, 9, excluded);
            assertEquals(ExportPreflight.Kind.WARN_SOURCE_DISABLED_ES8, verdict.kind(), source + " -> " + verdict);
            assertEquals(verdict, ExportPreflight.classify("idx", resolved(metadata, ES8), excluded), source);
        }
    }

    /** The registry reconstruction builds its MapperService from (IsolatedReconstructionWorkerImpl). */
    @Test
    void syntheticMapperTypesEqualTheReconstructionRegistry() {
        var registered = new TreeSet<>(IndicesModule.getMappers(
            List.of(new MapperExtrasPlugin(), new GefrierschrankXPackMapperPlugin())).keySet());
        assertEquals(registered, new TreeSet<>(ExportPreflight.SYNTHETIC_MAPPER_TYPES));
    }

    /** field-types' SYNTHETIC column says what survey would say about a synthetic index using the type. */
    @Test
    void syntheticExportableAgreesWithClassifyForEveryType() throws IOException {
        var types = new TreeSet<>(ExportPreflight.SYNTHETIC_MAPPER_TYPES);
        types.addAll(List.of("semantic_text", "annotated_text"));
        for (String type : types) {
            JsonNode metadata = metadata("{\"index.mode\":\"logsdb\"}",
                "[{\"_doc\":{\"properties\":{\"f\":{\"type\":\"" + type + "\"}}}}]");
            var verdict = ExportPreflight.classify("idx", resolved(metadata, ES8),
                (index, meta) -> new SourceVectors.Result(SourceVectors.Verdict.NONE, Set.of()));
            assertEquals(ExportPreflight.syntheticExportable(type), verdict.kind() == ExportPreflight.Kind.EXPORTABLE_SYNTHETIC,
                type + " -> " + verdict);
        }
        org.junit.jupiter.api.Assertions.assertTrue(ExportPreflight.syntheticExportable("nested"));
    }

    private static void assertVerdict(ExportPreflight.Kind kind, String reason, String settings, String mappings,
                                      String snapshot) throws IOException {
        JsonNode metadata = settings == null ? JSON.getNodeFactory().textNode("x") : metadata(settings, mappings);
        var verdict = ExportPreflight.classify("idx", resolved(metadata, snapshot), NEVER);
        assertEquals(kind, verdict.kind(), settings + " " + mappings + " -> " + verdict);
        assertEquals(reason, verdict.reason(), settings + " " + mappings + " -> " + verdict);
    }

    private static JsonNode metadata(String settings, String mappings) throws IOException {
        var metadata = JSON.createObjectNode();
        metadata.set("settings", JSON.readTree(settings));
        metadata.set("mappings", JSON.readTree(mappings));
        return metadata;
    }

    private static RepositorySources.Resolved resolved(JsonNode metadata, String snapshotUuid) {
        return new RepositorySources.Resolved(SOURCE, "idx", snapshotUuid, metadata, List.of("primary/" + snapshotUuid + "/idx"));
    }

    private record MemoryStore(Map<String, byte[]> blobs) implements BlobStore {
        @Override
        public Optional<BlobMetadata> stat(String key) {
            return Optional.ofNullable(blobs.get(key)).map(b -> new BlobMetadata(key, b.length));
        }

        @Override
        public InputStream read(String key) throws IOException {
            byte[] blob = blobs.get(key);
            if (blob == null) throw new NoSuchFileException(key);
            return new ByteArrayInputStream(blob);
        }

        @Override
        public List<String> list(String prefix) {
            return blobs.keySet().stream().filter(k -> k.startsWith(prefix)).sorted().toList();
        }

        @Override
        public List<BlobMetadata> listWithSizes(String prefix) {
            return list(prefix).stream().map(k -> new BlobMetadata(k, blobs.get(k).length)).toList();
        }

        @Override
        public void put(String key, byte[] content) {
            throw new UnsupportedOperationException();
        }
    }
}
