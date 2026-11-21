package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.syntheticsource.SyntheticSourceReconstructor;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Documents written after ES 8+ accepted {@code _source.enabled: false} on an existing index,
 * against Elasticsearch's own output: stored rows stay byte-exact ({@code stored}),
 * documents without stored {@code _source} are reconstructed ({@code synthetic})
 * and equal the native-synthetic twin's rendering with the values only the disabled source held
 * removed and the constant_keyword value added; a shard whose fields cannot be reconstructed
 * quarantines naming type tokens and leaves no row; documents listing a field in {@code _ignored}
 * are counted, once each, and noted content-free. (The existing SourceDisabledExportTest covers
 * an ES 7 index without source, which still fails before download.)
 */
class SourceDisabledLaterExportTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PREFIX = "documents without stored _source cannot be reconstructed from this index's doc values and stored fields: ";
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
    @ValueSource(strings = {"cov_srcoff_native", "cov_srcoff_merged", "cov8_srcoff_native"})
    void storedRowsStayByteExactAndTheRestIsReconstructed(String index, @TempDir Path tmp) throws Exception {
        JsonNode expected = SynthCovFixtures.expected(index);
        var export = SynthCovFixtures.export(index, tmp.resolve("table"), new Configuration());
        assertEquals(2, export.report().committedCount(), export.report().shards().toString());
        assertRows(expected, export, Set.of());
        assertTrue(expected.path("ignored").size() > 0, "the fixture has counted documents");
    }

    @Test
    void aTextFieldWithoutNativeSourceQuarantinesItsShardOnly(@TempDir Path tmp) throws Exception {
        assertQuarantined("cov_srcoff_text", tmp, "keyword:fallback text:no-native-source");
    }

    @Test
    void mappingFeaturesThatLeaveNoTraceRefuseByName(@TempDir Path tmp) throws Exception {
        assertQuarantined("cov_srcoff_mapping", tmp,
            "binary:not-indexed dynamic:false flattened:ignore_above object:enabled-false runtime:shadowing");
    }

    @Test
    void copyToSourcesAndTargetsRefuse(@TempDir Path tmp) throws Exception {
        assertQuarantined("cov_srcoff_copyto", tmp, "keyword:copy_to text:no-native-source");
    }

    private static void assertQuarantined(String index, Path tmp, String tokens) throws Exception {
        JsonNode expected = SynthCovFixtures.expected(index);
        var export = SynthCovFixtures.export(index, tmp.resolve("table"), new Configuration());
        assertEquals(RunReport.Outcome.COMMITTED, export.shard(0).outcome(), export.report().shards().toString());
        var refused = export.shard(1);
        assertEquals(RunReport.Outcome.QUARANTINED, refused.outcome());
        assertEquals("UnreconstructableSourceException", refused.errorClass());
        assertEquals(PREFIX + tokens, SynthCovFixtures.message(refused));
        assertRows(expected, export, Set.of(1));
        for (String id : SynthCovFixtures.idsOnShard(expected, 1)) {
            assertFalse(export.rows().containsKey(id), "a quarantined shard leaves no row: " + id);
        }
    }

    /** Every document of a committed shard is a row: stored ones byte-exact, reconstructed ones equal to the oracle. */
    private static void assertRows(JsonNode expected, SynthCovFixtures.Export export, Set<Integer> quarantined) throws Exception {
        Set<String> reconstructed = new HashSet<>();
        expected.path("reconstructed").forEach(id -> reconstructed.add(id.asText()));
        int rows = 0;
        for (var entry : expected.path("shards").properties()) {
            String id = entry.getKey();
            if (quarantined.contains(entry.getValue().asInt())) continue;
            var row = export.rows().get(id);
            assertTrue(row != null, "missing row " + id);
            rows++;
            if (reconstructed.contains(id)) {
                assertEquals("synthetic", row.kind(), id);
                OrderedJsonAssert.assertOrderedEquals(expected.path("sources").path(id), JSON.readTree(row.source()), id);
            } else {
                assertEquals("stored", row.kind(), id);
                assertEquals(expected.path("input").path(id).asText(), row.source(), id + " byte-exact");
            }
        }
        assertEquals(rows, export.rows().size());
    }

    /** The count is distinct reconstructed documents whose _ignored lists a field; the note names only the count. */
    @ParameterizedTest
    @ValueSource(strings = {"cov_srcoff_native", "cov_srcoff_merged", "cov8_srcoff_native"})
    void documentsListingAFieldInIgnoredAreCountedOnceAndNoted(String index, @TempDir Path tmp) throws Exception {
        JsonNode expected = SynthCovFixtures.expected(index);
        Set<String> ignored = new HashSet<>();
        expected.path("ignored").forEach(id -> ignored.add(id.asText()));
        for (int shard = 0; shard < 2; shard++) {
            long count = SynthCovFixtures.idsOnShard(expected, shard).stream().filter(ignored::contains).count();
            var read = SynthCovFixtures.readShard(index, shard, tmp);
            assertNull(read.failure(), index + "[" + shard + "]");
            assertEquals(count == 0 ? null : count, read.ignoredFieldDocuments(), index + "[" + shard + "]");
            String note = SyntheticSourceFallbackShardReader.ignoredFieldNote(index, count);
            if (count == 0) {
                assertFalse(read.stderr().contains("_ignored"), read.stderr());
            } else {
                assertEquals(List.of(note), read.stderr().lines().filter(l -> l.contains("_ignored")).toList());
                assertEquals("note: " + count + " document(s) of index " + index + " list fields in _ignored; values "
                    + "Elasticsearch ignored (and whole values of text fields whose keyword sub-field ignored them) were kept "
                    + "only in the disabled _source and are missing from their reconstructed rows", note);
            }
        }
    }

    /**
     * Forks share the root's count (a document reconstructed by a range fork is counted once, on the
     * root); only the root reports it, and only its close prints the note, unless deferred.
     */
    @Test
    void forksShareTheCountAndOnlyAnUndeferredRootPrintsTheNote(@TempDir Path tmp) throws Exception {
        String index = "cov_srcoff_native";
        JsonNode expected = SynthCovFixtures.expected(index);
        Set<String> ignored = new HashSet<>();
        expected.path("ignored").forEach(id -> ignored.add(id.asText()));
        long count = SynthCovFixtures.idsOnShard(expected, 1).stream().filter(ignored::contains).count();
        var store = new de.sloow.gefrierschrank.core.repo.FileSystemBlobStore(SynthCovFixtures.repository(index));
        var inspector = new de.sloow.gefrierschrank.core.repo.RepositoryInspector(store);
        Path shardDir = tmp.resolve("shard");
        new de.sloow.gefrierschrank.core.repo.ShardMaterializer(store).materialize(inspector.shardSnapshot(index, 1), shardDir);
        var previous = System.err;
        var captured = new java.io.ByteArrayOutputStream();
        System.setErr(new java.io.PrintStream(captured, true, java.nio.charset.StandardCharsets.UTF_8));
        try (var root = SyntheticSourceFallbackShardReader.wrap(new de.sloow.gefrierschrank.lucene10.Lucene10ShardReader(shardDir),
                 shardDir, index, inspector.indexMetadata(index))) {
            try (var fork = root.fork()) {
                for (int doc = 0; doc < fork.docCount(); doc++) {
                    if (fork.isLive(doc)) fork.readSource(doc, (bytes, length) -> {});
                }
                assertNull(SyntheticSourceFallbackShardReader.ignoredFieldDocuments(fork), "a fork never reports");
                SyntheticSourceFallbackShardReader.deferIgnoredFieldNote(fork);
            }
            for (int doc = 0; doc < root.docCount(); doc++) {
                if (root.isLive(doc)) root.readSource(doc, (bytes, length) -> {});
            }
            assertEquals(count, SyntheticSourceFallbackShardReader.ignoredFieldDocuments(root), "read twice, counted once");
            SyntheticSourceFallbackShardReader.deferIgnoredFieldNote(root);
        } finally {
            System.setErr(previous);
        }
        assertFalse(captured.toString(java.nio.charset.StandardCharsets.UTF_8).contains("_ignored"),
            "neither the fork nor the deferred root prints the note");
        assertNull(SyntheticSourceFallbackShardReader.ignoredFieldDocuments(new NotAWrapper()));
        assertNull(SyntheticSourceFallbackShardReader.unrecoverableUnmappedValues(new NotAWrapper()));
    }

    @Test
    void theKillSwitchRestoresSourceNotSyntheticException(@TempDir Path tmp) throws Exception {
        System.setProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, "false");
        var export = SynthCovFixtures.export("cov_srcoff_native", tmp.resolve("table"), new Configuration());
        assertEquals(RunReport.Outcome.COMMITTED, export.shard(0).outcome());
        assertEquals(RunReport.Outcome.QUARANTINED, export.shard(1).outcome());
        assertEquals("SourceNotSyntheticException", export.shard(1).errorClass());
    }

    /** A reader that is not the fallback wrapper. */
    private static final class NotAWrapper implements de.sloow.gefrierschrank.core.lucene.ShardReader {
        public String runtimeName() { return "none"; }
        public int docCount() { return 0; }
        public void readSource(int docId, SourceConsumer consumer) { throw new UnsupportedOperationException(); }
        public void close() {}
    }
}
