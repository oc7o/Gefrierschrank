package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves the synthetic-source fallback actually flows through the real production pipeline
 * (DumpToDeltaPipeline), not just the standalone gefrierschrank-synthetic-source module in
 * isolation - and guards against a regression: without metadata, the pipeline's
 * behavior for a synthetic-source shard is completely unchanged (still a hard, clear failure).
 *
 * <p>Uses the real ES 9.3.0 fixture repo from gefrierschrank-synthetic-source's own test
 * resources (real _bulk-indexed, real _snapshot output, not hand-built segments) - it has both a
 * synthetic-source index and ordinary stored-_source indices in one repository, exactly the mix
 * a dump-to-delta run needs to distinguish correctly.
 */
class SyntheticSourceFallbackIntegrationTest {

    private static final Path FIXTURE_REPO =
        Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures/synth-repo");
    private static final Path DELETES_REPO =
        Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures/synth-deletes-repo");
    private static final Path DELETES_EXPECTED =
        Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures/synth-deletes-expected.json");
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void aSyntheticSourceIndexIsReconstructedEndToEndThroughTheRealPipeline(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        String indexName = "test_plain_synth";

        DumpToDeltaPipeline.ShardReaderOpener opener =
            (shard, shardDir) -> new Lucene10ShardReader(shardDir);
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        RunReport report = pipeline.run(indexName);

        assertEquals(1, report.committedCount(), "the synthetic-source shard commits, not quarantined: " + report.shards());
        assertEquals(0, report.quarantinedCount());
        assertEquals(50, report.rowsWrittenThisRun());

        assertReconstructedFieldsPresent(tableUri, "name", "id");
    }

    @Test
    void theRealTsdbSyntheticSourceIndexCommitsThroughTheRealPipeline(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        String indexName = "test_tsdb_synth";

        DumpToDeltaPipeline.ShardReaderOpener opener =
            (shard, shardDir) -> new Lucene10ShardReader(shardDir);
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        RunReport report = pipeline.run(indexName);

        assertEquals(1, report.committedCount(),
            "the real ES819TSDB-codec shard commits via reconstruction, not quarantined: " + report.shards());
        assertEquals(0, report.quarantinedCount());
        assertEquals(60, report.rowsWrittenThisRun());
    }

    /**
     * Regression guard for the fallback: an ordinary stored-_source index is
     * completely unaffected by the fallback existing - SyntheticSourceFallbackShardReader.wrap
     * never even needs to intervene, since the fast path never throws in the first place. Uses
     * gefrierschrank-core's own demo-repo fixture (a plain ES 9.3.0 index, ordinary stored
     * _source, no synthetic source or TSDB involved at all).
     */
    @Test
    void anOrdinaryStoredSourceIndexIsUnaffectedByTheFallbackExisting(@TempDir Path tmp) throws Exception {
        String tableUri = tmp.resolve("out-table").toString();
        var store = new FileSystemBlobStore(
            Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo"));
        var inspector = new RepositoryInspector(store);
        String indexName = "test_data_9_3_0";

        DumpToDeltaPipeline.ShardReaderOpener opener =
            (shard, shardDir) -> new Lucene10ShardReader(shardDir);
        var pipeline = new DumpToDeltaPipeline(inspector, store, tableUri, new Configuration(), opener);

        RunReport report = pipeline.run(indexName);

        assertEquals(1, report.committedCount());
        assertEquals(0, report.quarantinedCount());
    }

    /**
     * The regression guard: when no metadata is available at all (the
     * DumpToDeltaPipeline.ShardCatalog-based test seam used throughout DumpToDeltaPipelineTest
     * defaults rawIndexMetadata to null), a synthetic-source shard must fail EXACTLY as it did
     * before the fallback existed - the original, clear "likely synthetic source" IOException,
     * not something new or different. Uses the real materialized synthetic-source shard directly
     * against the unwrapped Lucene10ShardReader, bypassing the pipeline/fallback entirely, to pin
     * the exact pre-existing behavior this guard protects.
     */
    @Test
    void withoutMetadataASyntheticSourceShardStillFailsWithTheOriginalClearError(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        ShardSnapshot shard = inspector.shardSnapshot("test_plain_synth", 0);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(shard, shardDir);

        try (ShardReader reader = new Lucene10ShardReader(shardDir)) {
            var holder = new Object[1];
            var e = assertThrows(java.io.IOException.class,
                () -> reader.readSource(0, (bytes, length) -> holder[0] = bytes));
            assertTrue(e.getMessage().contains("synthetic source"),
                "unwrapped reader must still throw the original clear error: " + e.getMessage());
        }
    }

    /**
     * ES 9.3 synthetic-source index (LZ4 stored fields) with four unmerged segments, 10% of the docs
     * deleted by id (soft-deleted, plus tombstones) and updated docs whose older versions stay
     * soft-deleted in their segments. Exactly ES's own live documents must come out, each once.
     * The ZSTD twin runs in the CLI module (see SyntheticArchiveGoldenTest for why).
     */
    @Test
    void deletedAndSupersededDocumentsNeverBecomeRows(@TempDir Path tmp) throws Exception {
        String index = "test_deletes_lz4";
        JsonNode expected = JSON.readTree(DELETES_EXPECTED.toFile()).path(index);
        String table = tmp.resolve("table").toString();
        RunReport report = deletesExporter().run(index, table, RunReportListener.NO_OP);
        assertEquals(1, report.committedCount(), report.shards().toString());

        Map<String, JsonNode> expectedSources = new HashMap<>();
        expected.path("sources").properties().forEach(e -> expectedSources.put(e.getKey(), e.getValue()));
        Map<String, JsonNode> actual = new HashMap<>();
        try (var rows = io.delta.standalone.DeltaLog.forTable(new Configuration(), table).snapshot().open()) {
            while (rows.hasNext()) {
                var row = rows.next();
                assertEquals("synthetic", row.getString("_source_kind"));
                assertNull(actual.put(row.getString("_id"), JSON.readTree(row.getString("_source"))),
                    "duplicate row for " + row.getString("_id"));
            }
        }
        assertEquals(expectedSources.size(), report.rowsWrittenThisRun());
        assertEquals(expectedSources, actual);
        for (JsonNode deleted : expected.path("deleted_ids")) {
            assertFalse(actual.containsKey(deleted.asText()), "deleted document exported: " + deleted.asText());
        }
        for (JsonNode updated : expected.path("updated_ids")) {
            assertEquals(expectedSources.get(updated.asText()), actual.get(updated.asText()),
                "an updated document must export its latest version only");
        }
    }

    /**
     * Nested objects under synthetic source: roots export with their nested objects exactly as
     * Elasticsearch's own GET rendered them (one object stays an object, several become an array in
     * indexed order), children never become rows, deleted and superseded roots never either.
     */
    @Test
    void nestedSyntheticSourceRootsExportWithTheirNestedObjects(@TempDir Path tmp) throws Exception {
        String index = "test_deletes_nested";
        JsonNode expected = JSON.readTree(DELETES_EXPECTED.toFile()).path(index);
        String table = tmp.resolve("table").toString();
        RunReport report = deletesExporter().run(index, table, RunReportListener.NO_OP);
        assertEquals(1, report.committedCount(), report.shards().toString());
        assertEquals(0, report.quarantinedCount());

        Map<String, JsonNode> expectedSources = new HashMap<>();
        expected.path("sources").properties().forEach(e -> expectedSources.put(e.getKey(), e.getValue()));
        Map<String, JsonNode> actual = new HashMap<>();
        try (var rows = io.delta.standalone.DeltaLog.forTable(new Configuration(), table).snapshot().open()) {
            while (rows.hasNext()) {
                var row = rows.next();
                assertEquals("synthetic", row.getString("_source_kind"));
                assertNull(actual.put(row.getString("_id"), JSON.readTree(row.getString("_source"))),
                    "duplicate row for " + row.getString("_id"));
            }
        }
        assertEquals(expectedSources.size(), report.rowsWrittenThisRun());
        OrderedJsonAssert.assertOrderedEquals(expectedSources, actual);
        for (JsonNode deleted : expected.path("deleted_ids")) {
            assertFalse(actual.containsKey(deleted.asText()), "deleted document exported: " + deleted.asText());
        }
    }

    @Test
    void segmentsWithoutSourceGoStraightToReconstruction(@TempDir Path tmp) throws Exception {
        Path shardDir = materialize("test_plain_synth", tmp);
        List<String> golden = golden("synth-repo/test_plain_synth");
        var counting = new CountingReader(new Lucene10ShardReader(shardDir), true);
        try (ShardReader reader = SyntheticSourceFallbackShardReader.wrap(counting, shardDir, "test_plain_synth",
            new RepositoryInspector(new FileSystemBlobStore(FIXTURE_REPO)).indexMetadata("test_plain_synth"))) {
            assertTrue(reader.storedSourceAbsent(0), "the wrapper forwards the delegate's answer");
            assertEquals(golden, readAll(reader));
            assertEquals(0, counting.readSourceCalls, "the delegate must not decode stored fields just to throw");
        }
    }

    @Test
    void readersThatCannotTellStillReconstructThroughTheSafetyNet(@TempDir Path tmp) throws Exception {
        Path shardDir = materialize("test_plain_synth", tmp);
        List<String> golden = golden("synth-repo/test_plain_synth");
        var counting = new CountingReader(new Lucene10ShardReader(shardDir), false);
        try (ShardReader reader = SyntheticSourceFallbackShardReader.wrap(counting, shardDir, "test_plain_synth",
            new RepositoryInspector(new FileSystemBlobStore(FIXTURE_REPO)).indexMetadata("test_plain_synth"))) {
            assertEquals(golden, readAll(reader));
            assertEquals(golden.size(), counting.readSourceCalls);
        }
    }

    @Test
    void unrelatedReadFailuresStillPropagateUnchanged(@TempDir Path tmp) throws Exception {
        Path shardDir = materialize("test_plain_synth", tmp);
        IOException corrupt = new IOException("checksum failed (hardware problem?)");
        ShardReader failing = new ShardReader() {
            public String runtimeName() { return "fake"; }
            public int docCount() { return 1; }
            public void readSource(int docId, SourceConsumer consumer) throws IOException { throw corrupt; }
            public void close() { }
        };
        ShardReader reader = SyntheticSourceFallbackShardReader.wrap(failing, shardDir, "test_plain_synth",
            new RepositoryInspector(new FileSystemBlobStore(FIXTURE_REPO)).indexMetadata("test_plain_synth"));
        assertSame(corrupt, assertThrows(IOException.class, () -> reader.readSource(0, (bytes, length) -> { })));
        assertSame(failing, SyntheticSourceFallbackShardReader.wrap(failing, shardDir, "test_plain_synth", null));
    }

    /**
     * Becomes active once Lucene10ShardReader reports segments without _source: then
     * no synthetic document of the fixture may reach the fast path's decode-and-throw at all.
     */
    @Test
    void realReaderRoutesEverySyntheticDocumentStraightToReconstruction(@TempDir Path tmp) throws Exception {
        Path shardDir = materialize("test_tsdb_synth", tmp);
        try (var real = new Lucene10ShardReader(shardDir)) {
            org.junit.jupiter.api.Assumptions.assumeTrue(real.storedSourceAbsent(0),
                "Lucene10ShardReader does not report storedSourceAbsent yet");
        }
        var counting = new CountingReader(new Lucene10ShardReader(shardDir), null);
        try (ShardReader reader = SyntheticSourceFallbackShardReader.wrap(counting, shardDir, "test_tsdb_synth",
            new RepositoryInspector(new FileSystemBlobStore(FIXTURE_REPO)).indexMetadata("test_tsdb_synth"))) {
            assertEquals(golden("synth-repo/test_tsdb_synth"), readAll(reader));
            assertEquals(0, counting.readSourceCalls);
        }
    }

    private static Path materialize(String index, Path tmp) throws IOException {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(new RepositoryInspector(store).shardSnapshot(index, 0), shardDir);
        return shardDir;
    }

    private static List<String> golden(String fixtureIndex) throws IOException {
        List<String> docs = new ArrayList<>();
        JSON.readTree(Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures/synth-reconstructed-golden.json")
            .toFile()).path("reconstructed").path(fixtureIndex).forEach(doc -> docs.add(doc.asText()));
        return docs;
    }

    private static List<String> readAll(ShardReader reader) throws IOException {
        List<String> docs = new ArrayList<>();
        for (int docId = 0; docId < reader.docCount(); docId++) {
            String[] holder = new String[1];
            reader.readSource(docId, (bytes, length) -> holder[0] = new String(bytes, 0, length, StandardCharsets.UTF_8));
            docs.add(holder[0]);
        }
        return docs;
    }

    /** Delegates to a real reader, counting readSource calls; storedSourceAbsent fixed unless null. */
    private static final class CountingReader implements ShardReader {
        private final ShardReader delegate;
        private final Boolean storedSourceAbsent;
        int readSourceCalls;

        CountingReader(ShardReader delegate, Boolean storedSourceAbsent) {
            this.delegate = delegate;
            this.storedSourceAbsent = storedSourceAbsent;
        }

        public String runtimeName() { return delegate.runtimeName(); }
        public int docCount() throws IOException { return delegate.docCount(); }
        public boolean isLive(int docId) throws IOException { return delegate.isLive(docId); }
        public boolean storedSourceAbsent(int docId) throws IOException {
            return storedSourceAbsent == null ? delegate.storedSourceAbsent(docId) : storedSourceAbsent;
        }
        public de.sloow.gefrierschrank.core.lucene.DocumentMetadata documentMetadata(int docId) throws IOException {
            return delegate.documentMetadata(docId);
        }
        public void readSource(int docId, SourceConsumer consumer) throws IOException {
            readSourceCalls++;
            delegate.readSource(docId, consumer);
        }
        public void close() throws IOException { delegate.close(); }
    }

    private static FullIndexExporter deletesExporter() {
        var sources = new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(DELETES_REPO)));
        return new FullIndexExporter(sources, new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
    }

    private void assertReconstructedFieldsPresent(String tableUri, String... expectedFields) throws Exception {
        var conf = new Configuration();
        var log = io.delta.standalone.DeltaLog.forTable(conf, tableUri);
        var fieldNames = java.util.Arrays.stream(log.snapshot().getMetadata().getSchema().getFields())
            .map(io.delta.standalone.types.StructField::getName)
            .collect(java.util.stream.Collectors.toSet());
        for (String expected : expectedFields) {
            assertTrue(fieldNames.contains(expected),
                "reconstructed table schema is missing '" + expected + "': " + fieldNames);
        }
        try (var it = log.snapshot().open()) {
            assertTrue(it.hasNext(), "table has no rows");
        }
    }
}
