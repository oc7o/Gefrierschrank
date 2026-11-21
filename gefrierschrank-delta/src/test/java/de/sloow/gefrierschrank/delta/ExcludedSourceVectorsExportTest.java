package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ES 9.2+ keeps dense vectors out of the stored {@code _source} by default. 1.0.0 exported such
 * an index with every embedding silently missing and 1.1.0 refused it; now the vectors are put
 * back through Elasticsearch's own loaders. Exclusions that cannot be rehydrated, and every
 * exclusion with rehydration switched off, still fail before anything is written or downloaded.
 */
class ExcludedSourceVectorsExportTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final String INDEX = "test_es93_vectors_default";

    @Test
    void excludedVectorsComeBackEqualToElasticsearchsFetch(@TempDir Path tmp) throws Exception {
        var result = VectorExportJvm.export(FIXTURES.resolve("es93-repo"), tmp, List.of(INDEX), 1, 2, Map.of());
        JsonNode first = result.reports().get(INDEX + "#1");
        assertEquals(1, first.path("committedCount").asInt(), first.toString());
        assertEquals(20, first.path("rowsWrittenThisRun").asLong(), first.toString());
        JsonNode rerun = result.reports().get(INDEX + "#2");
        assertEquals("SKIPPED_ALREADY_COMMITTED", rerun.path("shards").path(0).path("outcome").asText(), rerun.toString());
        assertEquals(0, rerun.path("rowsWrittenThisRun").asLong());

        JsonNode expected = VectorExportJvm.EXACT.readTree(FIXTURES.resolve("es93-vectors-default-expected.json").toFile());
        Map<String, VectorExportJvm.Row> rows = VectorExportJvm.rows(tmp.resolve(INDEX));
        assertEquals(20, rows.size());
        for (var row : rows.values()) {
            assertEquals("stored+vectors", row.kind(), row.id());
            JsonNode source = VectorExportJvm.EXACT.readTree(row.source());
            assertEquals(VectorExportJvm.canonical(expected.get(row.id())), VectorExportJvm.canonical(source), row.id());
            byte[] stored = result.stored().get(INDEX + "/" + row.id());
            byte[] bytes = row.source().getBytes(StandardCharsets.UTF_8);
            String member = new String(bytes, stored.length - 1, bytes.length - stored.length, StandardCharsets.UTF_8);
            assertTrue(member.startsWith(",\"embedding\":["), member);
            String withoutMember = new String(bytes, 0, stored.length - 1, StandardCharsets.UTF_8) + "}";
            assertArrayEquals(stored, withoutMember.getBytes(StandardCharsets.UTF_8), row.id());
        }
        for (Map<String, String> tags : VectorExportJvm.fileTags(tmp.resolve(INDEX))) {
            assertEquals(SourceVectorsPlan.FILE_TAG_VALUE, tags.get(SourceVectorsPlan.FILE_TAG), tags.toString());
        }
    }

    @Test
    void theKillSwitchFailsTheIndexBeforeAnyShardDataOrTableIsTouched(@TempDir Path tmp) throws Exception {
        var store = new RecordingBlobStore(new FileSystemBlobStore(FIXTURES.resolve("es93-repo")));
        var sources = new RepositorySources(new RepositorySources.Source("primary", store));
        var exporter = new FullIndexExporter(sources, new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
        Path table = tmp.resolve(INDEX);
        String previous = System.getProperty(SourceVectorsPlan.REHYDRATE_PROPERTY);
        System.setProperty(SourceVectorsPlan.REHYDRATE_PROPERTY, "false");
        SourceVectorsPlan.ExcludedSourceVectorsException failure;
        try {
            failure = assertThrows(SourceVectorsPlan.ExcludedSourceVectorsException.class,
                () -> exporter.run(INDEX, table.toString(), RunReportListener.NO_OP));
        } finally {
            if (previous == null) System.clearProperty(SourceVectorsPlan.REHYDRATE_PROPERTY);
            else System.setProperty(SourceVectorsPlan.REHYDRATE_PROPERTY, previous);
        }
        assertTrue(store.touched.stream().noneMatch(key -> key.contains("/__")), "no shard data blob may be read: " + store.touched);
        assertFalse(Files.exists(table), "neither the document table nor _export_metadata may be created");
        assertContentFree(failure.getMessage());
    }

    /** rank_vectors, semantic_text and an exclusion the probe cannot evaluate refuse the index in the preflight. */
    @Test
    void unrehydratableExclusionsFailThePreflightBeforeAnyShardData() throws Exception {
        var store = new RecordingBlobStore(new FileSystemBlobStore(FIXTURES.resolve("es93-repo")));
        var sources = new RepositorySources(new RepositorySources.Source("primary", store));
        var resolved = sources.resolve(INDEX, sources.primary().inspector().latestSnapshotUuid(INDEX));
        for (String type : List.of("rank_vectors", "semantic_text", "dense_vector_with_unknown_parameter")) {
            ObjectNode metadata = resolved.metadata().deepCopy();
            ObjectNode embedding = (ObjectNode) ExportPreflight.documentMapping(metadata).path("properties").path("embedding");
            if (type.startsWith("dense_vector")) embedding.put("no_such_parameter", true);
            else embedding.removeAll().put("type", type);
            var changed = new RepositorySources.Resolved(resolved.source(), resolved.indexName(), resolved.snapshotUuid(),
                metadata, resolved.referenceChain());
            var failure = assertThrows(SourceVectorsPlan.ExcludedSourceVectorsException.class,
                () -> ExportPreflight.require(INDEX, changed), type);
            assertContentFree(failure.getMessage());
        }
        assertTrue(store.touched.stream().noneMatch(key -> key.contains("/__")), "no shard data blob may be read: " + store.touched);
    }

    @Test
    void anIndexThatKeepsVectorsInSourceExportsThem(@TempDir Path tmp) throws Exception {
        Map<String, JsonNode> reports = Es93BinaryDocValuesPipelineTest.exportTwiceInChildJvm(tmp, List.of("test_es93_vectors_included"));
        JsonNode report = reports.get("test_es93_vectors_included#1");
        assertEquals(1, report.path("committedCount").asInt(), report.toString());
        assertEquals(20, report.path("rowsWrittenThisRun").asLong(), report.toString());

        JsonNode expected = JSON.readTree(FIXTURES.resolve("es93-vectors-included-expected.json").toFile());
        Map<String, JsonNode> expectedById = new HashMap<>();
        expected.fields().forEachRemaining(doc -> expectedById.put(doc.getKey(), doc.getValue()));
        Map<String, JsonNode> actual = new HashMap<>();
        try (var rows = io.delta.standalone.DeltaLog.forTable(new Configuration(),
                 tmp.resolve("test_es93_vectors_included").toString()).snapshot().open()) {
            while (rows.hasNext()) {
                var row = rows.next();
                assertEquals("stored", row.getString("_source_kind"));
                JsonNode source = JSON.readTree(row.getString("_source"));
                assertEquals(8, source.path("embedding").size(), source.toString());
                assertNull(actual.put(row.getString("_id"), source));
            }
        }
        assertEquals(expectedById, actual);
    }

    private static void assertContentFree(String message) throws IOException {
        assertFalse(message.contains("embedding"), message);
        assertFalse(message.contains("synthetic source"), message);
        JsonNode expected = JSON.readTree(FIXTURES.resolve("es93-vectors-default-expected.json").toFile());
        expected.fields().forEachRemaining(doc -> {
            assertFalse(message.contains(doc.getKey()), message);
            assertFalse(message.contains(doc.getValue().path("title").asText()), message);
        });
    }

    /** Records every key whose content or metadata is requested. */
    static final class RecordingBlobStore implements BlobStore {
        final List<String> touched = new CopyOnWriteArrayList<>();
        private final BlobStore delegate;

        RecordingBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<BlobMetadata> stat(String key) {
            touched.add(key);
            return delegate.stat(key);
        }

        @Override
        public InputStream read(String key) throws IOException {
            touched.add(key);
            return delegate.read(key);
        }

        @Override
        public List<String> list(String prefix) {
            return delegate.list(prefix);
        }

        @Override
        public List<BlobMetadata> listWithSizes(String prefix) {
            return delegate.listWithSizes(prefix);
        }

        @Override
        public void put(String key, byte[] content) {
            throw new UnsupportedOperationException("the source repository is read-only");
        }
    }
}
