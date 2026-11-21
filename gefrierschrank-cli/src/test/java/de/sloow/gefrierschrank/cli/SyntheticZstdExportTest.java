package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.delta.FullIndexExporter;
import de.sloow.gefrierschrank.delta.RunReport;
import de.sloow.gefrierschrank.delta.RunReportListener;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Synthetic-source ZSTD fixtures through the production export path on the CLI classpath, where
 * {@code gefrierschrank-lucene10}'s codecs win the ambient SPI lookup exactly as in the fat jar (the
 * delta module's test classpath resolves Elasticsearch's own ZSTD codec, which cannot decompress
 * outside an ES node, so these cases live here). Complements the delta module's
 * {@code SyntheticArchiveGoldenTest}, sharing its golden file.
 */
class SyntheticZstdExportTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final Path GOLDEN = FIXTURES.resolve("synth-reconstructed-golden.json");
    private static final List<String> INDICES = List.of(
        "zstd-synth-repo/test_zstd_synth", "zstd-synth-repo/test_zstd_logsdb",
        "synth-deletes-repo/test_deletes_zstd");

    static List<String> indices() {
        return INDICES;
    }

    @ParameterizedTest
    @MethodSource("indices")
    void archiveRowsMatchTheGolden(String fixtureIndex, @TempDir Path tmp) throws Exception {
        JsonNode expected = JSON.readTree(GOLDEN.toFile()).path("archiveRows").path(fixtureIndex);
        assertEquals(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(expected),
            JSON.writerWithDefaultPrettyPrinter().writeValueAsString(goldenRows(fixtureIndex, tmp)), fixtureIndex);
    }

    /** Same assertions as the delta module's LZ4 variant: ES's own live documents, nothing else. */
    @Test
    void zstdDeletesFixtureExportsExactlyTheLiveDocuments(@TempDir Path tmp) throws Exception {
        String index = "test_deletes_zstd";
        JsonNode expected = JSON.readTree(FIXTURES.resolve("synth-deletes-expected.json").toFile()).path(index);
        String table = tmp.resolve("table").toString();
        RunReport report = exporter(FIXTURES.resolve("synth-deletes-repo")).run(index, table, RunReportListener.NO_OP);
        assertEquals(1, report.committedCount(), report.shards().toString());

        Map<String, JsonNode> expectedSources = new HashMap<>();
        expected.path("sources").properties().forEach(e -> expectedSources.put(e.getKey(), e.getValue()));
        Map<String, JsonNode> actual = new HashMap<>();
        try (var rows = io.delta.standalone.DeltaLog.forTable(new Configuration(), table).snapshot().open()) {
            while (rows.hasNext()) {
                var row = rows.next();
                assertEquals("synthetic", row.getString("_source_kind"));
                assertTrue(actual.put(row.getString("_id"), JSON.readTree(row.getString("_source"))) == null,
                    "duplicate row for " + row.getString("_id"));
            }
        }
        assertEquals(expectedSources.size(), report.rowsWrittenThisRun());
        assertEquals(expectedSources, actual);
        for (JsonNode deleted : expected.path("deleted_ids")) {
            assertFalse(actual.containsKey(deleted.asText()), "deleted document exported: " + deleted.asText());
        }
    }

    /**
     * Reconstruction runs Elasticsearch's real codecs in an isolated class loader; the ambient SPI
     * of this JVM must keep resolving the lucene10 read-only codecs after synthetic exports.
     */
    @Test
    void ambientCodecLookupStillResolvesTheLucene10CodecsAfterSyntheticExports(@TempDir Path tmp) throws Exception {
        RunReport tsdb = exporter(FIXTURES.resolve("synth-repo")).run("test_tsdb_synth", tmp.resolve("tsdb").toString(),
            RunReportListener.NO_OP);
        RunReport zstd = exporter(FIXTURES.resolve("zstd-synth-repo")).run("test_zstd_logsdb",
            tmp.resolve("zstd").toString(), RunReportListener.NO_OP);
        RunReport plain = exporter(Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo"))
            .run("test_data_9_3_0", tmp.resolve("plain").toString(), RunReportListener.NO_OP);
        assertEquals(List.of(1, 1, 1), List.of(tsdb.committedCount(), zstd.committedCount(), plain.committedCount()));
        assertEquals("de.sloow.gefrierschrank.lucene10",
            DocValuesFormat.forName("ES819TSDB").getClass().getPackageName());
        assertEquals("de.sloow.gefrierschrank.lucene10",
            Codec.forName("ES93TSDBDefaultCompressionLucene103Codec").getClass().getPackageName());
    }

    /**
     * Rewrites this class's entries in the "archiveRows" section of the golden file. Run only
     * deliberately, on the build whose output is the reference: {@code mvn test
     * -Dtest=SyntheticZstdExportTest -Dgefrierschrank.captureGolden=true}.
     */
    @Test
    @EnabledIfSystemProperty(named = "gefrierschrank.captureGolden", matches = "true")
    void captureArchiveRowsGolden(@TempDir Path tmp) throws Exception {
        ObjectNode root = (ObjectNode) JSON.readTree(GOLDEN.toFile());
        ObjectNode section = root.withObject("archiveRows");
        for (String fixtureIndex : INDICES) {
            section.set(fixtureIndex, goldenRows(fixtureIndex, tmp.resolve(fixtureIndex.replace('/', '_'))));
        }
        Files.writeString(GOLDEN, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n",
            StandardCharsets.UTF_8);
    }

    private static FullIndexExporter exporter(Path repo) {
        var sources = new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(repo)));
        return new FullIndexExporter(sources, new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
    }

    /** Copy of the delta module's {@code SyntheticArchiveGoldenTest.export}: same row format. */
    private static ObjectNode goldenRows(String fixtureIndex, Path tmp) throws Exception {
        String repo = fixtureIndex.substring(0, fixtureIndex.indexOf('/'));
        String index = fixtureIndex.substring(fixtureIndex.indexOf('/') + 1);
        String table = tmp.resolve("table").toString();
        RunReport report = exporter(FIXTURES.resolve(repo)).run(index, table, RunReportListener.NO_OP);

        ObjectNode result = JSON.createObjectNode();
        ArrayNode outcomes = result.putArray("shards");
        for (var shard : report.shards()) {
            outcomes.add(shard.shardId() + ":" + shard.outcome()
                + (shard.errorClass() == null ? "" : ":" + shard.errorClass()) + ":" + shard.rowsWritten());
        }
        List<List<String>> rows = new ArrayList<>();
        if (report.committedCount() > 0) {
            var sha256 = MessageDigest.getInstance("SHA-256");
            try (var it = io.delta.standalone.DeltaLog.forTable(new Configuration(), table).snapshot().open()) {
                while (it.hasNext()) {
                    var row = it.next();
                    rows.add(List.of(
                        Long.toString(row.getLong("_shard")), Long.toString(row.getLong("_doc")),
                        String.valueOf(row.isNullAt("_id") ? null : row.getString("_id")),
                        String.valueOf(row.isNullAt("_routing") ? null : row.getString("_routing")),
                        row.getString("_source_kind"),
                        HexFormat.of().formatHex(sha256.digest(row.getString("_source").getBytes(StandardCharsets.UTF_8)))));
                }
            }
        }
        rows.sort(Comparator.<List<String>>comparingLong(r -> Long.parseLong(r.get(0)))
            .thenComparingLong(r -> Long.parseLong(r.get(1))));
        ArrayNode rowsNode = result.putArray("rows");
        for (List<String> row : rows) {
            rowsNode.add(String.join(" ", row));
        }
        return result;
    }
}
