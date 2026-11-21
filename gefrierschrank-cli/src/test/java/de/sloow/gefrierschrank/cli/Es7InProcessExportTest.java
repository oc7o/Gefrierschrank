package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ES 7 (Lucene 8) shards exported in-process by dump-to-delta, compared row by row with the
 * golden rows captured through the 1.0.0 worker path (fixtures/es7-golden-rows).
 */
class Es7InProcessExportTest {

    static final Path FIXTURES = Path.of("../gefrierschrank-core/src/test/resources/fixtures");
    static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void exportsEs7ShardInProcessAndResumes(@TempDir Path tmp) throws Exception {
        String table = tmp.resolve("delta").toString();
        Path report = tmp.resolve("report.json");
        assertEquals(0, dumpToDelta(FIXTURES.resolve("demo-repo"), "--index", "test_data_7_17_9", "-o", table,
            "--report", report.toString()));
        assertEquals(Golden.load("demo-repo", "test_data_7_17_9"), tableRows(table));
        assertEquals("COMMITTED", JSON.readTree(report.toFile()).path("shards").path(0).path("outcome").asText());

        assertEquals(0, dumpToDelta(FIXTURES.resolve("demo-repo"), "--index", "test_data_7_17_9", "-o", table,
            "--report", report.toString()));
        assertEquals("SKIPPED_ALREADY_COMMITTED", JSON.readTree(report.toFile()).path("shards").path(0).path("outcome").asText());
        assertEquals(500, tableRows(table).size(), "a rerun adds no rows");
    }

    @Test
    void es7FeatureIndicesMatchGoldenRowsAndSourcelessIndexFailsBeforeDownload(@TempDir Path tmp) throws Exception {
        Path root = tmp.resolve("tables");
        Path reports = tmp.resolve("reports");
        assertEquals(4, dumpToDelta(FIXTURES.resolve("es7-features-repo"), "--all-indices", "-o", root.toString(),
            "--report", reports.toString()));
        for (String index : List.of("es7_features", "es7_hard_deletes", "es7_completion")) {
            assertEquals(Golden.load("es7-features-repo", index), tableRows(root.resolve(index).toString()), index);
        }
        assertFailedBeforeDownload(root.resolve("es7_no_source"), reports.resolve("manifest.json"));

        assertEquals(4, dumpToDelta(FIXTURES.resolve("es7-features-repo"), "--all-indices", "-o", root.toString(),
            "--report", reports.toString()));
        assertFailedBeforeDownload(root.resolve("es7_no_source"), reports.resolve("manifest.json"));
        assertEquals("SKIPPED_ALREADY_COMMITTED",
            JSON.readTree(reports.resolve("es7_features.json").toFile()).path("shards").path(0).path("outcome").asText());
    }

    /** Mixed Lucene 8 + 9 segments, and a force-merged ES 7 index whose files are all Lucene 9. */
    @Test
    void es7IndicesUpgradedToEs8MatchGoldenRows(@TempDir Path tmp) throws Exception {
        Path root = tmp.resolve("tables");
        assertEquals(0, dumpToDelta(FIXTURES.resolve("es7-upgraded-repo"), "--all-indices", "-o", root.toString()));
        for (String index : List.of("es7_mixed", "es7_merged")) {
            assertEquals(Golden.load("es7-upgraded-repo", index), tableRows(root.resolve(index).toString()), index);
        }
    }

    /** ES 7.10-7.17 best_compression (Lucene87 DEFLATE with preset dictionaries), including a sliced document. */
    @Test
    void es7BestCompressionIndexMatchesGoldenRows(@TempDir Path tmp) throws Exception {
        String table = tmp.resolve("delta").toString();
        assertEquals(0, dumpToDelta(FIXTURES.resolve("es7-best-compression-repo"), "--index", "es7_best_compression",
            "-o", table));
        assertEquals(Golden.load("es7-best-compression-repo", "es7_best_compression"), tableRows(table));
    }

    /** The worker opt-out of 1.1.0 is refused before the repository is read, whatever the shard's Lucene version. */
    @Test
    void removedWorkerOptOutFailsTheRunBeforeAnyShardIsRead(@TempDir Path tmp) throws Exception {
        Path table = tmp.resolve("delta");
        Path report = tmp.resolve("report.json");
        for (String index : List.of("test_data_7_17_9", "test_data_9_3_0")) {
            var run = CommandRuns.captureStderr(() -> withProperties(new String[]{LuceneRuntimeSelector.LUCENE8_RUNTIME_PROPERTY, "worker"},
                () -> dumpToDelta(FIXTURES.resolve("demo-repo"), "--index", index, "-o", table.toString(),
                    "--report", report.toString())));
            assertEquals(2, run.exit(), index);
            assertTrue(run.stderr().lines().anyMatch("the Lucene 9 worker was removed in 1.2.0"::equals), run.stderr());
        }
        assertFalse(Files.exists(table), "nothing was written");
        assertFalse(Files.exists(report), "no shard was attempted");
    }

    /** A typo would otherwise be ignored or quarantine shards; it is a usage error like the removed value. */
    @Test
    void mistypedOptOutFailsTheRunBeforeAnyShardIsRead(@TempDir Path tmp) throws Exception {
        Path table = tmp.resolve("delta");
        Path report = tmp.resolve("report.json");
        var run = CommandRuns.captureStderr(() -> withProperties(new String[]{LuceneRuntimeSelector.LUCENE8_RUNTIME_PROPERTY, "Worker"},
            () -> dumpToDelta(FIXTURES.resolve("demo-repo"), "--index", "test_data_7_17_9", "-o", table.toString(),
                "--report", report.toString())));
        assertEquals(2, run.exit());
        assertTrue(run.stderr().lines().anyMatch(LuceneRuntimeSelector.INVALID_RUNTIME::equals), run.stderr());
        assertFalse(Files.exists(table), "nothing was written");
        assertFalse(Files.exists(report), "no shard was attempted");
    }

    /** ExportPreflight: _source was never stored, so the index fails before any table or download. */
    private static void assertFailedBeforeDownload(Path table, Path manifest) throws IOException {
        JsonNode failed = null;
        for (JsonNode entry : JSON.readTree(manifest.toFile()).path("indices")) {
            if (entry.path("indexName").asText().equals(table.getFileName().toString())) failed = entry;
        }
        assertTrue(failed != null, "manifest entry for " + table.getFileName());
        assertEquals("FAILED", failed.path("outcome").asText());
        assertEquals("SourceDisabledException", failed.path("errorClass").asText());
        assertFalse(Files.exists(table), "no table may be created");
    }

    static int dumpToDelta(Path repo, String... args) {
        var arguments = new ArrayList<String>();
        arguments.add(repo.toString());
        arguments.addAll(List.of(args));
        return new picocli.CommandLine(new DumpToDeltaCommand()).execute(arguments.toArray(String[]::new));
    }

    static List<Golden.Row> tableRows(String table) throws IOException {
        var rows = new ArrayList<Golden.Row>();
        var log = io.delta.standalone.DeltaLog.forTable(new Configuration(), table);
        try (var iterator = log.snapshot().open()) {
            while (iterator.hasNext()) {
                var row = iterator.next();
                rows.add(new Golden.Row(row.getString("_id"), row.isNullAt("_routing") ? null : row.getString("_routing"),
                    row.getString("_source_kind"), row.getLong("_doc"),
                    Golden.sha256(row.getString("_source").getBytes(StandardCharsets.UTF_8))));
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        rows.sort(Comparator.comparingLong(Golden.Row::doc));
        return rows;
    }

    /** Sets system properties for the duration of body; test classes run sequentially in one JVM. */
    static <T> T withProperties(String[] keyValue, Callable<T> body) throws Exception {
        var previous = new String[keyValue.length / 2];
        for (int i = 0; i < keyValue.length; i += 2) {
            previous[i / 2] = System.getProperty(keyValue[i]);
            System.setProperty(keyValue[i], keyValue[i + 1]);
        }
        try {
            return body.call();
        } finally {
            for (int i = 0; i < keyValue.length; i += 2) {
                if (previous[i / 2] == null) System.clearProperty(keyValue[i]);
                else System.setProperty(keyValue[i], previous[i / 2]);
            }
        }
    }

    /** Rows captured through the 1.0.0 worker path; see es7-golden-rows/*.json. */
    static final class Golden {
        record Row(String id, String routing, String sourceKind, long doc, String sha256) {}

        static List<Row> load(String repository, String index) throws IOException {
            JsonNode node = JSON.readTree(FIXTURES.resolve("es7-golden-rows").resolve(repository + ".json").toFile()).path(index);
            assertTrue(node.has("rows"), repository + "/" + index + " has no golden rows");
            var rows = new ArrayList<Row>();
            for (JsonNode row : node.path("rows")) {
                rows.add(new Row(row.path("_id").asText(), row.path("_routing").isNull() ? null : row.path("_routing").asText(),
                    row.path("_source_kind").asText(), row.path("_doc").asLong(), row.path("sha256").asText()));
            }
            return rows;
        }

        static Row row(DocumentMetadata metadata, byte[] source) {
            return new Row(metadata.id(), metadata.routing(), metadata.sourceKind(), metadata.slot(), sha256(source));
        }

        static String sha256(byte[] bytes) {
            try {
                return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
