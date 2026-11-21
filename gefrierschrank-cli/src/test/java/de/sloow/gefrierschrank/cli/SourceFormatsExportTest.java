package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * source-formats-repo (ES 9.3.0, scripts/generate_test_snapshots.py source-formats): the same
 * documents indexed as JSON, SMILE, CBOR and YAML export as the JSON twin's rows, each converted
 * document byte-equal to what Elasticsearch's GET returned for it (source-formats-expected.json).
 */
class SourceFormatsExportTest {

    static final Path FIXTURES = Path.of("../gefrierschrank-core/src/test/resources/fixtures");
    static final Path REPO = FIXTURES.resolve("source-formats-repo");
    static final ObjectMapper JSON = new ObjectMapper();
    static final List<String> CONVERTED = List.of("sf_smile", "sf_cbor", "sf_yaml");
    static JsonNode expected;

    @BeforeAll
    static void load() throws IOException {
        expected = JSON.readTree(FIXTURES.resolve("source-formats-expected.json").toFile()).path("indices");
    }

    record Row(String id, String routing, long shard, long doc, String kind, String source) {}

    static Map<String, Row> rows(Path table) throws IOException {
        Map<String, Row> rows = new TreeMap<>();
        try (var iterator = io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).snapshot().open()) {
            while (iterator.hasNext()) {
                var row = iterator.next();
                var previous = rows.put(row.getString("_id"), new Row(row.getString("_id"),
                    row.isNullAt("_routing") ? null : row.getString("_routing"), row.getLong("_shard"), row.getLong("_doc"),
                    row.getString("_source_kind"), row.getString("_source")));
                assertTrue(previous == null, "exactly once: " + row.getString("_id"));
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
        return rows;
    }

    record Run(int exitCode, String stderr) {}

    static Run dumpToDelta(Path root, Path reports, String... options) throws Exception {
        var args = new ArrayList<>(List.of(REPO.toString(), "--all-indices", "-o", root.toString(), "--report", reports.toString()));
        args.addAll(List.of(options));
        var err = new ByteArrayOutputStream();
        var stderr = System.err;
        System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
        int exit;
        try {
            exit = new picocli.CommandLine(new DumpToDeltaCommand()).execute(args.toArray(String[]::new));
        } finally {
            System.setErr(stderr);
        }
        return new Run(exit, err.toString(StandardCharsets.UTF_8));
    }

    static JsonNode shardReport(Path reports, String index) throws IOException {
        return JSON.readTree(reports.resolve(index + ".json").toFile()).path("shards").path(0);
    }

    static String golden(String index, String id) {
        JsonNode entry = expected.path(index).path(id);
        assertEquals(200, entry.path("get_status").asInt(), index + "/" + id);
        return entry.path("get_source").asText();
    }

    @Test
    void convertedIndicesExportTheTwinsRowsAsElasticsearchRendersThem(@TempDir Path tmp) throws Exception {
        Path root = tmp.resolve("tables");
        Path reports = tmp.resolve("reports");
        var run = dumpToDelta(root, reports, "--shard-threads", "1");
        assertEquals(3, run.exitCode(), "sf_yaml_nomarker quarantines:\n" + run.stderr());

        Map<String, Row> twin = rows(root.resolve("sf_json"));
        assertEquals(expected.path("sf_json").size(), twin.size());
        for (Row row : twin.values()) {
            assertEquals("stored", row.kind());
            assertEquals(expected.path("sf_json").path(row.id()).path("body").asText(), row.source(), "byte-exact twin " + row.id());
        }
        for (String index : CONVERTED) {
            Map<String, Row> rows = rows(root.resolve(index));
            assertEquals(twin.keySet(), rows.keySet(), index);
            String kind = "stored-" + index.substring("sf_".length());
            for (Row row : rows.values()) {
                Row json = twin.get(row.id());
                assertEquals(json.routing(), row.routing(), index + "/" + row.id());
                assertEquals(json.shard(), row.shard());
                assertEquals(kind, row.kind());
                assertEquals(JSON.readTree(json.source()), JSON.readTree(row.source()), "parsed equal to the twin: " + index + "/" + row.id());
                assertEquals(golden(index, row.id()), row.source(), "byte-equal to Elasticsearch: " + index + "/" + row.id());
            }
            assertEquals(1, run.stderr().lines().filter(line -> line.startsWith("note: shard 0 of " + index + ": ")).count(),
                run.stderr());
        }
        assertTrue(run.stderr().contains("note: shard 0 of sf_smile: 39 SMILE, 0 CBOR, 0 YAML documents converted to JSON"
            + " (not byte-exact; _source_kind stored-smile/stored-cbor/stored-yaml)"), run.stderr());
        assertTrue(run.stderr().contains("note: shard 0 of sf_cbor: 0 SMILE, 39 CBOR, 0 YAML documents"), run.stderr());
        assertTrue(run.stderr().contains("note: shard 0 of sf_yaml: 0 SMILE, 0 CBOR, 39 YAML documents"), run.stderr());
        assertTrue(run.stderr().contains("note: shard 0 of sf_nonfinite: 1 SMILE, 3 CBOR, 0 YAML documents"), run.stderr());
        assertFalse(run.stderr().contains("plain title"), "counts only");
        assertFalse(run.stderr().contains("note: shard 0 of sf_json"), run.stderr());

        for (String index : List.of("sf_nonfinite", "sf_yaml_text")) {
            Map<String, Row> rows = rows(root.resolve(index));
            assertEquals(expected.path(index).size(), rows.size(), index);
            for (Row row : rows.values()) {
                String format = expected.path(index).path(row.id()).path("format").asText();
                assertEquals("stored-" + format, row.kind());
                assertEquals(golden(index, row.id()), row.source(), "byte-equal to Elasticsearch: " + index + "/" + row.id());
            }
        }

        // Elasticsearch's YAML parser cannot read .nan/.inf, so its GET fails; the export writes the
        // strings Elasticsearch writes for non-finite SMILE and CBOR numbers.
        Map<String, Row> yamlNonFinite = rows(root.resolve("sf_yaml_nonfinite"));
        assertEquals(golden("sf_yaml_nonfinite", "yn-2"), yamlNonFinite.get("yn-2").source());
        assertEquals(500, expected.path("sf_yaml_nonfinite").path("yn-1").path("get_status").asInt());
        assertEquals("{\"n\":1,\"raw\":{\"nan\":\"NaN\",\"inf\":\"Infinity\",\"ninf\":\"-Infinity\"}}",
            yamlNonFinite.get("yn-1").source());

        // Without the leading '---' Elasticsearch cannot detect YAML either (its GET fails): the shard fails as in 1.1.0.
        for (JsonNode entry : expected.path("sf_yaml_nomarker")) assertEquals(500, entry.path("get_status").asInt());
        JsonNode nomarker = shardReport(reports, "sf_yaml_nomarker");
        assertEquals("QUARANTINED", nomarker.path("outcome").asText());
        assertNotEquals("SourceConversionException", nomarker.path("errorClass").asText());

        // A second run skips every committed shard and retries only the quarantined one.
        var again = dumpToDelta(root, reports, "--shard-threads", "1");
        assertEquals(3, again.exitCode());
        for (String index : List.of("sf_json", "sf_smile", "sf_cbor", "sf_yaml", "sf_nonfinite", "sf_yaml_text", "sf_yaml_nonfinite")) {
            assertEquals("SKIPPED_ALREADY_COMMITTED", shardReport(reports, index).path("outcome").asText(), index);
        }
        assertEquals(twin, rows(root.resolve("sf_json")));
        assertEquals("QUARANTINED", shardReport(reports, "sf_yaml_nomarker").path("outcome").asText());
        assertFalse(again.stderr().contains("converted to JSON"), "a skipped shard converts nothing");
    }

    @Test
    void fourShardThreadsExportIdenticalRows(@TempDir Path tmp) throws Exception {
        var one = dumpToDelta(tmp.resolve("one"), tmp.resolve("reports-one"), "--shard-threads", "1");
        var four = withProperty("gefrierschrank.shard.threads.minRangeLiveDocs", "1",
            () -> dumpToDelta(tmp.resolve("four"), tmp.resolve("reports-four"), "--shard-threads", "4"));
        assertEquals(3, one.exitCode());
        assertEquals(3, four.exitCode());
        for (String index : List.of("sf_json", "sf_smile", "sf_cbor", "sf_yaml", "sf_nonfinite", "sf_yaml_text")) {
            assertEquals(rows(tmp.resolve("one").resolve(index)), rows(tmp.resolve("four").resolve(index)), index);
        }
        assertEquals(notes(one.stderr()), notes(four.stderr()), "counted once per document");
        try (var files = Files.list(tmp.resolve("four").resolve("sf_smile"))) {
            assertTrue(files.filter(f -> f.toString().endsWith(".parquet")).count() > 1, "read in ranges");
        }
    }

    private static List<String> notes(String stderr) {
        return stderr.lines().filter(line -> line.startsWith("note: ")).sorted().toList();
    }

    @Test
    void dumpWritesTheJsonElasticsearchReturns(@TempDir Path tmp) throws Exception {
        Path root = tmp.resolve("tables");
        assertEquals(3, dumpToDelta(root, tmp.resolve("reports"), "--shard-threads", "1").exitCode());
        var byDoc = new ArrayList<>(rows(root.resolve("sf_smile")).values());
        byDoc.sort(Comparator.comparingLong(Row::doc));
        var expectedLines = new StringBuilder();
        for (Row row : byDoc) expectedLines.append(golden("sf_smile", row.id())).append('\n');

        Path output = tmp.resolve("sf_smile.jsonl");
        assertEquals(0, new picocli.CommandLine(new DumpCommand()).execute(REPO.toString(), "--index", "sf_smile",
            "-o", output.toString()));
        assertArrayEquals(expectedLines.toString().getBytes(StandardCharsets.UTF_8), Files.readAllBytes(output));
    }

    static <T> T withProperty(String key, String value, Callable<T> body) throws Exception {
        String previous = System.getProperty(key);
        System.setProperty(key, value);
        try {
            return body.call();
        } finally {
            if (previous == null) System.clearProperty(key);
            else System.setProperty(key, previous);
        }
    }
}
