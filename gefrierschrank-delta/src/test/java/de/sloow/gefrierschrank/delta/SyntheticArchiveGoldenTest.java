package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
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
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Archive rows of every synthetic-source fixture index, exported through the production
 * {@link FullIndexExporter} path, compared with rows captured on 1.0.0: shard, Lucene doc slot,
 * {@code _id}, {@code _routing}, {@code _source_kind}, SHA-256 of the {@code _source} text, and the
 * shard outcome. Reconstruction speedups must not change a single exported byte or row.
 *
 * <p>The ZSTD fixture indices are covered by the CLI module's {@code SyntheticZstdExportTest}
 * against the same golden file: on this module's test classpath the real Elasticsearch jar precedes
 * {@code gefrierschrank-lucene10}, so the ambient {@code Lucene10ShardReader} resolves ES's own ZSTD
 * codec, which cannot decompress outside an ES node. The CLI classpath (and its fat jar) resolves
 * the lucene10 codecs first, like production.
 */
class SyntheticArchiveGoldenTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final Path GOLDEN = FIXTURES.resolve("synth-reconstructed-golden.json");
    private static final List<String> INDICES = List.of(
        "synth-repo/test_plain_synth", "synth-repo/test_tsdb_synth",
        "synth-deletes-repo/test_deletes_lz4", "synth-deletes-repo/test_deletes_nested");

    static List<String> indices() {
        return INDICES;
    }

    @ParameterizedTest
    @MethodSource("indices")
    void archiveRowsMatchTheGolden(String fixtureIndex, @TempDir Path tmp) throws Exception {
        JsonNode expected = JSON.readTree(GOLDEN.toFile()).path("archiveRows").path(fixtureIndex);
        assertEquals(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(expected),
            JSON.writerWithDefaultPrettyPrinter().writeValueAsString(export(fixtureIndex, tmp)), fixtureIndex);
    }

    /**
     * Rewrites this class's entries in the "archiveRows" section of the golden file. Run only
     * deliberately, on the build whose output is the reference: {@code mvn test
     * -Dtest=SyntheticArchiveGoldenTest -Dgefrierschrank.captureGolden=true}.
     */
    @Test
    @EnabledIfSystemProperty(named = "gefrierschrank.captureGolden", matches = "true")
    void captureArchiveRowsGolden(@TempDir Path tmp) throws Exception {
        ObjectNode root = Files.exists(GOLDEN) ? (ObjectNode) JSON.readTree(GOLDEN.toFile()) : JSON.createObjectNode();
        ObjectNode section = root.withObject("archiveRows");
        for (String fixtureIndex : INDICES) {
            section.set(fixtureIndex, export(fixtureIndex, tmp.resolve(fixtureIndex.replace('/', '_'))));
        }
        Files.writeString(GOLDEN, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n",
            StandardCharsets.UTF_8);
    }

    static ObjectNode export(String fixtureIndex, Path tmp) throws Exception {
        String repo = fixtureIndex.substring(0, fixtureIndex.indexOf('/'));
        String index = fixtureIndex.substring(fixtureIndex.indexOf('/') + 1);
        var sources = new RepositorySources(new RepositorySources.Source("primary",
            new FileSystemBlobStore(FIXTURES.resolve(repo))));
        String table = tmp.resolve("table").toString();
        RunReport report = new FullIndexExporter(sources, new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir))
            .run(index, table, RunReportListener.NO_OP);

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
