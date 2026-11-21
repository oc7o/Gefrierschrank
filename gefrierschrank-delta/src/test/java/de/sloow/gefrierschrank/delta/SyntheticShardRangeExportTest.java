package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Synthetic-source shards in ranges: each range reconstructs through its own fork of the fallback
 * reader, with its own reconstructor, and the rows equal the 1.0.0 golden of
 * SyntheticArchiveGoldenTest (read-only here), nested roots included: a range starts at a root,
 * whose nested children precede it, and its fork collects them regardless of the range bounds.
 */
class SyntheticShardRangeExportTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final Path GOLDEN = FIXTURES.resolve("synth-reconstructed-golden.json");

    private String previousMinimum;

    @BeforeEach
    void smallRanges() {
        previousMinimum = System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "1");
    }

    @AfterEach
    void restore() {
        if (previousMinimum == null) System.clearProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY);
        else System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, previousMinimum);
    }

    record Case(String index, int threads) {
        @Override public String toString() { return index + " K=" + threads; }
    }

    static Stream<Case> cases() {
        return Stream.of("synth-repo/test_plain_synth", "synth-repo/test_tsdb_synth",
                "synth-deletes-repo/test_deletes_lz4", "synth-deletes-repo/test_deletes_nested")
            .flatMap(index -> Stream.of(2, 3, 4).map(k -> new Case(index, k)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void rangedSyntheticRowsMatchTheGolden(Case c, @TempDir Path tmp) throws Exception {
        JsonNode expected = JSON.readTree(GOLDEN.toFile()).path("archiveRows").path(c.index());
        assertFalse(expected.isMissingNode());
        var forks = new AtomicInteger();
        var threads = java.util.Collections.synchronizedSet(new java.util.HashSet<String>());
        ObjectNode actual = export(c.index(), tmp, c.threads(), forks, threads);
        assertEquals(JSON.writerWithDefaultPrettyPrinter().writeValueAsString(expected),
            JSON.writerWithDefaultPrettyPrinter().writeValueAsString(actual), c.toString());
        assertEquals(c.threads(), forks.get(), "one fork per range");
        if (actual.path("rows").size() > 0) assertTrue(threads.size() >= 2, "ranges read on their own threads: " + threads);
    }

    /** SyntheticArchiveGoldenTest's export with a shard-thread setting, and a reader that records forks and threads. */
    static ObjectNode export(String fixtureIndex, Path tmp, int shardThreads, AtomicInteger forks,
                             java.util.Set<String> threads) throws Exception {
        String repo = fixtureIndex.substring(0, fixtureIndex.indexOf('/'));
        String index = fixtureIndex.substring(fixtureIndex.indexOf('/') + 1);
        var sources = new RepositorySources(new RepositorySources.Source("primary",
            new FileSystemBlobStore(FIXTURES.resolve(repo))));
        String table = tmp.resolve("table").toString();
        var conf = new Configuration();
        conf.set(ShardRanges.SHARD_THREADS_KEY, Integer.toString(shardThreads));
        RunReport report = new FullIndexExporter(sources, conf,
            (shard, dir) -> new Recording(new Lucene10ShardReader(dir), forks, threads))
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
        for (List<String> row : rows) rowsNode.add(String.join(" ", row));
        return result;
    }

    /** A Lucene 10 reader that counts its forks and the threads that read documents. */
    static final class Recording implements ShardReader {
        private final Lucene10ShardReader delegate;
        private final AtomicInteger forks;
        private final java.util.Set<String> threads;

        Recording(Lucene10ShardReader delegate, AtomicInteger forks, java.util.Set<String> threads) {
            this.delegate = delegate;
            this.forks = forks;
            this.threads = threads;
        }

        @Override public String runtimeName() { return delegate.runtimeName(); }
        @Override public int docCount() { return delegate.docCount(); }
        @Override public boolean isLive(int docId) { return delegate.isLive(docId); }
        @Override public boolean storedSourceAbsent(int docId) { return delegate.storedSourceAbsent(docId); }

        @Override public de.sloow.gefrierschrank.core.lucene.DocumentMetadata documentMetadata(int docId) throws java.io.IOException {
            threads.add(Thread.currentThread().getName());
            return delegate.documentMetadata(docId);
        }

        @Override public void readSource(int docId, SourceConsumer consumer) throws java.io.IOException {
            delegate.readSource(docId, consumer);
        }

        @Override public ShardReader fork() {
            forks.incrementAndGet();
            return new Recording((Lucene10ShardReader) delegate.fork(), forks, threads);
        }

        @Override public void close() throws java.io.IOException { delegate.close(); }
    }
}
