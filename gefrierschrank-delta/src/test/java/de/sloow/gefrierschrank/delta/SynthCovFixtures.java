package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The synthetic-coverage fixtures (synthcov-repo, synthcov8-repo, and synth-deletes-repo's nested
 * index) as the delta tests use them: Elasticsearch's expected output, a full export through
 * {@link FullIndexExporter}, and a direct read of one shard through the fallback reader.
 */
final class SynthCovFixtures {

    static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final ObjectMapper JSON = new ObjectMapper();

    private SynthCovFixtures() {}

    static Path repository(String index) {
        if (index.startsWith("test_deletes_")) return FIXTURES.resolve("synth-deletes-repo");
        return FIXTURES.resolve(index.startsWith("cov8_") ? "synthcov8-repo" : "synthcov-repo");
    }

    static JsonNode expected(String index) throws IOException {
        if (index.startsWith("test_deletes_")) return JSON.readTree(FIXTURES.resolve("synth-deletes-expected.json").toFile()).path(index);
        return JSON.readTree(FIXTURES.resolve("synthcov-" + index + "-expected.json").toFile());
    }

    static JsonNode metadata(String index) throws IOException {
        return new RepositoryInspector(new FileSystemBlobStore(repository(index))).indexMetadata(index);
    }

    record Row(int shard, long doc, String kind, String source) {}

    record Export(RunReport report, Map<String, Row> rows, RecordingStore store, String stderr) {
        RunReport.ShardReport shard(int id) {
            return report.shards().stream().filter(s -> s.shardId() == id).findFirst().orElseThrow();
        }
    }

    /** Exports {@code index} into {@code table}; stderr is captured. */
    static Export export(String index, Path table, Configuration conf) throws Exception {
        RecordingStore store = new RecordingStore(new FileSystemBlobStore(repository(index)));
        var sources = new RepositorySources(new RepositorySources.Source("primary", store));
        PrintStream previous = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        RunReport report;
        try {
            report = new FullIndexExporter(sources, conf, (shard, dir) -> new Lucene10ShardReader(dir))
                .run(index, table.toString(), RunReportListener.NO_OP);
        } finally {
            System.setErr(previous);
            previous.print(captured.toString(StandardCharsets.UTF_8));
        }
        return new Export(report, rows(table), store, captured.toString(StandardCharsets.UTF_8));
    }

    static Map<String, Row> rows(Path table) throws IOException {
        Map<String, Row> rows = new TreeMap<>();
        if (!Files.exists(table.resolve("_delta_log"))) return rows;
        try (var it = io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).update().open()) {
            while (it.hasNext()) {
                var row = it.next();
                assertNull(rows.put(row.getString("_id"), new Row((int) row.getLong("_shard"), row.getLong("_doc"),
                    row.getString("_source_kind"), row.getString("_source"))), "duplicate row " + row.getString("_id"));
            }
        }
        return rows;
    }

    record ShardRead(Long ignoredFieldDocuments, Boolean unrecoverableUnmappedValues, String stderr, String failure) {}

    /**
     * Reads every live document of one shard through the fallback reader, as dump does, and reports
     * its accessors before the root wrapper's close and what that close printed.
     */
    static ShardRead readShard(String index, int shard, Path tmp) throws Exception {
        var store = new FileSystemBlobStore(repository(index));
        var inspector = new RepositoryInspector(store);
        Path shardDir = tmp.resolve(index + "-" + shard);
        Files.createDirectories(shardDir.getParent());
        new ShardMaterializer(store).materialize(inspector.shardSnapshot(index, shard), shardDir);
        PrintStream previous = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        Long ignored = null;
        Boolean unmapped = null;
        String failure = null;
        try (ShardReader reader = SyntheticSourceFallbackShardReader.wrap(new Lucene10ShardReader(shardDir), shardDir, index,
                 inspector.indexMetadata(index))) {
            try {
                for (int doc = 0; doc < reader.docCount(); doc++) {
                    if (reader.isLive(doc)) reader.readSource(doc, (bytes, length) -> {});
                }
            } catch (IOException e) {
                failure = e.getClass().getSimpleName();
            }
            ignored = SyntheticSourceFallbackShardReader.ignoredFieldDocuments(reader);
            unmapped = SyntheticSourceFallbackShardReader.unrecoverableUnmappedValues(reader);
        } finally {
            System.setErr(previous);
        }
        return new ShardRead(ignored, unmapped, captured.toString(StandardCharsets.UTF_8), failure);
    }

    /**
     * The exception message a shard report carries: its errorMessage is the class name, the safe
     * message and the frames ("Class: message at frame <- frame").
     */
    static String message(RunReport.ShardReport shard) {
        String text = shard.errorMessage();
        String head = text.substring(text.indexOf(": ") + 2);
        int frames = head.indexOf(" at ");
        return frames < 0 ? head : head.substring(0, frames);
    }

    /** Ids of the expected file's documents on {@code shard}. */
    static List<String> idsOnShard(JsonNode expected, int shard) {
        return expected.path("shards").properties().stream().filter(e -> e.getValue().asInt() == shard)
            .map(Map.Entry::getKey).sorted().toList();
    }

    /** Records every blob key read. */
    static final class RecordingStore implements BlobStore {
        final List<String> read = new CopyOnWriteArrayList<>();
        private final BlobStore delegate;

        RecordingStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<BlobMetadata> stat(String key) {
            return delegate.stat(key);
        }

        @Override
        public InputStream read(String key) throws IOException {
            read.add(key);
            return delegate.read(key);
        }

        @Override
        public RangeRead openRange(String key, long offset, long length, String expectedVersion) throws IOException {
            read.add(key);
            return delegate.openRange(key, offset, length, expectedVersion);
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
            delegate.put(key, content);
        }
    }
}
