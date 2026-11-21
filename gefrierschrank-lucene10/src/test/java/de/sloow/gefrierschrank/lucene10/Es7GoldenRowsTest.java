package de.sloow.gefrierschrank.lucene10;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ES 7-created shards read in-process must produce exactly the rows the 1.0.0 Lucene 9 worker
 * produced: (_id, _routing, _source_kind, _doc, sha256 of the source bytes) for every live root
 * document, in order. Deleted versions, tombstones and nested children never appear. The
 * es7_completion index needs lucene-suggest's Completion84 postings, which only the CLI classpath
 * carries; Es7InProcessExportTest covers it there.
 */
class Es7GoldenRowsTest {

    static final Path FIXTURES = Path.of("../gefrierschrank-core/src/test/resources/fixtures");
    static final ObjectMapper JSON = new ObjectMapper();

    record Row(String id, String routing, String sourceKind, long doc, String sha256) {}

    @ParameterizedTest(name = "{0}/{1}")
    @CsvSource({
        "demo-repo, test_data_7_17_9, 500",
        "es7-features-repo, es7_features, 61",
        "es7-features-repo, es7_hard_deletes, 27",
        "es7-upgraded-repo, es7_mixed, 53",
        "es7-upgraded-repo, es7_merged, 53",
        "es7-best-compression-repo, es7_best_compression, 257"})
    void inProcessRowsEqualTheWorkerGoldenRows(String repository, String index, int liveRoots, @TempDir Path tmp) throws Exception {
        var blobs = new FileSystemBlobStore(FIXTURES.resolve(repository));
        var inspector = new RepositoryInspector(blobs);
        JsonNode golden = JSON.readTree(FIXTURES.resolve("es7-golden-rows").resolve(repository + ".json").toFile()).path(index);
        var shard = inspector.shardSnapshot(index, 0);
        assertEquals(golden.path("snapshot").asText(), inspector.latestSnapshotUuid(index));
        new ShardMaterializer(blobs).materialize(shard, tmp);

        var expected = new ArrayList<Row>();
        for (JsonNode row : golden.path("rows")) {
            expected.add(new Row(row.path("_id").asText(), row.path("_routing").isNull() ? null : row.path("_routing").asText(),
                row.path("_source_kind").asText(), row.path("_doc").asLong(), row.path("sha256").asText()));
        }
        assertEquals(liveRoots, expected.size());

        var actual = new ArrayList<Row>();
        int slots;
        try (var reader = new Lucene10ShardReader(tmp)) {
            slots = reader.docCount();
            for (int doc = 0; doc < slots; doc++) {
                if (!reader.isLive(doc)) continue;
                byte[][] source = new byte[1][];
                reader.readSource(doc, (bytes, length) -> source[0] = Arrays.copyOf(bytes, length));
                DocumentMetadata metadata = reader.documentMetadata(doc);
                actual.add(new Row(metadata.id(), metadata.routing(), metadata.sourceKind(), metadata.slot(), sha256(source[0])));
            }
        }
        assertEquals(expected, actual);
        if (!index.equals("test_data_7_17_9")) {
            assertTrue(slots > liveRoots, "fixture keeps deleted versions, tombstones or nested children: " + slots);
        }
    }

    /**
     * es7_merged must stay an ES 7-created index whose files were all rewritten by Lucene 9, the shape
     * that never reached the worker; es7_mixed must keep Lucene 8 segments next to Lucene 9 ones.
     */
    @ParameterizedTest(name = "{0}")
    @CsvSource({"es7_mixed, '8,9'", "es7_merged, '9'"})
    void upgradedFixtureShapesArePinned(String index, String writtenByMajors) throws Exception {
        var inspector = new RepositoryInspector(new FileSystemBlobStore(FIXTURES.resolve("es7-upgraded-repo")));
        int created = inspector.indexMetadata(index).path("settings").path("index.version.created").asInt();
        assertTrue(created >= 7_000_000 && created < 8_000_000, "ES 7-created: " + created);
        var majors = inspector.shardSnapshot(index, 0).files().stream()
            .map(RepoFileInfo::writtenBy)
            .map(version -> version.substring(0, version.indexOf('.')))
            .collect(Collectors.toCollection(TreeSet::new));
        assertEquals(writtenByMajors, String.join(",", majors));
    }

    /**
     * ES 7.10-7.17 best_compression: Lucene87 DEFLATE with preset dictionaries, read through the
     * backward-codecs Lucene50 compressing reader in merge mode. The fixture must keep a segment that
     * spans several 480 KB chunks and a document stored in slices (larger than two chunks).
     */
    @Test
    void bestCompressionFixtureShapeIsPinned(@TempDir Path tmp) throws Exception {
        var blobs = new FileSystemBlobStore(FIXTURES.resolve("es7-best-compression-repo"));
        new ShardMaterializer(blobs).materialize(new RepositoryInspector(blobs).shardSnapshot("es7_best_compression", 0), tmp);
        int chunk = 491_520;
        try (var dir = new NIOFSDirectory(tmp)) {
            var infos = SegmentInfos.readLatestCommit(dir, 8);
            assertEquals(8, infos.getIndexCreatedVersionMajor());
            assertTrue(infos.size() >= 2, "segments: " + infos.size());
            for (var segment : infos) {
                assertEquals("Lucene87", segment.info.getCodec().getName());
                assertEquals("BEST_COMPRESSION", segment.info.getAttribute("Lucene87StoredFieldsFormat.mode"));
            }
        }
        long largestSegmentSourceBytes = 0;
        int largestSource = 0;
        try (var dir = new NIOFSDirectory(tmp);
             var reader = DirectoryReader.open(DirectoryReader.listCommits(dir).getLast(), 8, null)) {
            for (var leaf : reader.leaves()) {
                var storedFields = leaf.reader().storedFields();
                long segmentSourceBytes = 0;
                for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                    var source = storedFields.document(doc).getBinaryValue("_source");
                    if (source == null) continue;
                    segmentSourceBytes += source.length;
                    largestSource = Math.max(largestSource, source.length);
                }
                largestSegmentSourceBytes = Math.max(largestSegmentSourceBytes, segmentSourceBytes);
            }
        }
        assertTrue(largestSegmentSourceBytes > chunk, "a segment spans several chunks: " + largestSegmentSourceBytes);
        assertTrue(largestSource > 2 * chunk, "a sliced document: " + largestSource);
    }

    static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
