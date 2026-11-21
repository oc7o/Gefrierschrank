package de.sloow.gefrierschrank.lucene10;

import de.sloow.gefrierschrank.core.lucene.DocumentId;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An index created by ES 7 (index-created Lucene major 8) whose segments were all rewritten by
 * Lucene 9 carries only 9.x written_by versions, and Lucene 10's default open rejects it.
 * Lucene10ShardReader opens the latest commit with a created-major floor of 8.
 *
 * <p>The fixture was generated with Lucene 9 by {@code Es7RewrittenFixtureGenerator}, a test of
 * the gefrierschrank-lucene9 module that was removed with it in 1.2.0 (see v1.1.0).
 */
class Lucene10ShardReaderCreatedMajor8Test {

    static final Path FIXTURE = Path.of("src/test/resources/fixtures/es7-created-lucene9-rewritten");
    static final Path DEMO_REPO = Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");

    @Test
    void fixtureIsEs7CreatedAndFullyRewrittenByLucene9() throws IOException {
        try (var dir = new NIOFSDirectory(FIXTURE)) {
            var infos = SegmentInfos.readLatestCommit(dir, 0);
            assertEquals(8, infos.getIndexCreatedVersionMajor());
            assertTrue(infos.size() > 0);
            for (var segment : infos) assertEquals(9, segment.info.getVersion().major, segment.toString());
        }
    }

    @Test
    void defaultOpenRejectsIt() throws IOException {
        try (var dir = new NIOFSDirectory(FIXTURE)) {
            assertThrows(IndexFormatTooOldException.class, () -> DirectoryReader.open(dir).close());
        }
    }

    @Test
    void readsEveryLiveDocumentLikeTheOriginalEs7Shard(@TempDir Path tmp) throws Exception {
        Path original = tmp.resolve("original");
        var blobs = new FileSystemBlobStore(DEMO_REPO);
        new ShardMaterializer(blobs).materialize(new RepositoryInspector(blobs).shardSnapshot("test_data_7_17_9", 0), original);
        Map<String, Stored> expected = readWithCreatedMajorFloor8(original);
        assertEquals(500, expected.size());

        Path rewritten = copy(FIXTURE, tmp.resolve("rewritten"));
        var seen = new HashMap<String, Stored>();
        try (var reader = new Lucene10ShardReader(rewritten)) {
            for (int doc = 0; doc < reader.docCount(); doc++) {
                if (!reader.isLive(doc)) continue;
                byte[][] source = new byte[1][];
                reader.readSource(doc, (bytes, length) -> source[0] = Arrays.copyOf(bytes, length));
                DocumentMetadata metadata = reader.documentMetadata(doc);
                assertEquals("stored", metadata.sourceKind());
                assertEquals(doc, metadata.slot());
                assertFalse(seen.containsKey(metadata.id()), "duplicate _id " + metadata.id());
                seen.put(metadata.id(), new Stored(metadata.routing(), source[0]));
            }
        }
        assertEquals(498, seen.size(), "the hard-deleted and the soft-deleted document are excluded");
        assertFalse(seen.containsKey("7"), "soft-deleted _id 7");
        assertFalse(seen.containsKey("13"), "hard-deleted _id 13");
        expected.keySet().removeAll(java.util.Set.of("7", "13"));
        assertEquals(expected.keySet(), seen.keySet());
        for (var entry : expected.entrySet()) {
            var actual = seen.get(entry.getKey());
            assertEquals(entry.getValue().routing(), actual.routing(), "routing of " + entry.getKey());
            assertArrayEquals(entry.getValue().source(), actual.source(), "source bytes of " + entry.getKey());
        }
    }

    /**
     * An ES 6-created index fully rewritten by ES 7 (created major 7, Lucene 8 segments) stays beyond
     * the N-2 floor and must fail loudly, not partially.
     */
    @Test
    void lucene7CreatedCommitStillFailsLoudly(@TempDir Path tmp) throws Exception {
        es6CreatedCommit(tmp, false);
        try (var dir = new NIOFSDirectory(tmp)) {
            assertEquals(7, SegmentInfos.readLatestCommit(dir, 0).getIndexCreatedVersionMajor());
        }
        assertThrows(IndexFormatTooOldException.class, () -> new Lucene10ShardReader(tmp).close());
    }

    /**
     * An ES 6-created index that still holds Lucene 7 segments, as in ES 7 snapshots of never-merged
     * ES 6 indices. listCommits resolves segment codecs before any version check and would blame a
     * missing backward-codecs jar; the reader must report the index as too old, like the default open.
     */
    @Test
    void lucene7CreatedCommitWithLucene7SegmentsFailsAsTooOld(@TempDir Path tmp) throws Exception {
        es6CreatedCommit(tmp, true);
        try (var dir = new NIOFSDirectory(tmp)) {
            assertThrows(IndexFormatTooOldException.class, () -> DirectoryReader.open(dir).close());
            var listed = assertThrows(IllegalArgumentException.class, () -> DirectoryReader.listCommits(dir));
            assertTrue(listed.getMessage().contains("Lucene70"), listed.getMessage());
        }
        assertThrows(IndexFormatTooOldException.class, () -> new Lucene10ShardReader(tmp).close());
    }

    /** test_data_7_17_9 shard 0 with its commit patched to created major 7 and, optionally, Lucene70 segments. */
    private static void es6CreatedCommit(Path shardDir, boolean lucene7Segments) throws Exception {
        var blobs = new FileSystemBlobStore(DEMO_REPO);
        new ShardMaterializer(blobs).materialize(new RepositoryInspector(blobs).shardSnapshot("test_data_7_17_9", 0), shardDir);
        Path segments;
        try (var files = Files.list(shardDir)) {
            segments = files.filter(f -> f.getFileName().toString().startsWith("segments_")).findFirst().orElseThrow();
        }
        byte[] bytes = Files.readAllBytes(segments);
        int pos = 4;                              // codec magic
        pos += 1 + bytes[pos];                    // codec name
        pos += 4 + 16;                            // format version, segment id
        pos += 1 + bytes[pos];                    // suffix
        pos += 3;                                 // Lucene version that wrote the commit (8.11.1, one byte each)
        assertEquals(8, bytes[pos], "index-created major");
        bytes[pos] = 7;
        if (lucene7Segments) {
            byte[] from = "Lucene87".getBytes(StandardCharsets.US_ASCII);
            byte[] to = "Lucene70".getBytes(StandardCharsets.US_ASCII);
            int renamed = 0;
            for (int i = 0; i + from.length <= bytes.length - 16; i++) {
                if (Arrays.equals(bytes, i, i + from.length, from, 0, from.length)) {
                    System.arraycopy(to, 0, bytes, i, to.length);
                    renamed++;
                }
            }
            assertTrue(renamed > 0, "segment codec names");
        }
        var crc = new java.util.zip.CRC32();
        crc.update(bytes, 0, bytes.length - 8);
        java.nio.ByteBuffer.wrap(bytes, bytes.length - 8, 8).putLong(crc.getValue());
        Files.write(segments, bytes);
    }

    @Test
    void opensLatestCommitLikeDefaultOpen(@TempDir Path tmp) throws Exception {
        try (var dir = new NIOFSDirectory(tmp);
             var writer = new IndexWriter(dir, new IndexWriterConfig()
                 .setMergePolicy(NoMergePolicy.INSTANCE)
                 .setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE))) {
            int n = 0;
            for (int commit = 0; commit < 3; commit++) {
                for (int i = 0; i < 5 + commit; i++, n++) {
                    var doc = new Document();
                    doc.add(new StoredField("_id", new BytesRef(Lucene10ShardReaderEquivalenceTest.utf8Id("doc-" + n))));
                    doc.add(new StoredField("_source", new BytesRef("{\"n\":" + n + "}")));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
        try (var dir = new NIOFSDirectory(tmp); var defaultOpen = DirectoryReader.open(dir);
             var reader = new Lucene10ShardReader(tmp)) {
            assertEquals(3, DirectoryReader.listCommits(dir).size(), "older commits are kept");
            assertEquals(3, defaultOpen.leaves().size());
            assertEquals(defaultOpen.maxDoc(), reader.docCount());
            var storedFields = defaultOpen.storedFields();
            for (int doc = 0; doc < defaultOpen.maxDoc(); doc++) {
                var expected = storedFields.document(doc);
                var id = expected.getBinaryValue("_id");
                assertEquals(DocumentId.decode(id.bytes, id.offset, id.length), reader.documentMetadata(doc).id());
                var sourceRef = expected.getBinaryValue("_source");
                byte[] expectedSource = Arrays.copyOfRange(sourceRef.bytes, sourceRef.offset, sourceRef.offset + sourceRef.length);
                reader.readSource(doc, (bytes, length) -> assertArrayEquals(expectedSource, Arrays.copyOf(bytes, length)));
            }
        }
    }

    record Stored(String routing, byte[] source) {}

    static Map<String, Stored> readWithCreatedMajorFloor8(Path shardDir) throws IOException {
        var result = new HashMap<String, Stored>();
        try (var dir = new NIOFSDirectory(shardDir)) {
            var commits = DirectoryReader.listCommits(dir);
            try (var reader = DirectoryReader.open(commits.get(commits.size() - 1), 8, null)) {
                var storedFields = reader.storedFields();
                for (int doc = 0; doc < reader.maxDoc(); doc++) {
                    var document = storedFields.document(doc);
                    var id = document.getBinaryValue("_id");
                    var source = document.getBinaryValue("_source");
                    assertNotNull(source);
                    result.put(DocumentId.decode(id.bytes, id.offset, id.length), new Stored(document.get("_routing"),
                        Arrays.copyOfRange(source.bytes, source.offset, source.offset + source.length)));
                }
            }
        }
        return result;
    }

    static Path copy(Path from, Path to) throws IOException {
        Files.createDirectories(to);
        try (var files = Files.list(from)) {
            for (Path file : files.toList()) Files.copy(file, to.resolve(file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
        }
        return to;
    }
}
