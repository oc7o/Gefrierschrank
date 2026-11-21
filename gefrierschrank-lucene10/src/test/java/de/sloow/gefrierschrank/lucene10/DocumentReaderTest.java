package de.sloow.gefrierschrank.lucene10;

import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end: repository fixture -> materialized shard dir -> Lucene reader -> documents.
 * Uses Lucene 10 + backward-codecs, which covers ES 7 (Lucene 8), ES 8 (Lucene 9) and ES 9 (Lucene 10) shards.
 */
class DocumentReaderTest {

    @Test
    void readsStoredFieldsFromEs8FixtureShard(@TempDir Path target) throws Exception {
        var blobs = new FileSystemBlobStore(Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo"));
        var inspector = new RepositoryInspector(blobs);
        var shard = inspector.shardSnapshot("test_data_8_17_0", 0);
        new ShardMaterializer(blobs).materialize(shard, target);

        try (Directory dir = new NIOFSDirectory(target);
             DirectoryReader reader = DirectoryReader.open(dir)) {

            assertEquals(500, reader.maxDoc(), "500 documents were indexed by the generator");
            StoredFields storedFields = reader.storedFields();
            var source = storedFields.document(0).getField("_source").binaryValue();
            assertNotNull(source, "ES stores the document as _source binary field");
            var parsed = new com.fasterxml.jackson.databind.ObjectMapper().readTree(source.bytes);
            assertTrue(parsed.get("title").asText().startsWith("Product"),
                "unexpected _source content: " + source.utf8ToString().substring(0, 80));
        }
    }

    @Test
    void readsStoredFieldsFromEs9FixtureShard(@TempDir Path target) throws Exception {
        var blobs = new FileSystemBlobStore(Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo"));
        var inspector = new RepositoryInspector(blobs);
        var shard = inspector.shardSnapshot("test_data_9_3_0", 0);
        new ShardMaterializer(blobs).materialize(shard, target);

        try (Directory dir = new NIOFSDirectory(target);
             DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(500, reader.maxDoc());
            var source = reader.storedFields().document(0).getField("_source").binaryValue();
            assertNotNull(source);
        }
    }

    /** ES 7 writes Lucene 8 segments; Lucene 10 reads them through its N-2 created-major floor. */
    @Test
    void readsEs7FixtureShardInProcess(@TempDir Path target) throws Exception {
        var blobs = new FileSystemBlobStore(Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo"));
        var shard = new RepositoryInspector(blobs).shardSnapshot("test_data_7_17_9", 0);
        assertTrue(shard.files().stream().allMatch(f -> f.writtenBy() == null || f.writtenBy().startsWith("8.")));
        new ShardMaterializer(blobs).materialize(shard, target);

        try (var reader = new Lucene10ShardReader(target)) {
            assertEquals(500, reader.docCount());
            var json = new com.fasterxml.jackson.databind.ObjectMapper();
            for (int doc = 0; doc < reader.docCount(); doc++) {
                assertTrue(reader.isLive(doc));
                reader.readSource(doc, (bytes, length) -> {
                    try {
                        assertTrue(json.readTree(bytes, 0, length).path("title").asText().startsWith("Product"));
                    } catch (java.io.IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                });
                var metadata = reader.documentMetadata(doc);
                assertEquals("stored", metadata.sourceKind());
                assertNotNull(metadata.id());
            }
        }
    }

    @Test
    void excludesHardAndSoftDeletedVersions(@TempDir Path target) throws Exception {
        try (Directory directory = new NIOFSDirectory(target);
             var writer = new org.apache.lucene.index.IndexWriter(directory,
                 new org.apache.lucene.index.IndexWriterConfig()
                     .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE)
                     .setSoftDeletesField("_soft_deletes"))) {
            for (int id = 0; id < 4; id++) {
                var doc = new org.apache.lucene.document.Document();
                doc.add(new org.apache.lucene.document.StringField("key", "" + id,
                    org.apache.lucene.document.Field.Store.NO));
                if (id != 0) doc.add(new org.apache.lucene.document.StoredField("_source",
                    ("{\"id\":" + id + "}").getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                if (id == 0 || id == 2) doc.add(new org.apache.lucene.document.NumericDocValuesField("_soft_deletes", 1));
                writer.addDocument(doc);
            }
            writer.commit();
            writer.deleteDocuments(new org.apache.lucene.index.Term("key", "1"));
            writer.commit();
        }
        try (var reader = new Lucene10ShardReader(target)) {
            assertEquals(4, reader.docCount());
            assertFalse(reader.isLive(0));
            assertFalse(reader.isLive(1));
            assertFalse(reader.isLive(2));
            assertTrue(reader.isLive(3));
            reader.readSource(3, (bytes, length) -> assertEquals("{\"id\":3}",
                new String(bytes, 0, length, java.nio.charset.StandardCharsets.UTF_8)));
        }
    }

}
