package de.sloow.gefrierschrank.lucene10;

import de.sloow.gefrierschrank.core.lucene.DocumentId;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Per-segment "no stored _source" routing: {@code storedSourceAbsent} is decided from FieldInfos
 * only, so it is true exactly for segments that have no _source field at all, while a segment that
 * has the field but lacks it on some documents still goes through readSource's per-doc guard.
 */
class SyntheticSegmentRoutingTest {

    @Test
    void segmentWithoutSourceFieldIsFlaggedAndMetadataMatchesTheLegacyFormula(@TempDir Path tmp) throws Exception {
        assertRouting(tmp, new Lucene103Codec());
    }

    @Test
    void sameOnZstdStoredFields(@TempDir Path tmp) throws Exception {
        assertRouting(tmp, new Lucene10ShardReaderEquivalenceTest.WritableEsZstdCodec(245760, 2048, 3));
    }

    /** Real ES synthetic-source shards (plain, TSDB, ZSTD logsdb) store no _source field in any segment. */
    @Test
    void elasticsearchSyntheticSourceShardsAreFlaggedEntirely(@TempDir Path tmp) throws Exception {
        var shards = Lucene10ShardReaderEquivalenceTest.luceneNineAndTenFixtureShards().stream()
            .filter(shard -> shard.repo().endsWith("synth-repo") || shard.repo().endsWith("zstd-synth-repo")).toList();
        assertEquals(6, shards.size(), shards.toString());
        for (var shard : shards) {
            Path dir = Lucene10ShardReaderEquivalenceTest.materialize(shard, tmp.resolve(shard.index()));
            try (var reader = new Lucene10ShardReader(dir)) {
                for (int doc = 0; doc < reader.docCount(); doc++) {
                    assertTrue(reader.storedSourceAbsent(doc), shard + " doc " + doc);
                    assertEquals("synthetic", reader.documentMetadata(doc).sourceKind());
                }
            }
        }
    }

    private static void assertRouting(Path dir, Codec codec) throws Exception {
        int[] segmentSizes = {12, 9, 10};
        try (var directory = new NIOFSDirectory(dir);
             var writer = new IndexWriter(directory, new IndexWriterConfig().setCodec(codec)
                 .setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int i = 0; i < segmentSizes[0]; i++) writer.addDocument(storedSourceDoc(i));
            writer.commit();
            for (int i = 0; i < segmentSizes[1]; i++) writer.addDocument(syntheticDoc(100 + i));
            writer.commit();
            for (int i = 0; i < segmentSizes[2]; i++) writer.addDocument(i % 3 == 0 ? syntheticDoc(200 + i) : storedSourceDoc(200 + i));
            writer.commit();
        }

        try (var legacy = DirectoryReader.open(new NIOFSDirectory(dir)); var reader = new Lucene10ShardReader(dir)) {
            assertEquals(3, legacy.leaves().size());
            var storedFields = legacy.storedFields();
            for (int doc = 0; doc < reader.docCount(); doc++) {
                int segment = doc < segmentSizes[0] ? 0 : doc < segmentSizes[0] + segmentSizes[1] ? 1 : 2;
                assertEquals(segment == 1, reader.storedSourceAbsent(doc), "storedSourceAbsent of doc " + doc);

                var document = storedFields.document(doc);
                var id = document.getBinaryValue("_id");
                var expected = new DocumentMetadata(
                    id == null ? document.get("_id") : DocumentId.decode(id.bytes, id.offset, id.length),
                    document.get("_routing"), document.getField("_source") == null ? "synthetic" : "stored", doc);
                assertEquals(expected, reader.documentMetadata(doc), "metadata of doc " + doc);
                if (document.getField("_source") == null) {
                    int docId = doc;
                    var failure = assertThrows(IOException.class, () -> reader.readSource(docId, (bytes, length) -> {}));
                    assertTrue(failure.getMessage().contains("synthetic source"), failure.getMessage());
                    assertEquals(expected, reader.documentMetadata(doc), "metadata after the synthetic-source failure");
                }
            }
            assertThrows(IllegalArgumentException.class, () -> reader.storedSourceAbsent(-1));
            assertThrows(IllegalArgumentException.class, () -> reader.storedSourceAbsent(reader.docCount()));
        }
    }

    private static Document storedSourceDoc(int n) {
        var doc = identity(n);
        doc.add(new StoredField("_source", new BytesRef("{\"n\":" + n + "}")));
        return doc;
    }

    /** Synthetic-source shaped: ids, optional routing and _ignored_source, but no _source. */
    private static Document syntheticDoc(int n) {
        var doc = identity(n);
        doc.add(new StoredField("_ignored_source", new BytesRef(new byte[]{1, (byte) n})));
        return doc;
    }

    private static Document identity(int n) {
        var doc = new Document();
        doc.add(new StringField("key", "k" + n, Field.Store.NO));
        doc.add(new NumericDocValuesField("_primary_term", 1));
        switch (n % 4) {
            case 0 -> doc.add(new StoredField("_id", new BytesRef(Lucene10ShardReaderEquivalenceTest.packedNumericId(Integer.toString(n + 1)))));
            case 1 -> doc.add(new StoredField("_id", "plain-" + n));
            case 2 -> {
                doc.add(new StoredField("_id", "string-first-" + n));
                doc.add(new StoredField("_id", new BytesRef(Lucene10ShardReaderEquivalenceTest.utf8Id("binary-" + n))));
            }
            default -> doc.add(new StoredField("_id", new BytesRef(new byte[]{0x11, 0x22, (byte) n})));
        }
        if (n % 2 == 0) doc.add(new StoredField("_routing", "r" + (n % 5)));
        return doc;
    }
}
