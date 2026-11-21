package de.sloow.gefrierschrank.syntheticsource.isolated;

import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequentialStoredFieldsDirectoryReaderTest {

    /** Lucene 10.3's two stored-fields modes: LZ4 80 KB chunks and DEFLATE 480 KB chunks with sub-blocks. */
    @ParameterizedTest
    @EnumSource(Lucene103Codec.Mode.class)
    void everyLeafReadsThroughItsOwnMergeInstanceWithIdenticalResults(Lucene103Codec.Mode mode, @TempDir Path tmp)
            throws Exception {
        try (Directory directory = new NIOFSDirectory(tmp)) {
            writeSegments(directory, mode);
            try (DirectoryReader reader = productionWrap(directory)) {
                assertTrue(reader.leaves().stream().filter(l -> l.reader().maxDoc() > 10).count() >= 3,
                    "expected at least 3 segments of more than 10 docs");
                assertTrue(reader.numDeletedDocs() > 0, "expected hard deletes");
                for (LeafReaderContext context : reader.leaves()) {
                    var leaf = assertInstanceOf(SequentialStoredFieldsLeafReader.class, context.reader());
                    LeafReader delegate = ((SequentialStoredFieldsDirectoryReader.Leaf) leaf).getDelegate();
                    var codecReader = assertInstanceOf(CodecReader.class, delegate);
                    assertSame(delegate.getCoreCacheHelper(), leaf.getCoreCacheHelper());
                    assertSame(delegate.getReaderCacheHelper(), leaf.getReaderCacheHelper());
                    assertNotSame(codecReader.getFieldsReader(), leaf.getSequentialStoredFieldsReader());
                    assertSameStoredFieldsInEveryOrder(leaf);
                }
            }
        }
    }

    /**
     * ES 8.17 best_compression segments (zstd): the zstd wrapper must sit inside the sequential
     * one, or the merge instance would come from ES's own decompressor, which needs ES native access.
     */
    @Test
    void zstdSegmentsReadTheirMergeInstanceThroughTheZstdWrapper(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(Path.of("src/test/resources/fixtures/zstd-synth-repo"));
        var inspector = new RepositoryInspector(store);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot("test_zstd_logsdb", 0), shardDir);
        try (Directory directory = new NIOFSDirectory(shardDir); DirectoryReader reader = productionWrap(directory)) {
            for (LeafReaderContext context : reader.leaves()) {
                var leaf = (SequentialStoredFieldsDirectoryReader.Leaf) context.reader();
                assertEquals(JniZstdStoredFieldsReader.class.getName() + "$Leaf", leaf.getDelegate().getClass().getName());
                assertSameStoredFieldsInEveryOrder(leaf);

                Set<String> fields = new HashSet<>();
                for (IndexableField field : leaf.storedFields().document(0)) {
                    fields.add(field.name());
                }
                fields.removeAll(Set.of("_id", "_routing"));
                assertTrue(fields.size() > 0, "fixture docs carry stored fields besides _id: " + fields);
                LeafStoredFieldLoader sequential = StoredFieldLoader.create(false, fields, true).getLoader(context, null);
                for (int doc = 0; doc < leaf.maxDoc(); doc++) {
                    LeafStoredFieldLoader perDoc = StoredFieldLoader.create(false, fields).getLoader(context, new int[] {doc});
                    perDoc.advanceTo(doc);
                    sequential.advanceTo(doc);
                    assertEquals(perDoc.id(), sequential.id());
                    assertEquals(perDoc.routing(), sequential.routing());
                    assertEquals(perDoc.storedFields(), sequential.storedFields(), "doc " + doc);
                    assertTrue(!sequential.storedFields().isEmpty());
                }
            }
        }
    }

    @Test
    void closingTheWrapperReleasesTheInnerReaders(@TempDir Path tmp) throws Exception {
        try (Directory directory = new NIOFSDirectory(tmp)) {
            writeSegments(directory, Lucene103Codec.Mode.BEST_SPEED);
            DirectoryReader raw = DirectoryReader.open(directory);
            DirectoryReader zstd = JniZstdStoredFieldsReader.wrap(raw);
            DirectoryReader wrapped = SequentialStoredFieldsDirectoryReader.wrap(zstd);
            assertSame(zstd, ((SequentialStoredFieldsDirectoryReader) wrapped).getDelegate());
            wrapped.close();
            assertEquals(0, raw.getRefCount());
            assertEquals(0, zstd.getRefCount());
        }
    }

    private static DirectoryReader productionWrap(Directory directory) throws IOException {
        return SequentialStoredFieldsDirectoryReader.wrap(JniZstdStoredFieldsReader.wrap(DirectoryReader.open(directory)));
    }

    /** Four segments of 40 docs with hard deletes, soft deletes and updates, never merged. */
    private static void writeSegments(Directory directory, Lucene103Codec.Mode mode) throws IOException {
        var config = new IndexWriterConfig()
            .setCodec(new Lucene103Codec(mode))
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setSoftDeletesField("__soft_deletes");
        Random random = new Random(42);
        try (var writer = new IndexWriter(directory, config)) {
            int id = 0;
            for (int segment = 0; segment < 4; segment++) {
                for (int i = 0; i < 40; i++, id++) {
                    writer.addDocument(document(id, random));
                }
                writer.commit();
            }
            for (int doc = 5; doc < id; doc += 17) {
                writer.deleteDocuments(new Term("id", Integer.toString(doc)));
            }
            for (int doc = 9; doc < id; doc += 23) {
                writer.softUpdateDocument(new Term("id", Integer.toString(doc)), document(doc, random),
                    new NumericDocValuesField("__soft_deletes", 1));
            }
            writer.commit();
        }
    }

    private static Document document(int id, Random random) {
        Document document = new Document();
        document.add(new StringField("id", Integer.toString(id), Field.Store.NO));
        document.add(new StoredField("_id", new BytesRef(new byte[] {(byte) 0xfe, (byte) id, (byte) (id >> 8)})));
        StringBuilder text = new StringBuilder();
        for (int i = 0, n = 20 + random.nextInt(400); i < n; i++) {
            text.append("word").append(random.nextInt(1000)).append(' ');
        }
        document.add(new StoredField("message", text.toString()));
        document.add(new StoredField("bytes", random.nextLong()));
        if (id % 3 == 0) {
            document.add(new StoredField("_routing", "r" + id));
        }
        return document;
    }

    /** Ascending, descending, stride-3 and shuffled reads through one merge instance each. */
    private static void assertSameStoredFieldsInEveryOrder(SequentialStoredFieldsLeafReader leaf) throws IOException {
        int maxDoc = leaf.maxDoc();
        List<List<String>> expected = new ArrayList<>();
        var random = leaf.storedFields();
        for (int doc = 0; doc < maxDoc; doc++) {
            expected.add(fields(random.document(doc)));
        }
        List<Integer> shuffled = new ArrayList<>(IntStream.range(0, maxDoc).boxed().toList());
        Collections.shuffle(shuffled, new Random(7));
        List<Integer> stride = new ArrayList<>();
        for (int start = 0; start < 3; start++) {
            for (int doc = start; doc < maxDoc; doc += 3) {
                stride.add(doc);
            }
        }
        Map<String, List<Integer>> orders = Map.of(
            "ascending", IntStream.range(0, maxDoc).boxed().toList(),
            "descending", IntStream.range(0, maxDoc).map(i -> maxDoc - 1 - i).boxed().toList(),
            "stride-3", stride,
            "shuffled", shuffled);
        for (var order : orders.entrySet()) {
            StoredFieldsReader sequential = leaf.getSequentialStoredFieldsReader();
            for (int doc : order.getValue()) {
                var visitor = new DocumentStoredFieldVisitor();
                sequential.document(doc, visitor);
                assertEquals(expected.get(doc), fields(visitor.getDocument()), order.getKey() + " doc " + doc);
            }
        }
    }

    private static List<String> fields(Document document) {
        List<String> fields = new ArrayList<>();
        for (IndexableField field : document) {
            BytesRef binary = field.binaryValue();
            Object value = binary != null ? BytesRef.deepCopyOf(binary) : field.numericValue() != null
                ? field.numericValue() : field.stringValue();
            fields.add(field.name() + "=" + value);
        }
        return fields;
    }
}
