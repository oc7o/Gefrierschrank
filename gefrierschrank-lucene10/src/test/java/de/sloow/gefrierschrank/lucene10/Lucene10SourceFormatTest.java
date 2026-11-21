package de.sloow.gefrierschrank.lucene10;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.source.SourceConversionException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Stored {@code _source} in SMILE, CBOR and YAML is handed out as its JSON rendering with the kind
 * of its format, while JSON keeps the byte path: the reader's own array, kind {@code stored}.
 */
class Lucene10SourceFormatTest {

    private static final String JSON_TEXT = "{\"n\":1,\"s\":\"a\"}";
    private static final String CONVERTED = "{\"n\":1,\"s\":\"a\"}";
    private static final String MARKER = "SECRET-DOCUMENT-TEXT";

    @TempDir
    static Path shardDir;
    private static byte[] smileBytes;

    @BeforeAll
    static void writeShard() throws IOException {
        var tree = new ObjectMapper().readTree(JSON_TEXT);
        smileBytes = new ObjectMapper(SmileFactory.builder().build()).writeValueAsBytes(tree);
        byte[] cbor = new ObjectMapper(CBORFactory.builder().build()).writeValueAsBytes(tree);
        byte[] yaml = "---\nn: 1\ns: a\n".getBytes(StandardCharsets.UTF_8);
        byte[] corruptSmile = Arrays.copyOf(new ObjectMapper(SmileFactory.builder().build())
            .writeValueAsBytes(new ObjectMapper().createObjectNode().put("secret", MARKER)), 12);
        try (var dir = new NIOFSDirectory(shardDir);
             var writer = new IndexWriter(dir, new IndexWriterConfig().setUseCompoundFile(false))) {
            writer.addDocument(doc("json-0", JSON_TEXT.getBytes(StandardCharsets.UTF_8)));
            writer.addDocument(doc("smile-1", smileBytes));
            writer.addDocument(doc("cbor-2", cbor));
            writer.addDocument(doc("yaml-3", yaml));
            writer.addDocument(doc("json-4", JSON_TEXT.getBytes(StandardCharsets.UTF_8)));
            writer.addDocument(doc("corrupt-5", corruptSmile));
            writer.addDocument(doc("unknown-6", new byte[] {0x00, 0x01, 0x02}));
            var noSource = new Document();
            noSource.add(new StoredField("_id", "synthetic-7"));
            writer.addDocument(noSource);
            writer.addDocument(doc("smile-8", smileBytes));
            writer.forceMerge(1);
            writer.commit();
        }
    }

    private static Document doc(String id, byte[] source) {
        var doc = new Document();
        doc.add(new StoredField("_id", id));
        doc.add(new StoredField("_source", source));
        return doc;
    }

    private record Handed(byte[] bytes, int length) {
        String text() {
            return new String(bytes, 0, length, StandardCharsets.UTF_8);
        }
    }

    private static Handed read(ShardReader reader, int doc) throws IOException {
        var handed = new Handed[1];
        reader.readSource(doc, (bytes, length) -> handed[0] = new Handed(bytes, length));
        assertNotNull(handed[0]);
        return handed[0];
    }

    /** The visitor's array for the visited document, before readSource hands it out. */
    private static byte[] visitedArray(Lucene10ShardReader reader) throws ReflectiveOperationException {
        var field = Lucene10ShardReader.class.getDeclaredField("visitor");
        field.setAccessible(true);
        return ((SourceIdentityVisitor) field.get(reader)).source;
    }

    @Test
    void jsonKeepsTheReadersOwnArray() throws Exception {
        try (var reader = new Lucene10ShardReader(shardDir)) {
            assertEquals("stored", reader.documentMetadata(0).sourceKind());
            byte[] stored = visitedArray(reader);
            var handed = read(reader, 0);
            assertSame(stored, handed.bytes());
            assertEquals(stored.length, handed.length());
            assertEquals(JSON_TEXT, handed.text());
        }
    }

    @Test
    void convertedDocumentsInProductionOrder() throws Exception {
        try (var reader = new Lucene10ShardReader(shardDir)) {
            assertConverted(reader, 1, "smile-1", "stored-smile", true);
            assertConverted(reader, 2, "cbor-2", "stored-cbor", true);
            assertConverted(reader, 3, "yaml-3", "stored-yaml", true);
            assertEquals(JSON_TEXT, read(reader, 4).text());
            assertEquals("stored", reader.documentMetadata(4).sourceKind(), "the next JSON document is stored again");
        }
    }

    @Test
    void metadataBeforeTheSourceReportsTheSameKind() throws Exception {
        try (var reader = new Lucene10ShardReader(shardDir)) {
            assertConverted(reader, 3, "yaml-3", "stored-yaml", false);
            assertConverted(reader, 1, "smile-1", "stored-smile", false);
            assertConverted(reader, 2, "cbor-2", "stored-cbor", false);
            assertEquals("stored", reader.documentMetadata(0).sourceKind());
            // Reading the same document twice converts it twice, into separate arrays.
            var first = read(reader, 8);
            var second = read(reader, 8);
            assertEquals(CONVERTED, first.text());
            assertEquals(CONVERTED, second.text());
            assertFalse(first.bytes() == second.bytes());
            assertEquals("stored-smile", reader.documentMetadata(8).sourceKind());
        }
    }

    private static void assertConverted(Lucene10ShardReader reader, int doc, String id, String kind, boolean sourceFirst)
            throws IOException {
        Handed handed;
        if (sourceFirst) {
            handed = read(reader, doc);
        } else {
            assertEquals(kind, reader.documentMetadata(doc).sourceKind());
            handed = read(reader, doc);
        }
        assertEquals(CONVERTED, handed.text());
        var metadata = reader.documentMetadata(doc);
        assertEquals(kind, metadata.sourceKind());
        assertEquals(id, metadata.id());
        assertEquals(doc, metadata.slot());
    }

    @Test
    void forksConvert() throws Exception {
        try (var reader = new Lucene10ShardReader(shardDir);
             var fork = reader.fork()) {
            assertEquals(CONVERTED, read(fork, 2).text());
            assertEquals("stored-cbor", fork.documentMetadata(2).sourceKind());
            assertEquals(JSON_TEXT, read(fork, 0).text());
            assertEquals("stored", fork.documentMetadata(0).sourceKind());
            assertEquals(CONVERTED, read(reader, 1).text());
            assertEquals("stored-smile", reader.documentMetadata(1).sourceKind());
        }
    }

    @Test
    void corruptSmileFailsWithoutItsText() throws Exception {
        try (var reader = new Lucene10ShardReader(shardDir)) {
            var failure = assertThrows(SourceConversionException.class, () -> read(reader, 5));
            assertFalse(failure.getMessage().contains(MARKER));
            assertFalse(failure.getMessage().contains("synthetic source"));
            assertTrue(failure.getMessage().startsWith("stored _source in SMILE format"));
            assertEquals("stored-smile", reader.documentMetadata(5).sourceKind());
            assertEquals(JSON_TEXT, read(reader, 4).text(), "the reader goes on");
        }
    }

    @Test
    void unknownContentIsHandedOutAsStored() throws Exception {
        try (var reader = new Lucene10ShardReader(shardDir)) {
            assertArrayEquals(new byte[] {0x00, 0x01, 0x02}, read(reader, 6).bytes());
            assertEquals("stored", reader.documentMetadata(6).sourceKind());
        }
    }

    @Test
    void documentsWithoutSourceStaySynthetic() throws Exception {
        try (var reader = new Lucene10ShardReader(shardDir)) {
            read(reader, 1);
            assertEquals("synthetic", reader.documentMetadata(7).sourceKind());
            var failure = assertThrows(IOException.class, () -> read(reader, 7));
            assertTrue(failure.getMessage().contains("synthetic source"));
        }
    }
}
