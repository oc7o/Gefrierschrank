package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the failure an unregistered ES doc-values format name causes:
 * {@code An SPI class of type org.apache.lucene.codecs.DocValuesFormat with name 'ES819TSDB'
 * does not exist}, which stops TSDB shards from even opening.
 */
class EsTsdbDocValuesFormatTest {

    @Test
    void tsdbDocValuesSpiNamesResolve() {
        for (String name : new String[]{"ES87TSDB", "ES819TSDB"}) {
            assertEquals(name, DocValuesFormat.forName(name).getName(), "SPI name should resolve: " + name);
        }
    }

    /**
     * The stub reports fields as having no doc values rather than decoding ES's proprietary
     * layout. That is safe for a _source dump (stored fields only) but means doc-values content
     * is genuinely unavailable - the reason synthetic-source indices cannot be exported.
     */
    @Test
    void theStubReportsEveryFieldAsHavingNoDocValues() throws IOException {
        DocValuesFormat format = DocValuesFormat.forName("ES819TSDB");
        var producer = format.fieldsProducer(null);

        assertNull(producer.getNumeric(null));
        assertNull(producer.getBinary(null));
        assertNull(producer.getSorted(null));
        assertNull(producer.getSortedNumeric(null));
        assertNull(producer.getSortedSet(null));
        assertNull(producer.getSkipper(null));
    }

    /**
     * A document without stored _source must fail loudly, never yield an empty row: a silently
     * truncated export is far worse than a quarantined shard, since nothing downstream would
     * reveal that documents went missing.
     */
    @Test
    void aDocumentWithoutStoredSourceFailsLoudly(@org.junit.jupiter.api.io.TempDir Path tmp) throws IOException {
        Path shardDir = Files.createDirectory(tmp.resolve("shard"));
        try (Directory dir = new org.apache.lucene.store.NIOFSDirectory(shardDir);
             IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            // doc-values only, exactly the shape of a synthetic-source document
            doc.add(new NumericDocValuesField("metric", 42));
            writer.addDocument(doc);
            writer.commit();
        }

        try (Lucene10ShardReader reader = new Lucene10ShardReader(shardDir)) {
            assertEquals(1, reader.docCount());
            IOException e = assertThrows(IOException.class,
                    () -> reader.readSource(0, (bytes, length) -> { }));
            assertTrue(e.getMessage().contains("synthetic source"),
                    "error should explain the cause: " + e.getMessage());
        }
    }

    /** A normal stored-_source document still reads back unchanged. */
    @Test
    void aDocumentWithStoredSourceStillReads(@org.junit.jupiter.api.io.TempDir Path tmp) throws IOException {
        Path shardDir = Files.createDirectory(tmp.resolve("shard"));
        try (Directory dir = new org.apache.lucene.store.NIOFSDirectory(shardDir);
             IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StoredField("_source", "{\"ok\":true}".getBytes(StandardCharsets.UTF_8)));
            writer.addDocument(doc);
            writer.commit();
        }

        try (Lucene10ShardReader reader = new Lucene10ShardReader(shardDir)) {
            var holder = new String[1];
            reader.readSource(0, (bytes, length) -> holder[0] = new String(bytes, 0, length, StandardCharsets.UTF_8));
            assertNotNull(holder[0]);
            assertEquals("{\"ok\":true}", holder[0]);
        }
    }
}
