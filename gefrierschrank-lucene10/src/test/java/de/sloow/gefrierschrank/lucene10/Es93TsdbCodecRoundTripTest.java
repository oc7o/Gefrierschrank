package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reads a segment through the TSDB (time-series data stream) codec end to end: "_source comes
 * back intact" needs proving against actual fdt/fdx/fdm files, not just SPI resolution.
 *
 * <p>This writes with the production codec directly: unlike the ZSTD codecs, the TSDB codec's
 * stored fields are plain Lucene103, so it is writable as-is and no test-only compressor is
 * needed. What this cannot reproduce without Elasticsearch itself is ES's bloom-filter sidecar
 * and its synthetic-{@code _id} rewrite - those are ES-side write concerns; the read path
 * ignores the former (its document() is a no-op) and handles the latter through
 * {@link TsdbSyntheticIdPostingsFormat}, both covered separately in
 * {@link EsCodecSpiRegistrationTest}.
 */
class Es93TsdbCodecRoundTripTest {

    @Test
    void readsSourceBackThroughTheTsdbCodec() throws IOException {
        int docCount = 40;
        Codec tsdb = Codec.forName("ES93TSDBDefaultCompressionLucene103Codec");

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(tsdb))) {
                for (int i = 0; i < docCount; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("_id", "id-" + i, Field.Store.NO));
                    String source = "{\"@timestamp\":\"2026-01-01T00:00:0" + (i % 10) + "Z\",\"metric\":" + i + "}";
                    doc.add(new StoredField("_source", source.getBytes(StandardCharsets.UTF_8)));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(docCount, reader.maxDoc());
                var storedFields = reader.storedFields();
                for (int docId : new int[]{0, 17, 39}) {
                    var value = storedFields.document(docId).getField("_source").binaryValue();
                    String json = new String(value.bytes, value.offset, value.length, StandardCharsets.UTF_8);
                    assertTrue(json.contains("\"metric\":" + docId), "doc " + docId + " content: " + json);
                }
            }
        }
    }
}
