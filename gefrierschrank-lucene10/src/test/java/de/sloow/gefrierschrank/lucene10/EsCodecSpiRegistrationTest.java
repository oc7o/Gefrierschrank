package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Pins the failure an unregistered ES codec name causes:
 * {@code IllegalArgumentException: An SPI class of type org.apache.lucene.codecs.Codec with
 * name 'Elasticsearch900Lucene101' does not exist} when opening an ES 9 shard. Every ES
 * codec name observed in the 8.17.0/9.0.0/9.3.0 jars must resolve via Codec.forName.
 */
class EsCodecSpiRegistrationTest {

    @Test
    void allEsCodecSpiNamesResolve() {
        for (String name : new String[]{
                "Elasticsearch814",
                "Elasticsearch816",
                "Elasticsearch900",
                "Elasticsearch900Lucene101",
                "Elasticsearch92Lucene103",
                "ES93TSDBDefaultCompressionLucene103Codec",
        }) {
            Codec codec = assertDoesNotThrow(() -> Codec.forName(name), "SPI name should resolve: " + name);
            assertEquals(name, codec.getName());
        }
    }

    /**
     * The TSDB codec reads through the plain Lucene103 stored fields - "DefaultCompression" is
     * literal, it is NOT one of the ZSTD codecs. Its ES-side bloom sidecar contributes no stored
     * fields (its reader's document() is a verified no-op) and its synthetic-_id wrapper only
     * touches the _id field, so a wholesale delegate reads _source correctly.
     */
    @Test
    void tsdbCodecUsesPlainLuceneStoredFieldsNotZstd() {
        Codec codec = Codec.forName("ES93TSDBDefaultCompressionLucene103Codec");

        StoredFieldsFormat format = assertDoesNotThrow(codec::storedFieldsFormat);
        assertFalse(format instanceof EsZstdStoredFieldsFormat,
                "TSDB segments use Lucene's default stored-fields compression, not ES's ZSTD format");
        assertEquals(new Lucene103Codec().storedFieldsFormat().getClass(), format.getClass());
    }

    /** ES 9.3 stamps this postings name onto _id in TSDB segments; opening one must not fail on it. */
    @Test
    void tsdbSyntheticIdPostingsFormatResolves() {
        assertEquals("TSDBSyntheticId", PostingsFormat.forName("TSDBSyntheticId").getName());
    }
}
