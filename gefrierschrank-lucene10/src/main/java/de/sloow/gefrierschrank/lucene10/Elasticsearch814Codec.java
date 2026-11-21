package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.backward_codecs.lucene99.Lucene99Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;

/**
 * Read-only stand-in for Elasticsearch's {@code Elasticsearch814Codec} SPI name. ES's real
 * codec delegates to {@code Lucene99Codec} for everything except stored fields, which it
 * writes with ZSTD instead of Lucene's default LZ4/DEFLATE.
 */
public final class Elasticsearch814Codec extends FilterCodec {

    public Elasticsearch814Codec() {
        super("Elasticsearch814", new Lucene99Codec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return EsZstdStoredFieldsFormat.INSTANCE;
    }
}
