package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.backward_codecs.lucene100.Lucene100Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;

/**
 * Read-only stand-in for Elasticsearch's {@code Elasticsearch900Codec} SPI name. ES's real
 * codec delegates to {@code Lucene100Codec} for everything except stored fields, which it
 * writes with ZSTD instead of Lucene's default LZ4/DEFLATE.
 */
public final class Elasticsearch900Codec extends FilterCodec {

    public Elasticsearch900Codec() {
        super("Elasticsearch900", new Lucene100Codec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return EsZstdStoredFieldsFormat.INSTANCE;
    }
}
