package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.backward_codecs.lucene101.Lucene101Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;

/**
 * Read-only stand-in for Elasticsearch's {@code Elasticsearch900Lucene101Codec} SPI name -
 * without it, Lucene cannot open a segment ES wrote with that codec. ES's real
 * codec delegates to {@code Lucene101Codec} for everything except stored fields, which it
 * writes with ZSTD instead of Lucene's default LZ4/DEFLATE.
 */
public final class Elasticsearch900Lucene101Codec extends FilterCodec {

    public Elasticsearch900Lucene101Codec() {
        super("Elasticsearch900Lucene101", new Lucene101Codec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return EsZstdStoredFieldsFormat.INSTANCE;
    }
}
