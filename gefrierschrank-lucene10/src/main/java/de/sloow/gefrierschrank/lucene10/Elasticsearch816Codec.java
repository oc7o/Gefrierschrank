package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.backward_codecs.lucene912.Lucene912Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;

/**
 * Read-only stand-in for Elasticsearch's {@code Elasticsearch816Codec} SPI name. ES's real
 * codec delegates to {@code Lucene912Codec} for everything except stored fields, which it
 * writes with ZSTD instead of Lucene's default LZ4/DEFLATE.
 */
public final class Elasticsearch816Codec extends FilterCodec {

    public Elasticsearch816Codec() {
        super("Elasticsearch816", new Lucene912Codec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return EsZstdStoredFieldsFormat.INSTANCE;
    }
}
