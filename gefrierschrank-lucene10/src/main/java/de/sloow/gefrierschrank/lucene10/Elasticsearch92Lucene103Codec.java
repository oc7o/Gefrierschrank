package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;

/**
 * Read-only stand-in for Elasticsearch's {@code Elasticsearch92Lucene103Codec} SPI name. ES's
 * real codec delegates to {@code Lucene103Codec} for everything except stored fields, which it
 * writes with ZSTD instead of Lucene's default LZ4/DEFLATE.
 */
public final class Elasticsearch92Lucene103Codec extends FilterCodec {

    public Elasticsearch92Lucene103Codec() {
        super("Elasticsearch92Lucene103", new Lucene103Codec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return EsZstdStoredFieldsFormat.INSTANCE;
    }
}
