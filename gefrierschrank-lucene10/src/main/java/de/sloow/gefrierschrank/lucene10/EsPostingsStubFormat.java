package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Read-only no-op postings format satisfying Elasticsearch-specific postings SPI
 * names ({@code ES87BloomFilter}, {@code ES812Postings}, ...) during segment open.
 * Their postings only serve term lookups on the {@code _id} field; a bulk dump
 * reads stored fields and doc values, never postings, so an empty producer
 * suffices. New ES format names get a concrete stub registered in
 * META-INF/services.
 */
public class EsPostingsStubFormat extends PostingsFormat {

    private final String spiName;

    public EsPostingsStubFormat(String spiName) {
        super(spiName);
        this.spiName = spiName;
    }

    public String spiName() {
        return spiName;
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) {
        throw new UnsupportedOperationException("read-only format");
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) {
        return EmptyFieldsProducer.INSTANCE;
    }
}
