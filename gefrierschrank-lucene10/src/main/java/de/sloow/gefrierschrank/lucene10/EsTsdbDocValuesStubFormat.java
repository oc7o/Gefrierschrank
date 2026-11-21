package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

/**
 * Read-only stub for Elasticsearch's proprietary TSDB doc-values SPI names
 * ({@code ES87TSDB}, {@code ES819TSDB}), which ES uses for time-series indices. It exists so a
 * TSDB segment can be opened at all; it reports every field as having no doc values rather than
 * decoding ES's custom on-disk layout.
 *
 * <p><strong>Scope, and an important caveat.</strong> A {@code _source} dump reads stored fields
 * only, never doc values, so for an index that stores {@code _source} normally this stub costs
 * nothing. But TSDB indices frequently enable <em>synthetic source</em>, where {@code _source} is
 * not stored at all and Elasticsearch reconstructs it at query time <em>from doc values</em>. In
 * that case the documents genuinely are not present as stored fields, and this stub cannot
 * conjure them: {@link Lucene10ShardReader} reports such a shard as having no readable
 * {@code _source} instead of silently emitting empty rows. Fully supporting those indices means
 * reimplementing both ES's doc-values format and its synthetic-source reconstruction, which is a
 * substantially larger undertaking than these read-only stubs.
 */
public class EsTsdbDocValuesStubFormat extends DocValuesFormat {

    public EsTsdbDocValuesStubFormat(String spiName) {
        super(spiName);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) {
        throw new UnsupportedOperationException("read-only format");
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) {
        return EmptyDocValuesProducer.INSTANCE;
    }
}
