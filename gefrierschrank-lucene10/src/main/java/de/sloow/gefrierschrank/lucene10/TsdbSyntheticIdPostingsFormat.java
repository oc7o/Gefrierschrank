package de.sloow.gefrierschrank.lucene10;

/**
 * Read-only stub for the {@code TSDBSyntheticId} postings SPI name, which Elasticsearch 9.3+
 * stamps onto the {@code _id} field of TSDB (time-series data stream) segments via
 * {@code TSDBSyntheticIdCodec}'s field-infos rewrite. In those segments {@code _id} isn't stored
 * on disk at all - it's synthesized from the time-series dimensions at read time - so there are
 * no real postings behind this name. A bulk {@code _source} dump never resolves {@code _id}
 * terms, so an empty producer is sufficient, exactly as for the other ES postings stubs here.
 */
public final class TsdbSyntheticIdPostingsFormat extends EsPostingsStubFormat {
    public TsdbSyntheticIdPostingsFormat() {
        super("TSDBSyntheticId");
    }
}
