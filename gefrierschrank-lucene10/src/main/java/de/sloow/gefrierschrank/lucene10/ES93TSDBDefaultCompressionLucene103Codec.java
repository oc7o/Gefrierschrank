package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;

/**
 * Read-only stand-in for Elasticsearch's {@code ES93TSDBDefaultCompressionLucene103Codec}, used
 * by TSDB (time-series data stream) indices in ES 9.3+. Note this is the one ES codec whose SPI
 * name keeps its {@code Codec} suffix, unlike the other five in this package.
 *
 * <p>Unlike its siblings here it does <em>not</em> use ZSTD: "DefaultCompression" is literal, and
 * ES builds its stored fields from the plain {@code Lucene103Codec}'s format (verified via javap
 * on {@code TSDBCodecWithSyntheticId}, which passes {@code delegate.storedFieldsFormat()} through).
 *
 * <p>ES wraps that in a {@code TSDBStoredFieldsFormat} pairing it with an
 * {@code ES93BloomFilterStoredFieldsFormat} sidecar over {@code _id}, and wraps the whole codec in
 * {@code TSDBSyntheticIdCodec}. Neither affects reading {@code _source}:
 * <ul>
 *   <li>the bloom sidecar's reader implements {@code document(int, StoredFieldVisitor)} as a bare
 *       {@code return;} - a verified no-op that contributes no stored fields; it exists only to
 *       serve {@code mayContainTerm} for {@code _id} lookups, which this tool never performs, so
 *       the {@code .bfi}/{@code .bfm} sidecar files can be ignored entirely;</li>
 *   <li>{@code TSDBSyntheticIdCodec} only rewrites the {@code _id} field's info to carry the
 *       {@code TSDBSyntheticId} postings format name (handled by
 *       {@link TsdbSyntheticIdPostingsFormat}); it leaves every other field untouched.</li>
 * </ul>
 * So delegating wholesale to {@code Lucene103Codec} reads these segments' documents correctly.
 */
public final class ES93TSDBDefaultCompressionLucene103Codec extends FilterCodec {

    public ES93TSDBDefaultCompressionLucene103Codec() {
        super("ES93TSDBDefaultCompressionLucene103Codec", new Lucene103Codec());
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return delegate.storedFieldsFormat();
    }
}
