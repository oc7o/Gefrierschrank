package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;

/**
 * A segment's own codec with the five structures a stored-source export never opens (postings,
 * points, norms, term vectors, kNN vectors) replaced by {@link PrunedFormats}. Stored fields,
 * field infos, segment infos, live docs, doc values and the compound format are the delegate's,
 * so documents, deletions and doc values read exactly as with the delegate. The per-field
 * postings and vector formats are never resolved, so their files need not exist. Never registered
 * with SPI; the name is the delegate's and is only used when writing, which this codec never does.
 */
final class PrunedCodec extends FilterCodec {

    PrunedCodec(Codec delegate) {
        super(delegate.getName(), delegate);
    }

    Codec delegate() {
        return delegate;
    }

    @Override
    public PostingsFormat postingsFormat() {
        return PrunedFormats.POSTINGS;
    }

    @Override
    public PointsFormat pointsFormat() {
        return PrunedFormats.POINTS;
    }

    @Override
    public NormsFormat normsFormat() {
        return PrunedFormats.NORMS;
    }

    @Override
    public TermVectorsFormat termVectorsFormat() {
        return PrunedFormats.TERM_VECTORS;
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
        return PrunedFormats.KNN_VECTORS;
    }
}
