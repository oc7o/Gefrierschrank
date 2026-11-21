package de.sloow.gefrierschrank.lucene10;

import de.sloow.gefrierschrank.core.repo.PrunedFormatAccessException;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.UncheckedIOException;
import java.util.Iterator;

/**
 * Read-only stand-ins for the formats {@link PrunedCodec} replaces. They open no file, and every
 * read throws {@link PrunedFormatAccessException}: an empty answer ("this field has no terms")
 * would let a caller silently drop data, a throw makes the shard complete its download instead.
 * Producers are never null, because a segment reader asserts them for fields that have the
 * structure.
 */
final class PrunedFormats {

    private PrunedFormats() {}

    private static UnsupportedOperationException readOnly() {
        return new UnsupportedOperationException("read-only format");
    }

    static final PostingsFormat POSTINGS = new PostingsFormat("PrunedPostings") {
        @Override
        public FieldsConsumer fieldsConsumer(SegmentWriteState state) {
            throw readOnly();
        }

        @Override
        public FieldsProducer fieldsProducer(SegmentReadState state) {
            return new FieldsProducer() {
                @Override
                public Iterator<String> iterator() {
                    throw new UncheckedIOException(new PrunedFormatAccessException());
                }

                @Override
                public Terms terms(String field) throws PrunedFormatAccessException {
                    throw new PrunedFormatAccessException();
                }

                @Override
                public int size() {
                    return -1;
                }

                @Override
                public void checkIntegrity() {}

                @Override
                public void close() {}
            };
        }
    };

    static final NormsFormat NORMS = new NormsFormat() {
        @Override
        public NormsConsumer normsConsumer(SegmentWriteState state) {
            throw readOnly();
        }

        @Override
        public NormsProducer normsProducer(SegmentReadState state) {
            return new NormsProducer() {
                @Override
                public NumericDocValues getNorms(FieldInfo field) throws PrunedFormatAccessException {
                    throw new PrunedFormatAccessException();
                }

                @Override
                public void checkIntegrity() {}

                @Override
                public void close() {}
            };
        }
    };

    static final PointsFormat POINTS = new PointsFormat() {
        @Override
        public PointsWriter fieldsWriter(SegmentWriteState state) {
            throw readOnly();
        }

        @Override
        public PointsReader fieldsReader(SegmentReadState state) {
            return new PointsReader() {
                @Override
                public PointValues getValues(String field) throws PrunedFormatAccessException {
                    throw new PrunedFormatAccessException();
                }

                @Override
                public void checkIntegrity() {}

                @Override
                public void close() {}
            };
        }
    };

    static final TermVectorsFormat TERM_VECTORS = new TermVectorsFormat() {
        @Override
        public TermVectorsReader vectorsReader(Directory directory, SegmentInfo segmentInfo, FieldInfos fieldInfos,
                                               IOContext context) {
            return new PrunedTermVectorsReader();
        }

        @Override
        public TermVectorsWriter vectorsWriter(Directory directory, SegmentInfo segmentInfo, IOContext context) {
            throw readOnly();
        }
    };

    private static final class PrunedTermVectorsReader extends TermVectorsReader {
        @Override
        public Fields get(int doc) throws PrunedFormatAccessException {
            throw new PrunedFormatAccessException();
        }

        @Override
        public TermVectorsReader clone() {
            return this;
        }

        @Override
        public void checkIntegrity() {}

        @Override
        public void close() {}
    }

    static final KnnVectorsFormat KNN_VECTORS = new KnnVectorsFormat("PrunedKnnVectors") {
        @Override
        public KnnVectorsWriter fieldsWriter(SegmentWriteState state) {
            throw readOnly();
        }

        @Override
        public KnnVectorsReader fieldsReader(SegmentReadState state) {
            return new KnnVectorsReader() {
                @Override
                public FloatVectorValues getFloatVectorValues(String field) throws PrunedFormatAccessException {
                    throw new PrunedFormatAccessException();
                }

                @Override
                public ByteVectorValues getByteVectorValues(String field) throws PrunedFormatAccessException {
                    throw new PrunedFormatAccessException();
                }

                @Override
                public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
                        throws PrunedFormatAccessException {
                    throw new PrunedFormatAccessException();
                }

                @Override
                public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
                        throws PrunedFormatAccessException {
                    throw new PrunedFormatAccessException();
                }

                @Override
                public void checkIntegrity() {}

                @Override
                public void close() {}
            };
        }

        @Override
        public int getMaxDimensions(String fieldName) {
            return DEFAULT_MAX_DIMENSIONS;
        }
    };
}
