package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.util.List;

/**
 * A leaf that knows only a segment's FieldInfos: every structure the FieldInfos declare is present
 * and empty, every other one is absent, exactly as a codec reader answers {@code null} for a field
 * without that structure. The commit-point preflight evaluates {@link ReconstructionCoverage#refusals}
 * on it with the same code as read time, before any data file of the segment is downloaded;
 * FieldInfosLeafReaderTest and the coverage matrix pin that both give the same answer, except for
 * the per-document check the rule skips on this leaf (see ReconstructionCoverage#refusals). Anything
 * a presence check has no business asking (points, term vectors, nearest vectors) is refused loudly.
 */
final class FieldInfosLeafReader extends LeafReader {

    private final FieldInfos fieldInfos;
    private final int maxDoc;
    private final LeafMetaData metaData;

    FieldInfosLeafReader(FieldInfos fieldInfos, int maxDoc, LeafMetaData metaData) {
        this.fieldInfos = fieldInfos;
        this.maxDoc = maxDoc;
        this.metaData = metaData;
    }

    private boolean hasDocValues(String field, DocValuesType type) {
        FieldInfo info = fieldInfos.fieldInfo(field);
        return info != null && info.getDocValuesType() == type;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return null;
    }

    @Override
    public Terms terms(String field) {
        FieldInfo info = fieldInfos.fieldInfo(field);
        if (info == null || info.getIndexOptions() == IndexOptions.NONE) return null;
        return new EmptyTerms(info);
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) {
        return hasDocValues(field, DocValuesType.NUMERIC) ? DocValues.emptyNumeric() : null;
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) {
        return hasDocValues(field, DocValuesType.BINARY) ? DocValues.emptyBinary() : null;
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) {
        return hasDocValues(field, DocValuesType.SORTED) ? new PresentSorted() : null;
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) {
        return hasDocValues(field, DocValuesType.SORTED_NUMERIC) ? DocValues.emptySortedNumeric() : null;
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) {
        return hasDocValues(field, DocValuesType.SORTED_SET) ? new PresentSortedSet() : null;
    }

    @Override
    public NumericDocValues getNormValues(String field) {
        FieldInfo info = fieldInfos.fieldInfo(field);
        return info != null && info.hasNorms() ? DocValues.emptyNumeric() : null;
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) {
        throw new UnsupportedOperationException("presence-only leaf");
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) {
        FieldInfo info = fieldInfos.fieldInfo(field);
        return info != null && info.getVectorDimension() > 0 && info.getVectorEncoding() == VectorEncoding.FLOAT32
            ? FloatVectorValues.fromFloats(List.of(), info.getVectorDimension()) : null;
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) {
        FieldInfo info = fieldInfos.fieldInfo(field);
        return info != null && info.getVectorDimension() > 0 && info.getVectorEncoding() == VectorEncoding.BYTE
            ? ByteVectorValues.fromBytes(List.of(), info.getVectorDimension()) : null;
    }

    @Override
    public void searchNearestVectors(String field, float[] target, KnnCollector collector, AcceptDocs acceptDocs) {
        throw new UnsupportedOperationException("presence-only leaf");
    }

    @Override
    public void searchNearestVectors(String field, byte[] target, KnnCollector collector, AcceptDocs acceptDocs) {
        throw new UnsupportedOperationException("presence-only leaf");
    }

    @Override
    public FieldInfos getFieldInfos() {
        return fieldInfos;
    }

    @Override
    public Bits getLiveDocs() {
        return null;
    }

    @Override
    public PointValues getPointValues(String field) {
        throw new UnsupportedOperationException("presence-only leaf");
    }

    @Override
    public void checkIntegrity() {
    }

    @Override
    public LeafMetaData getMetaData() {
        return metaData;
    }

    @Override
    public TermVectors termVectors() {
        throw new UnsupportedOperationException("presence-only leaf");
    }

    @Override
    public int numDocs() {
        return maxDoc;
    }

    @Override
    public int maxDoc() {
        return maxDoc;
    }

    @Override
    public StoredFields storedFields() {
        return new StoredFields() {
            @Override
            public void document(int docID, StoredFieldVisitor visitor) {
            }
        };
    }

    @Override
    protected void doClose() {
    }

    /**
     * Sorted doc values of a field that has them: no document is positioned, but the value count
     * is not zero, because loaders treat a dictionary without terms as an absent field (on a real
     * leaf that means no document has a value, which the rule treats as covered).
     */
    private static final class PresentSortedSet extends SortedSetDocValues {
        private int doc = -1;

        @Override public long nextOrd() { throw new IllegalStateException("no document is positioned"); }
        @Override public int docValueCount() { return 0; }
        @Override public BytesRef lookupOrd(long ord) { throw new UnsupportedOperationException("presence-only leaf"); }
        @Override public long getValueCount() { return 1; }
        @Override public boolean advanceExact(int target) { doc = target; return false; }
        @Override public int docID() { return doc; }
        @Override public int nextDoc() { return doc = NO_MORE_DOCS; }
        @Override public int advance(int target) { return doc = NO_MORE_DOCS; }
        @Override public long cost() { return 0; }
    }

    /** {@link PresentSortedSet} for single-valued sorted doc values. */
    private static final class PresentSorted extends SortedDocValues {
        private int doc = -1;

        @Override public int ordValue() { throw new IllegalStateException("no document is positioned"); }
        @Override public BytesRef lookupOrd(int ord) { throw new UnsupportedOperationException("presence-only leaf"); }
        @Override public int getValueCount() { return 1; }
        @Override public boolean advanceExact(int target) { doc = target; return false; }
        @Override public int docID() { return doc; }
        @Override public int nextDoc() { return doc = NO_MORE_DOCS; }
        @Override public int advance(int target) { return doc = NO_MORE_DOCS; }
        @Override public long cost() { return 0; }
    }

    /** Terms of a field that is indexed but whose terms were never read. */
    private static final class EmptyTerms extends Terms {
        private final FieldInfo info;

        EmptyTerms(FieldInfo info) {
            this.info = info;
        }

        @Override
        public TermsEnum iterator() {
            return TermsEnum.EMPTY;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public long getSumTotalTermFreq() {
            return 0;
        }

        @Override
        public long getSumDocFreq() {
            return 0;
        }

        @Override
        public int getDocCount() {
            return 0;
        }

        @Override
        public boolean hasFreqs() {
            return info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
        }

        @Override
        public boolean hasOffsets() {
            return info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
        }

        @Override
        public boolean hasPositions() {
            return info.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
        }

        @Override
        public boolean hasPayloads() {
            return info.hasPayloads();
        }
    }
}
