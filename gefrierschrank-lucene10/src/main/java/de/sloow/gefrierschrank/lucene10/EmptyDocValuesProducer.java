package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;

/**
 * Doc-values producer that reports every field as absent, so a segment using an
 * Elasticsearch-proprietary doc-values format can still be opened and its stored fields read.
 *
 * <p>Returning {@code null} is the contract Lucene expects for "this field has no doc values in
 * this segment", and is safe for a {@code _source} dump, which reads stored fields only. It is
 * NOT safe for anything that needs the doc-values content itself - see
 * {@link EsTsdbDocValuesStubFormat} for why that matters for synthetic-source indices.
 */
final class EmptyDocValuesProducer extends DocValuesProducer {

    static final EmptyDocValuesProducer INSTANCE = new EmptyDocValuesProducer();

    private EmptyDocValuesProducer() {}

    @Override
    public NumericDocValues getNumeric(FieldInfo field) {
        return null;
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) {
        return null;
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) {
        return null;
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
        return null;
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) {
        return null;
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) {
        return null;
    }

    @Override
    public void checkIntegrity() {}

    @Override
    public void close() {}
}
