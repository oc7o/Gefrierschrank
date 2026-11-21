package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.Terms;

import java.util.Collections;
import java.util.Iterator;

/** Postings producer over zero fields; satisfies segment open without any file access. */
final class EmptyFieldsProducer extends FieldsProducer {

    static final EmptyFieldsProducer INSTANCE = new EmptyFieldsProducer();

    private EmptyFieldsProducer() {}

    @Override
    public Iterator<String> iterator() {
        return Collections.emptyIterator();
    }

    @Override
    public Terms terms(String field) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void checkIntegrity() {}

    @Override
    public void close() {}
}
