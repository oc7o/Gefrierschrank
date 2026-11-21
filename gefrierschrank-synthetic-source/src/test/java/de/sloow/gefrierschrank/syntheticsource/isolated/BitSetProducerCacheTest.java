package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BitSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BitSetProducerCacheTest {

    @Test
    void equalQueriesShareOneProducer() {
        var cache = new BitSetProducerCache();
        var producer = cache.producer(new TermQuery(new Term("type", "root")));
        assertSame(producer, cache.producer(new TermQuery(new Term("type", "root"))));
        assertNotSame(producer, cache.producer(new TermQuery(new Term("type", "child"))));
    }

    @Test
    void aSegmentsBitsetIsBuiltOncePerProducer() throws IOException {
        try (var directory = new ByteBuffersDirectory()) {
            index(directory, false);
            try (var reader = DirectoryReader.open(directory)) {
                var weights = new AtomicInteger();
                var cache = new BitSetProducerCache();
                LeafReaderContext leaf = reader.leaves().get(0);
                BitSet first = cache.producer(new CountingQuery(new TermQuery(new Term("type", "root")), weights)).getBitSet(leaf);
                BitSet second = cache.producer(new CountingQuery(new TermQuery(new Term("type", "root")), weights)).getBitSet(leaf);
                assertSame(first, second);
                assertEquals(1, weights.get(), "the second lookup builds nothing");
            }
        }
    }

    /** A scorer ignores live docs: deleted roots still delimit their children's block, as in Elasticsearch. */
    @Test
    void theBitsetHoldsDeletedRoots() throws IOException {
        try (var directory = new ByteBuffersDirectory()) {
            index(directory, true);
            try (var reader = DirectoryReader.open(directory)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                assertFalse(leaf.reader().getLiveDocs().get(2), "root 2 is deleted");
                BitSet roots = new BitSetProducerCache().producer(new TermQuery(new Term("type", "root"))).getBitSet(leaf);
                assertTrue(roots.get(2));
                assertEquals(2, roots.cardinality());
                assertFalse(roots.get(0));
                assertFalse(roots.get(1));
            }
        }
    }

    /** Docs 0,1 children of root 2 (deleted when asked), doc 3 a root without children. */
    private static void index(ByteBuffersDirectory directory, boolean deleteFirstRoot) throws IOException {
        try (var writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            writer.addDocuments(java.util.List.of(doc("child", "a"), doc("child", "a"), doc("root", "a")));
            writer.addDocument(doc("root", "b"));
            if (deleteFirstRoot) writer.deleteDocuments(new Term("id", "a"));
            writer.commit();
        }
    }

    private static Document doc(String type, String id) {
        Document document = new Document();
        document.add(new StringField("type", type, Field.Store.NO));
        document.add(new StringField("id", id, Field.Store.NO));
        return document;
    }

    private static final class CountingQuery extends Query {
        private final Query delegate;
        private final AtomicInteger weights;

        CountingQuery(Query delegate, AtomicInteger weights) {
            this.delegate = delegate;
            this.weights = weights;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            weights.incrementAndGet();
            return delegate.createWeight(searcher, scoreMode, boost);
        }

        @Override
        public String toString(String field) {
            return "counting(" + delegate.toString(field) + ")";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            delegate.visit(visitor);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof CountingQuery counting && counting.delegate.equals(delegate);
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }
    }
}
