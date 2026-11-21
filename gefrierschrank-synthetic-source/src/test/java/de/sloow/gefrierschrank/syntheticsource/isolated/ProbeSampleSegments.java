package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Test helper, defined by the isolated loader: writes one segment per entry of {@code blocks} and
 * returns, per leaf, the probe's sample and live roots. In a segment with nested blocks, block
 * {@code i} is {@code i % 3} children without {@code _primary_term} followed by its root; without
 * them every document is flat and no document holds {@code _primary_term}. Document {@code i} of
 * either kind (the block's documents for a root) is hard-deleted when {@code i % 7 == 3} and
 * soft-deleted when {@code i % 5 == 1}. JDK types only.
 */
public final class ProbeSampleSegments {

    private ProbeSampleSegments() {}

    public static List<int[][]> sampleAndLiveRoots(String dir, int[] blocks, boolean[] nested) throws IOException {
        IsolatedEsBootstrap.ensure();
        try (Directory directory = new NIOFSDirectory(Path.of(dir))) {
            IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (int segment = 0; segment < blocks.length; segment++) {
                    for (int i = 0; i < blocks[segment]; i++) {
                        List<Document> block = new ArrayList<>();
                        for (int child = 0; nested[segment] && child < i % 3; child++) block.add(document(segment, i, false));
                        block.add(document(segment, i, nested[segment]));
                        writer.addDocuments(block);
                    }
                    for (int i = 3; i < blocks[segment]; i += 7) writer.deleteDocuments(new Term("id", segment + "/" + i));
                    writer.commit();
                }
            }
            List<int[][]> result = new ArrayList<>();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    result.add(new int[][] {IsolatedSyntheticFetchProbeImpl.sample(leaf.reader()),
                        IsolatedSyntheticFetchProbeImpl.liveRoots(leaf.reader())});
                }
            }
            return result;
        }
    }

    private static Document document(int segment, int i, boolean root) {
        Document document = new Document();
        document.add(new StringField("id", segment + "/" + i, Field.Store.NO));
        if (root) document.add(new NumericDocValuesField("_primary_term", 1));
        if (i % 5 == 1) document.add(new NumericDocValuesField("__soft_deletes", 1));
        return document;
    }
}
