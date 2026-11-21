package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Test helper, defined by the isolated loader so Elasticsearch's codecs read the source: rewrites a
 * shard's latest commit into another directory with one non-compound segment per source segment
 * (every fixture segment is compound, and selective fetch prunes only non-compound segments). The
 * segments are written with Lucene's default codec; hard-deleted documents are dropped, soft-deleted
 * ones kept. Index sort, the block parent field and the index's created major version are carried
 * over. JDK types only.
 */
public final class NonCompoundShards {

    private NonCompoundShards() {}

    public static void rewrite(String from, String to) throws IOException {
        IsolatedEsBootstrap.ensure();
        try (Directory source = new NIOFSDirectory(Path.of(from));
             DirectoryReader reader = DirectoryReader.open(source);
             Directory target = new NIOFSDirectory(Path.of(to))) {
            LeafReader first = reader.leaves().get(0).reader();
            TieredMergePolicy policy = new TieredMergePolicy();
            policy.setNoCFSRatio(0.0);
            IndexWriterConfig config = new IndexWriterConfig().setUseCompoundFile(false).setMergePolicy(policy)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE)
                .setIndexCreatedVersionMajor(SegmentInfos.readLatestCommit(source).getIndexCreatedVersionMajor());
            Sort sort = first.getMetaData().sort();
            if (sort != null) config.setIndexSort(sort);
            String parent = first.getFieldInfos().getParentField();
            if (parent != null) config.setParentField(parent);
            if (first.getFieldInfos().fieldInfo("__soft_deletes") != null) config.setSoftDeletesField("__soft_deletes");
            try (IndexWriter writer = new IndexWriter(target, config)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    writer.addIndexes((CodecReader) leaf.reader());
                    writer.commit();
                }
            }
        }
    }
}
