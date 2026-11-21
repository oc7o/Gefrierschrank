package de.sloow.gefrierschrank.lucene10;

import de.sloow.gefrierschrank.core.lucene.DocumentId;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * ShardReader on Lucene 10: native for ES 9 shards, backward-reads ES 8 and ES 7-created shards
 * (index-created Lucene major 8, whether or not ES 8 rewrote their segments). Older indices fail
 * with IndexFormatTooOldException.
 *
 * <p>One stored-field visit per document serves both {@link #readSource} and
 * {@link #documentMetadata}, through the current segment's sequential (merge) stored-fields
 * instance, which decompresses each block once instead of once per document. Any doc id order is
 * correct; ascending order is fastest. The reader is stateful and not thread-safe: use one instance
 * per thread, or one {@link #fork} per additional thread.
 */
public final class Lucene10ShardReader implements ShardReader {

    // ES 7 creates indices with Lucene 8; Lucene reads indices created two majors back (N-2), read-only.
    private static final int OLDEST_READABLE_LUCENE_MAJOR = 8;
    // A merge instance keeps its last decompressed block, which after a source this large is about the
    // document's own size; dropping the instance lets the block go before the document moves downstream.
    private static final int RELEASE_BLOCK_AFTER_SOURCE_BYTES = 1 << 20;

    private final Directory directory;
    private final DirectoryReader reader;
    private final List<LeafReaderContext> leaves;
    private final boolean[] sourceFieldAbsent;
    private final java.util.BitSet deleted;
    private final SourceIdentityVisitor visitor = new SourceIdentityVisitor();
    private StoredFields leafFields;
    private int leafBase;
    private int leafEnd;
    private int visitedDoc = -1;
    private boolean sourceHandedOut;
    // A fork shares the opened directory and reader with its parent, which alone closes them.
    private boolean fork;
    // Format of the visited document's stored _source: SMILE, CBOR and YAML are handed out as JSON.
    private de.sloow.gefrierschrank.core.source.SourceFormat visitedFormat = de.sloow.gefrierschrank.core.source.SourceFormat.JSON;

    public Lucene10ShardReader(Path shardDir) throws IOException {
        this(openLatestCommit(new NIOFSDirectory(shardDir)));
    }

    private static DirectoryReader openLatestCommit(Directory directory) throws IOException {
        try {
            // listCommits parses with floor 0 and resolves every segment's codec first, so an ES 6-created
            // commit with Lucene 7 segments would fail there with a misleading "missing backward-codecs"
            // IllegalArgumentException; readLatestCommit rejects it with IndexFormatTooOldException first.
            SegmentInfos.readLatestCommit(directory, OLDEST_READABLE_LUCENE_MAJOR);
            // Same commit and leaf order as DirectoryReader.open(directory), minus its created-major floor of 9.
            var commits = DirectoryReader.listCommits(directory);
            return DirectoryReader.open(commits.get(commits.size() - 1), OLDEST_READABLE_LUCENE_MAJOR, null);
        } catch (IOException | RuntimeException | Error failure) {
            try { directory.close(); } catch (IOException e) { failure.addSuppressed(e); }
            throw failure;
        }
    }

    /**
     * Opens the shard's latest commit without postings, points, norms, term vectors and vectors
     * (see {@link PrunedCodec}): their files need not be in {@code shardDir}, and reading one of
     * those structures throws {@link de.sloow.gefrierschrank.core.repo.PrunedFormatAccessException}.
     * Leaves, doc bases, deletions and every document read exactly as with the public constructor.
     */
    public static Lucene10ShardReader openPruned(Path shardDir) throws IOException {
        return openPruned(new NIOFSDirectory(shardDir));
    }

    /** {@link #openPruned(Path)} over {@code directory}, which the reader takes over (and closes on failure). */
    static Lucene10ShardReader openPruned(Directory directory) throws IOException {
        DirectoryReader opened;
        try {
            opened = openPrunedReader(directory);
        } catch (IOException | RuntimeException | Error failure) {
            try { directory.close(); } catch (IOException e) { failure.addSuppressed(e); }
            throw failure;
        }
        return new Lucene10ShardReader(opened);
    }

    /** The latest commit, every segment with its codec wrapped in a {@link PrunedCodec}; the directory stays open on failure. */
    static DirectoryReader openPrunedReader(Directory directory) throws IOException {
        SegmentInfos infos = SegmentInfos.readLatestCommit(directory, OLDEST_READABLE_LUCENE_MAJOR);
        SegmentInfos pruned = infos.clone();
        pruned.clear();
        for (SegmentCommitInfo info : infos) pruned.add(prunedCopy(info));
        // One SegmentReader per segment in commit order, like the public constructor's open.
        return StandardDirectoryReader.open(directory, pruned, null, null);
    }

    /** A copy with the codec wrapped: SegmentInfo.setCodec asserts that no codec is set yet. */
    private static SegmentCommitInfo prunedCopy(SegmentCommitInfo info) {
        SegmentInfo si = info.info;
        var copy = new SegmentInfo(si.dir, si.getVersion(), si.getMinVersion(), si.name, si.maxDoc(),
            si.getUseCompoundFile(), si.getHasBlocks(), new PrunedCodec(si.getCodec()), si.getDiagnostics(), si.getId(),
            si.getAttributes(), si.getIndexSort());
        copy.setFiles(si.files());
        var commit = new SegmentCommitInfo(copy, info.getDelCount(), info.getSoftDelCount(), info.getDelGen(),
            info.getFieldInfosGen(), info.getDocValuesGen(), info.getId());
        commit.setFieldInfosFiles(info.getFieldInfosFiles());
        commit.setDocValuesUpdatesFiles(info.getDocValuesUpdatesFiles());
        return commit;
    }

    /** Takes over {@code opened} and its directory, closing both if the shard cannot be prepared. */
    private Lucene10ShardReader(DirectoryReader opened) throws IOException {
        this.directory = opened.directory();
        this.reader = opened;
        try {
            this.leaves = reader.leaves();
            this.sourceFieldAbsent = new boolean[leaves.size()];
            for (int leaf = 0; leaf < leaves.size(); leaf++) {
                sourceFieldAbsent[leaf] = leaves.get(leaf).reader().getFieldInfos().fieldInfo("_source") == null;
            }
            this.deleted = DeletedDocuments.load(reader);
        } catch (IOException | RuntimeException | Error failure) {
            try { opened.close(); } catch (IOException e) { failure.addSuppressed(e); }
            try { directory.close(); } catch (IOException e) { failure.addSuppressed(e); }
            throw failure;
        }
    }

    /**
     * The opened shard is immutable and safe to share: DirectoryReader and its leaves, the deleted
     * docs and the per-leaf _source flags. Each fork has its own visitor and cursor, and its own
     * merge instances, which clone the segment inputs and decompressor.
     */
    private Lucene10ShardReader(Lucene10ShardReader parent) {
        this.directory = parent.directory;
        this.reader = parent.reader;
        this.leaves = parent.leaves;
        this.sourceFieldAbsent = parent.sourceFieldAbsent;
        this.deleted = parent.deleted;
        this.fork = true;
    }

    @Override
    public ShardReader fork() {
        return new Lucene10ShardReader(this);
    }

    @Override
    public String runtimeName() {
        return "lucene10";
    }

    @Override
    public int docCount() {
        return reader.maxDoc();
    }

    @Override
    public boolean isLive(int docId) {
        return docId >= 0 && docId < reader.maxDoc() && !deleted.get(docId);
    }

    /** FieldInfos list every stored field of a segment, so a segment without _source has no stored source. */
    @Override
    public boolean storedSourceAbsent(int docId) {
        checkDocId(docId);
        return sourceFieldAbsent[ReaderUtil.subIndex(docId, leaves)];
    }

    @Override
    public DocumentMetadata documentMetadata(int docId) throws IOException {
        checkDocId(docId);
        if (docId != visitedDoc) visit(docId);
        String id = visitor.idBinary != null
            ? DocumentId.decode(visitor.idBinary, 0, visitor.idBinary.length) : visitor.idText;
        return new DocumentMetadata(id, visitor.routing, visitor.sawSource ? visitedFormat.storedKind() : "synthetic", docId);
    }

    @Override
    public void readSource(int docId, SourceConsumer consumer) throws IOException {
        checkDocId(docId);
        if (docId != visitedDoc || sourceHandedOut) visit(docId);
        if (!visitor.sawSource) {
            // Synthetic source: the index stores no _source at all and Elasticsearch rebuilds it
            // from doc values at query time. Reconstructing it here would mean reimplementing both
            // ES's proprietary TSDB doc-values format and its synthetic-source logic. Failing
            // loudly is the only honest option - emitting empty or partial rows would silently
            // corrupt the exported table, which is far worse than a quarantined shard.
            throw new IOException("document " + docId + " has no stored _source (synthetic source);"
                + " dump-to-delta and dump reconstruct it when the index metadata is available");
        }
        if (visitor.source == null) {
            throw new IOException("document " + docId + " stores _source as a non-binary field");
        }
        // Lucene allocates an exact-length array per visit, so the consumer may keep it. The reader
        // lets go of it, so a large source is garbage as soon as the consumer is done with it.
        byte[] source = visitor.source;
        visitor.source = null;
        sourceHandedOut = true;
        if (visitedFormat.converted()) {
            var json = de.sloow.gefrierschrank.core.source.SourceJsonConverter.toJson(source, source.length, visitedFormat);
            consumer.accept(json.bytes(), json.length());
        } else {
            consumer.accept(source, source.length);
        }
    }

    /** One byte compare for JSON; anything else is detected as Elasticsearch detects it. */
    private static de.sloow.gefrierschrank.core.source.SourceFormat formatOf(byte[] source) {
        return source == null || source.length > 0 && source[0] == '{'
            ? de.sloow.gefrierschrank.core.source.SourceFormat.JSON
            : de.sloow.gefrierschrank.core.source.SourceFormat.detect(source, source.length);
    }

    /** Same failure as the composite reader's StoredFields; also keeps -1 from matching an empty cache. */
    private void checkDocId(int docId) {
        if (docId < 0 || docId >= reader.maxDoc()) {
            throw new IllegalArgumentException(
                "docID must be >= 0 and < maxDoc=" + reader.maxDoc() + " (got docID=" + docId + ")");
        }
    }

    private void visit(int docId) throws IOException {
        visitedDoc = -1;
        visitor.reset();
        StoredFields fields = fieldsFor(docId);
        fields.document(docId - leafBase, visitor);
        visitedFormat = formatOf(visitor.source);
        if (visitor.source != null && visitor.source.length > RELEASE_BLOCK_AFTER_SOURCE_BYTES) releaseLeafFields();
        visitedDoc = docId;
        sourceHandedOut = false;
    }

    private StoredFields fieldsFor(int docId) throws IOException {
        if (docId >= leafBase && docId < leafEnd) return leafFields;
        // Drop the previous segment's instance first: a merge instance keeps its last decompressed
        // block, which for a sliced oversized document is that document's whole block.
        releaseLeafFields();
        var leaf = leaves.get(ReaderUtil.subIndex(docId, leaves));
        var codecFields = leaf.reader() instanceof CodecReader codec ? codec.getFieldsReader() : null;
        // Merge instances are clones over the segment core's inputs; Lucene's own merging never
        // closes them either, and the segment core is closed with the DirectoryReader.
        leafFields = codecFields != null ? codecFields.getMergeInstance() : leaf.reader().storedFields();
        leafBase = leaf.docBase;
        leafEnd = leaf.docBase + leaf.reader().maxDoc();
        return leafFields;
    }

    private void releaseLeafFields() {
        leafFields = null;
        leafBase = leafEnd = 0;
    }

    /**
     * A fork lets go of its decompressed block, which after the last documents of a range can hold
     * the next range's large document, and keeps nothing shared from being closed.
     */
    @Override
    public void close() throws IOException {
        if (fork) {
            releaseLeafFields();
            visitor.reset();
            visitedDoc = -1;
            return;
        }
        try (directory; reader) { /* close both even if one fails */ }
    }
}
