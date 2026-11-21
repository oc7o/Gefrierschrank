package de.sloow.gefrierschrank.lucene10;

import de.sloow.gefrierschrank.core.repo.CommitPoint;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Reads the {@link CommitPoint} of a shard directory that holds at least its commit files
 * ({@code segments_N}, {@code .si}, {@code .fnm}, compound {@code .cfs/.cfe}); opens nothing else.
 */
public final class CommitPointReader {

    // The created-major floor of Lucene10ShardReader, so both reject the same commits.
    private static final int OLDEST_READABLE_LUCENE_MAJOR = 8;

    private CommitPointReader() {}

    public static CommitPoint read(Path shardDir) throws IOException {
        try (Directory directory = new NIOFSDirectory(shardDir)) {
            SegmentInfos infos = SegmentInfos.readLatestCommit(directory, OLDEST_READABLE_LUCENE_MAJOR);
            List<CommitPoint.Segment> segments = new ArrayList<>(infos.size());
            for (SegmentCommitInfo info : infos) {
                FieldInfos fields = readFieldInfos(info);
                boolean softDeletesFlagged = fields.getSoftDeletesField() != null;
                segments.add(new CommitPoint.Segment(info.info.name, info.info.getUseCompoundFile(), info.info.maxDoc(),
                    info.getDelCount(), info.getSoftDelCount(), softDeletesFlagged,
                    softDeletesFlagged || fields.fieldInfo("__soft_deletes") != null,
                    fields.fieldInfo("_source") != null, fields.fieldInfo("_primary_term") != null,
                    fields.fieldInfo("_nested_path") != null, fields.fieldInfo("_type") != null,
                    new HashSet<>(info.files())));
            }
            return new CommitPoint(infos.getSegmentsFileName(), segments);
        }
    }

    /** Lucene's package-private {@code IndexWriter.readFieldInfos}: the latest field-infos generation of the segment. */
    private static FieldInfos readFieldInfos(SegmentCommitInfo info) throws IOException {
        Codec codec = info.info.getCodec();
        if (info.hasFieldUpdates()) {
            String suffix = Long.toString(info.getFieldInfosGen(), Character.MAX_RADIX);
            return codec.fieldInfosFormat().read(info.info.dir, info.info, suffix, IOContext.READONCE);
        }
        if (info.info.getUseCompoundFile()) {
            try (Directory compound = codec.compoundFormat().getCompoundReader(info.info.dir, info.info)) {
                return codec.fieldInfosFormat().read(compound, info.info, "", IOContext.READONCE);
            }
        }
        return codec.fieldInfosFormat().read(info.info.dir, info.info, "", IOContext.READONCE);
    }
}
