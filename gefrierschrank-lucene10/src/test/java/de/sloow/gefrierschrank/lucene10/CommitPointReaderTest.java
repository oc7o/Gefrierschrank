package de.sloow.gefrierschrank.lucene10;

import de.sloow.gefrierschrank.core.repo.CommitPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommitPointReaderTest {

    static int liveRoots(Path dir) throws Exception {
        try (var reader = new Lucene10ShardReader(dir)) {
            int live = 0;
            for (int doc = 0; doc < reader.docCount(); doc++) if (reader.isLive(doc)) live++;
            return live;
        }
    }

    @Test
    void readsSegmentFactsFromTheCommitFilesOnly(@TempDir Path tmp) throws Exception {
        Path dir = tmp.resolve("index");
        PrunedCodecTest.writeEsShapedIndex(dir, false);
        // Only the commit files: everything else is absent and must not be needed.
        Path commitOnly = Files.createDirectory(tmp.resolve("commit"));
        try (var files = Files.list(dir)) {
            for (Path file : files.toList()) {
                String name = file.getFileName().toString();
                if (de.sloow.gefrierschrank.core.repo.SelectiveFetch.isCommitFile(name)) Files.copy(file, commitOnly.resolve(name));
            }
        }
        CommitPoint commit = CommitPointReader.read(commitOnly);
        assertEquals(4, commit.segments().size());
        long live = 0;
        for (CommitPoint.Segment segment : commit.segments()) {
            assertFalse(segment.compound());
            assertTrue(segment.hasSource() && segment.hasPrimaryTerm(), segment.toString());
            assertEquals(segment.softDeletesField(), segment.softDeletesFlagged(), "only segments with soft deletes have the field");
            assertTrue(segment.liveDocs() >= 0);
            live += segment.liveDocs();
        }
        assertTrue(commit.segments().get(1).hasNestedPath());
        assertFalse(commit.segments().get(0).hasNestedPath());
        assertTrue(commit.segments().get(0).softDeletesFlagged() && commit.segments().get(0).softDelCount() > 0,
            "soft updates of the first segment");
        assertTrue(commit.segments().get(0).files().stream().anyMatch(f -> f.matches("_0_\\d+\\.fnm")),
            "the field-infos generation of the doc-values update: " + commit.segments().get(0).files());
        assertTrue(commit.segments().get(0).files().stream().anyMatch(f -> f.endsWith(".liv")));

        try (var directory = new NIOFSDirectory(dir)) {
            var expectedFiles = new HashSet<String>();
            for (var info : SegmentInfos.readLatestCommit(directory)) expectedFiles.addAll(info.files());
            var files = new HashSet<String>();
            commit.segments().forEach(s -> files.addAll(s.files()));
            assertEquals(expectedFiles, files);
            assertEquals(SegmentInfos.readLatestCommit(directory).getSegmentsFileName(), commit.segmentsFile());
        }
        // Nested children carry no _primary_term, so they are never live rows; the commit counts every Lucene document.
        long children = 2 * 10 - 2;
        assertEquals(liveRoots(dir) + children, live);
    }

    @Test
    void compoundSegmentsReadTheirFieldInfosFromTheCompoundFile(@TempDir Path tmp) throws Exception {
        Path dir = tmp.resolve("index");
        PrunedCodecTest.writeEsShapedIndex(dir, true);
        CommitPoint commit = CommitPointReader.read(dir);
        assertTrue(commit.segments().stream().allMatch(CommitPoint.Segment::compound));
        assertTrue(commit.segments().stream().allMatch(CommitPoint.Segment::hasSource));
        assertTrue(commit.segments().get(1).files().contains("_1.cfs"));
    }

    @Test
    void softDeletesLuceneDidNotCountMakeTheLiveCountUnknown(@TempDir Path tmp) throws Exception {
        try (var directory = new NIOFSDirectory(tmp);
             var writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            var doc = new Document();
            doc.add(new StoredField("_id", new BytesRef("1")));
            doc.add(new NumericDocValuesField("__soft_deletes", 1));
            writer.addDocument(doc);
            var noSource = new Document();
            noSource.add(new StringField("_nested_path", "x", Field.Store.NO));
            writer.addDocument(noSource);
        }
        CommitPoint.Segment segment = CommitPointReader.read(tmp).segments().get(0);
        assertFalse(segment.softDeletesFlagged());
        assertTrue(segment.softDeletesField());
        assertEquals(CommitPoint.UNKNOWN, segment.liveDocs());
        assertFalse(segment.hasSource());
        assertFalse(segment.hasPrimaryTerm());
        assertTrue(segment.needsLegacyNestedMarkers());
    }

    @Test
    void aDocumentDeletedBothWaysIsCountedOnce(@TempDir Path tmp) throws Exception {
        try (var directory = new NIOFSDirectory(tmp);
             var writer = new IndexWriter(directory, new IndexWriterConfig().setSoftDeletesField("__soft_deletes")
                 .setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int id = 0; id < 5; id++) writer.addDocument(PrunedCodecTest.root(id, false));
            writer.commit();
            writer.softUpdateDocument(new Term("id", "id-0"), PrunedCodecTest.root(10, false),
                new NumericDocValuesField("__soft_deletes", 1));
            writer.softUpdateDocument(new Term("id", "id-1"), PrunedCodecTest.root(11, false),
                new NumericDocValuesField("__soft_deletes", 1));
            writer.commit();
            writer.deleteDocuments(new Term("id", "id-0"), new Term("id", "id-2"));
        }
        List<CommitPoint.Segment> segments = CommitPointReader.read(tmp).segments();
        CommitPoint.Segment first = segments.get(0);
        assertEquals(5, first.maxDoc());
        assertEquals(2, first.delCount());
        assertEquals(1, first.softDelCount(), "id-1 only: id-0 is hard-deleted as well");
        assertEquals(2, first.liveDocs());
        assertEquals(liveRoots(tmp), segments.stream().mapToLong(CommitPoint.Segment::liveDocs).sum());
    }
}
