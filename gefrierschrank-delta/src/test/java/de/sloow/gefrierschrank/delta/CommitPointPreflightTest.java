package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.CommitPoint;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.lucene10.CommitPointReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The residual commit-point preflight and its plumbing, driven through the coverage seam. */
class CommitPointPreflightTest {

    static final Path DELETES_REPO = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures/synth-deletes-repo");

    /** A refusal standing in for the reconstruction's coverage check; content-free like the real ones. */
    static final class Refused extends IOException implements de.sloow.gefrierschrank.core.FailureDiagnostics.SafeMessage {
        Refused() {
            super("refused by the test coverage check");
        }
    }

    private CommitPointPreflight.CoverageCheck savedCoverage;
    private String savedSelective;
    final List<Set<String>> checked = new CopyOnWriteArrayList<>();

    @BeforeEach
    void installRefusingCoverage() {
        savedCoverage = CommitPointPreflight.coverage;
        savedSelective = System.getProperty(SelectiveFetch.PROPERTY);
        CommitPointPreflight.coverage = (dir, index, metadata, segments) -> {
            checked.add(segments);
            throw new Refused();
        };
    }

    @AfterEach
    void restore() {
        CommitPointPreflight.coverage = savedCoverage;
        if (savedSelective == null) System.clearProperty(SelectiveFetch.PROPERTY);
        else System.setProperty(SelectiveFetch.PROPERTY, savedSelective);
    }

    static CommitPoint.Segment segment(String name, boolean hasSource, int maxDoc, int deleted, boolean softFlagged) {
        return new CommitPoint.Segment(name, false, maxDoc, deleted, 0, softFlagged, true, hasSource, true, false, false, Set.of());
    }

    @Test
    void onlyLiveSourcelessSegmentsAreChecked() throws Exception {
        JsonNode metadata = SelectiveShardFixture.STORED_METADATA;
        var commit = new CommitPoint("segments_3", List.of(segment("_0", true, 10, 0, true), segment("_1", false, 10, 3, true),
            segment("_2", false, 10, 10, true), segment("_3", false, 10, 0, false), segment("_4", false, 5, 0, true)));
        assertThrows(Refused.class, () -> CommitPointPreflight.require(commit, metadata, "idx", Path.of(".")));
        assertEquals(List.of(Set.of("_1", "_4")), checked, "stored, fully deleted and uncounted segments are not checked");

        checked.clear();
        CommitPointPreflight.require(new CommitPoint("segments_3", List.of(segment("_0", true, 10, 0, true),
            segment("_2", false, 10, 10, true))), metadata, "idx", Path.of("."));
        CommitPointPreflight.require(commit, null, "idx", Path.of("."));
        CommitPointPreflight.require(new CommitPoint("segments_1", List.of()), metadata, "idx", Path.of("."));
        assertEquals(List.of(), checked, "nothing live without _source, no metadata, or an empty shard: no check");
    }

    /**
     * The shipped seam is reconstruction's own predicate: it passes the nested shard that
     * reconstruction exports, so with the flag that shard commits after being checked once.
     */
    @Test
    void theShippedSeamPassesAShardReconstructionExports(@TempDir Path tmp) throws Exception {
        CommitPointPreflight.CoverageCheck shipped = savedCoverage;
        CommitPointPreflight.coverage = (dir, index, metadata, segments) -> {
            checked.add(segments);
            shipped.check(dir, index, metadata, segments);
        };
        Set<String> expected = liveSourceless(tmp);
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        RunReport report = exportNested(new SelectiveShardFixture.CountingStore(DELETES_REPO), tmp.resolve("t"),
            new SelectiveShardFixture.Opener());
        RunReport.ShardReport shard = report.shards().get(0);
        assertEquals(RunReport.Outcome.COMMITTED, shard.outcome(), report.toString());
        assertEquals(54, shard.rowsWritten());
        assertEquals("full", shard.fetch(), "a synthetic index takes the full plan");
        assertEquals(List.of(expected), checked);
    }

    /** A segment mixing stored and source-less documents carries _source in its FieldInfos. */
    @Test
    void aMixedSegmentIsNotPreflighted(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.shard(tmp, writer -> {
            writer.addDocument(SelectiveShardFixture.root(1));
            var sourceless = new Document();
            sourceless.add(new StoredField("_id", new BytesRef("2")));
            sourceless.add(new NumericDocValuesField("_primary_term", 1));
            writer.addDocument(sourceless);
        });
        var dir = tmp.resolve("open");
        new ShardMaterializer(new FileSystemBlobStore(shard.repo())).materialize(shard.snapshot(), dir);
        CommitPoint commit = CommitPointReader.read(dir);
        assertEquals(1, commit.segments().size());
        assertTrue(commit.segments().get(0).hasSource());
        CommitPointPreflight.require(commit, SelectiveShardFixture.STORED_METADATA, "idx", dir);
        assertEquals(List.of(), checked);
    }

    @Test
    void anEmptyShardCommitsNothingWithoutACheck(@TempDir Path tmp) throws Exception {
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var shard = SelectiveShardFixture.shard(tmp, writer -> {
            var sourceless = new Document();
            sourceless.add(new StoredField("_id", new BytesRef("2")));
            sourceless.add(new org.apache.lucene.document.StringField("_id_terms", "2", org.apache.lucene.document.Field.Store.NO));
            writer.addDocument(sourceless);
            writer.commit();
            writer.deleteDocuments(new Term("_id_terms", "2"));
        });
        RunReport report = SelectiveShardFixture.export(shard, new SelectiveShardFixture.CountingStore(shard.repo()),
            tmp.resolve("t").toString(), new Configuration(), new SelectiveShardFixture.Opener(), SelectiveShardFixture.STORED_METADATA);
        assertEquals(RunReport.Outcome.COMMITTED, report.shards().get(0).outcome(), report.toString());
        assertEquals(0, report.rowsWrittenThisRun());
        assertEquals(List.of(), checked);
    }

    static ShardSnapshot nestedShard() throws IOException {
        return new RepositoryInspector(new FileSystemBlobStore(DELETES_REPO)).shardSnapshot("test_deletes_nested", 0);
    }

    /** What the seam must receive: the live segments without _source, read from the fully materialized shard. */
    static Set<String> liveSourceless(Path tmp) throws Exception {
        Path full = tmp.resolve("full-for-expectation");
        new ShardMaterializer(new FileSystemBlobStore(DELETES_REPO)).materialize(nestedShard(), full);
        return CommitPointReader.read(full).segments().stream().filter(s -> s.liveDocs() > 0 && !s.hasSource())
            .map(CommitPoint.Segment::name).collect(Collectors.toCollection(TreeSet::new));
    }

    static RunReport exportNested(SelectiveShardFixture.CountingStore store, Path table, SelectiveShardFixture.Opener opener)
            throws Exception {
        var sources = new RepositorySources(new RepositorySources.Source("primary", store));
        return new FullIndexExporter(sources, new Configuration(), opener).run("test_deletes_nested", table.toString(),
            RunReportListener.NO_OP);
    }

    @Test
    void withTheFlagARefusedShardQuarantinesAfterItsCommitFilesOnly(@TempDir Path tmp) throws Exception {
        Set<String> expected = liveSourceless(tmp);
        assertFalse(expected.isEmpty(), "test_deletes_nested has live segments without stored _source");
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var store = new SelectiveShardFixture.CountingStore(DELETES_REPO);
        var opener = new SelectiveShardFixture.Opener();
        RunReport report = exportNested(store, tmp.resolve("t"), opener);

        RunReport.ShardReport shard = report.shards().get(0);
        assertEquals(RunReport.Outcome.QUARANTINED, shard.outcome());
        assertEquals("Refused", shard.errorClass());
        assertTrue(shard.errorMessage().startsWith(Refused.class.getName() + ": refused by the test coverage check"), shard.errorMessage());
        assertEquals("preflight", shard.fetch());
        ShardSnapshot snapshot = nestedShard();
        var commitFiles = snapshot.files().stream().filter(f -> SelectiveFetch.isCommitFile(f.physicalName())).toList();
        assertEquals(commitFiles.stream().filter(f -> !f.blobName().startsWith("v__")).mapToLong(RepoFileInfo::length).sum(),
            shard.bytesFetched());
        Set<String> commitBlobs = commitFiles.stream().map(RepoFileInfo::blobName).filter(b -> !b.startsWith("v__"))
            .collect(Collectors.toSet());
        Set<String> readShardBlobs = store.blobsRead().stream().map(key -> key.substring(key.lastIndexOf('/') + 1))
            .filter(blob -> blob.startsWith("__")).collect(Collectors.toCollection(TreeSet::new));
        assertEquals(new TreeSet<>(commitBlobs), readShardBlobs, "only the commit files were downloaded");
        assertEquals(List.of(expected), checked, "checked once, never retried, with exactly the live source-less segments");
        assertEquals(0, opener.opens.get() + opener.prunedOpens.get());
    }

    @Test
    void withoutTheFlagNothingIsPreflighted(@TempDir Path tmp) throws Exception {
        var store = new SelectiveShardFixture.CountingStore(DELETES_REPO);
        RunReport report = exportNested(store, tmp.resolve("t"), new SelectiveShardFixture.Opener());
        RunReport.ShardReport shard = report.shards().get(0);
        assertEquals(RunReport.Outcome.COMMITTED, shard.outcome(), "nested synthetic source is reconstructed since synthetic-coverage");
        assertEquals(null, shard.errorClass());
        assertEquals(54, shard.rowsWritten());
        assertEquals("full", shard.fetch());
        assertEquals(nestedShard().files().stream().filter(f -> !f.blobName().startsWith("v__")).mapToLong(RepoFileInfo::length).sum(),
            shard.bytesFetched());
        assertEquals(List.of(), checked);
    }

    @Test
    void thePreflightOverloadRefusesWithoutTheFlag(@TempDir Path tmp) throws Exception {
        Set<String> expected = liveSourceless(tmp);
        var store = new SelectiveShardFixture.CountingStore(DELETES_REPO);
        var tracker = new ShardOpening.Tracker();
        JsonNode metadata = new RepositoryInspector(new FileSystemBlobStore(DELETES_REPO)).indexMetadata("test_deletes_nested");
        var refused = assertThrows(Refused.class, () -> ShardOpening.open(new ShardMaterializer(store), new SelectiveShardFixture.Opener(),
            nestedShard(), tmp.resolve("shard"), "test_deletes_nested", metadata, true, tracker));
        assertSame(Refused.class, refused.getClass());
        assertEquals("preflight", tracker.fetchMode());
        assertTrue(tracker.preflightRefused());
        assertEquals(List.of(expected), checked);

        checked.clear();
        var opened = ShardOpening.open(new ShardMaterializer(store), new SelectiveShardFixture.Opener(), nestedShard(),
            tmp.resolve("shard2"), "test_deletes_nested", metadata, false, new ShardOpening.Tracker());
        opened.rawReader().close();
        assertEquals(List.of(), checked, "the constant off and no flag: no preflight");
        assertFalse(SelectiveFetch.COMMIT_PREFLIGHT);
    }
}
