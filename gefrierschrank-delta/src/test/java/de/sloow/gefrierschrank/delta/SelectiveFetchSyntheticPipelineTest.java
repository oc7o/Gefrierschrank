package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.CommitPoint;
import de.sloow.gefrierschrank.core.repo.FetchPlan;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch.Structure;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.syntheticsource.NonCompoundFixtures;
import de.sloow.gefrierschrank.syntheticsource.SyntheticFetchProbe;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * dump-to-delta with {@code --selective-fetch} on shards whose live documents Elasticsearch's
 * loaders reconstruct: fixture shards rewritten with non-compound segments (every fixture segment is
 * compound) are fetched as far as the probe's plan reaches, export the same table as without the
 * flag, never read a skipped blob and read every fetched blob once; a structure the plan missed is
 * caught by the worker's guard and completes the shard in the same run; the rules that fetch a
 * candidate in full give their token in a stderr note.
 */
class SelectiveFetchSyntheticPipelineTest {

    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final Pattern IGNORED_NOTE = Pattern.compile("note: (\\d+) document\\(s\\) of index idx list fields in _ignored.*");

    private String savedSelective;
    private String savedMinBytes;
    private String savedMinRange;

    @BeforeEach
    void save() {
        savedSelective = System.getProperty(SelectiveFetch.PROPERTY);
        savedMinBytes = System.getProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY);
        savedMinRange = System.getProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY);
        System.clearProperty(SelectiveFetch.PROPERTY);
        System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, "0");
        System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "3");
    }

    @AfterEach
    void restore() {
        restore(SelectiveFetch.PROPERTY, savedSelective);
        restore(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, savedMinBytes);
        restore(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, savedMinRange);
        ShardOpening.prober = SyntheticFetchProbe::probe;
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }

    /** A fixture shard ({@code repository/index[shard]}) rewritten non-compound and served as a repository. */
    record Fixture(SelectiveShardFixture.Shard shard, JsonNode metadata) {
        Set<String> blobsOf(Set<Structure> structures) {
            return shard.blobsOf(name -> {
                Structure structure = SelectiveFetch.structureOf(name);
                return structure != null && structures.contains(structure);
            });
        }
    }

    /** An m3-repo shard, non-compound as Elasticsearch wrote it, served as a repository. */
    static Fixture m3(String index, Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURES.resolve("m3-repo"));
        var inspector = new RepositoryInspector(store);
        Path dir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot(index, 0), dir);
        return new Fixture(SelectiveShardFixture.served(dir, tmp.resolve("repo")), inspector.indexMetadata(index));
    }

    static Fixture nonCompound(String name, Path tmp) throws Exception {
        String repo = name.substring(0, name.indexOf('/'));
        String index = name.substring(name.indexOf('/') + 1, name.indexOf('['));
        int shardId = Integer.parseInt(name.substring(name.indexOf('[') + 1, name.indexOf(']')));
        var store = new FileSystemBlobStore(FIXTURES.resolve(repo));
        var inspector = new RepositoryInspector(store);
        Path compound = tmp.resolve("compound");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot(index, shardId), compound);
        Path rewritten = tmp.resolve("noncompound");
        NonCompoundFixtures.rewrite(compound, rewritten);
        return new Fixture(SelectiveShardFixture.served(rewritten, tmp.resolve("repo")), inspector.indexMetadata(index));
    }

    record Run(RunReport report, String table, List<String> notes, SelectiveShardFixture.CountingStore store) {
        RunReport.ShardReport shard() {
            return report.shards().get(0);
        }

        List<String> notes(String containing) {
            return notes.stream().filter(line -> line.contains(containing)).toList();
        }

        Long ignoredNoteCount() {
            List<Long> counts = new ArrayList<>();
            for (String line : notes) {
                Matcher matcher = IGNORED_NOTE.matcher(line);
                if (matcher.matches()) counts.add(Long.parseLong(matcher.group(1)));
            }
            assertTrue(counts.size() <= 1, "the _ignored note at most once: " + notes);
            return counts.isEmpty() ? null : counts.get(0);
        }
    }

    static Run export(Fixture fixture, Path table, int threads, DumpToDeltaPipeline.ShardReaderOpener opener) throws Exception {
        var store = new SelectiveShardFixture.CountingStore(fixture.shard().repo());
        PrintStream stderr = System.err;
        var captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        RunReport report;
        try {
            report = SelectiveShardFixture.export(fixture.shard(), store, table.toString(),
                SelectiveFetchPipelineTest.threads(threads), opener, fixture.metadata());
        } finally {
            System.setErr(stderr);
        }
        String output = captured.toString(StandardCharsets.UTF_8);
        stderr.print(output);
        List<String> notes = output.lines().filter(line -> line.startsWith("warning:") || line.startsWith("note:")).toList();
        return new Run(report, table.toString(), notes, store);
    }

    static Run export(Fixture fixture, Path table, int threads) throws Exception {
        return export(fixture, table, threads, new SelectiveShardFixture.Opener());
    }

    private static void assertSameTable(Run expected, Run actual) throws Exception {
        assertEquals(RunReport.Outcome.COMMITTED, actual.shard().outcome(), actual.report().toString());
        assertEquals(expected.shard().rowsWritten(), actual.shard().rowsWritten());
        assertEquals(SelectiveShardFixture.rows(expected.table()), SelectiveShardFixture.rows(actual.table()));
        assertEquals(SelectiveShardFixture.commitShape(expected.table()), SelectiveShardFixture.commitShape(actual.table()));
    }

    private static long bytesOf(Fixture fixture, Set<String> blobs) {
        return fixture.shard().snapshot().files().stream().filter(f -> blobs.contains(f.blobName())).mapToLong(RepoFileInfo::length).sum();
    }

    /**
     * Nested roots: the plan keeps postings (Elasticsearch finds a root's children through
     * {@code _nested_path}) and leaves out points, norms, term vectors and vectors, which are
     * neither downloaded nor read.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void aNestedShardFetchesOnlyThePostingsBesideWhatEveryPlanKeeps(int threads, @TempDir Path tmp) throws Exception {
        Fixture fixture = nonCompound("synth-deletes-repo/test_deletes_nested[0]", tmp);
        Run off = export(fixture, tmp.resolve("off"), threads);
        assertEquals("full", off.shard().fetch());

        System.setProperty(SelectiveFetch.PROPERTY, "true");
        Run on = export(fixture, tmp.resolve("on"), threads);
        assertSameTable(off, on);
        assertEquals("selective", on.shard().fetch());
        Set<String> skipped = fixture.blobsOf(Set.of(Structure.POINTS, Structure.NORMS, Structure.TERM_VECTORS, Structure.VECTORS));
        assertFalse(skipped.isEmpty());
        for (String blob : skipped) assertEquals(0, on.store().reads(blob), "never read: " + blob);
        for (String blob : on.store().blobsRead()) assertEquals(1, on.store().reads(blob), "read once: " + blob);
        assertEquals(fixture.shard().downloadBytes() - bytesOf(fixture, skipped), on.shard().bytesFetched());
        assertEquals(1, on.notes("selective fetch kept postings (2 probe call(s), ").size(), on.notes().toString());
    }

    /**
     * A plan that misses a structure: the probe is told nothing was touched, so the nested shard is
     * fetched without its postings; the worker's guard trips on the first root, the attempt fails,
     * and the shard completes its download and commits once in the same run.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void aStructureThePlanMissedCompletesTheShardInTheSameRun(int threads, @TempDir Path tmp) throws Exception {
        Fixture fixture = nonCompound("synth-deletes-repo/test_deletes_nested[0]", tmp);
        Run off = export(fixture, tmp.resolve("off"), threads);

        System.setProperty(SelectiveFetch.PROPERTY, "true");
        AtomicInteger probes = new AtomicInteger();
        ShardOpening.prober = (dir, index, metadata) -> {
            probes.incrementAndGet();
            return new SyntheticFetchProbe.Result(Set.of(), false);
        };
        var opener = new SelectiveShardFixture.Opener();
        Run missed = export(fixture, tmp.resolve("missed"), threads, opener);
        assertSameTable(off, missed);
        assertEquals("completed", missed.shard().fetch());
        assertEquals(1, probes.get());
        assertEquals(1, opener.prunedOpens.get());
        assertEquals(1, opener.opens.get(), "the retry opens the completed directory normally");
        assertEquals(fixture.shard().downloadBytes(), missed.shard().bytesFetched());
        for (String blob : missed.store().blobsRead()) assertEquals(1, missed.store().reads(blob), "read once: " + blob);
        assertEquals(List.of(), List.copyOf(SelectiveShardFixture.unreferencedDataFiles(missed.table())));
    }

    /**
     * {@code _source} disabled after the fact: the rewrite's leaf check keeps postings and norms;
     * the {@code _ignored} note is printed once, after the commit. A failing attempt is completed and
     * retried, and the note carries the retry's count only.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void aShardWhoseSourceWasDisabledLaterCountsItsIgnoredDocumentsOnceAlsoWhenRetried(int threads, @TempDir Path tmp)
            throws Exception {
        Fixture fixture = nonCompound("synthcov-repo/cov_srcoff_native[1]", tmp);
        Run off = export(fixture, tmp.resolve("off"), threads);
        Long counted = off.shard().ignoredFieldDocuments();
        assertNotEquals(null, counted);
        assertEquals(counted, off.ignoredNoteCount());

        System.setProperty(SelectiveFetch.PROPERTY, "true");
        Run on = export(fixture, tmp.resolve("on"), threads);
        assertSameTable(off, on);
        assertEquals("selective", on.shard().fetch());
        assertEquals(counted, on.shard().ignoredFieldDocuments());
        assertEquals(counted, on.ignoredNoteCount());
        Set<String> skipped = fixture.blobsOf(Set.of(Structure.POINTS, Structure.TERM_VECTORS, Structure.VECTORS));
        for (String blob : skipped) assertEquals(0, on.store().reads(blob), "never read: " + blob);
        assertEquals(1, on.notes("selective fetch kept postings, norms (3 probe call(s), ").size(), on.notes().toString());

        Run retried = export(fixture, tmp.resolve("retried"), threads, new LastLiveDocumentFailsOnce());
        assertSameTable(off, retried);
        assertEquals("completed", retried.shard().fetch());
        assertEquals(counted, retried.shard().ignoredFieldDocuments());
        assertEquals(counted, retried.ignoredNoteCount(), "the retry's count, once");
    }

    /**
     * The first reader fails reading the metadata of the shard's last live document, which is read
     * after its source, so every other document was reconstructed (and counted) before.
     */
    static final class LastLiveDocumentFailsOnce extends SelectiveShardFixture.Opener {
        private final AtomicInteger readers = new AtomicInteger();

        private ShardReader flaky(ShardReader reader) {
            return readers.incrementAndGet() > 1 ? reader : new LastDocumentFails(reader);
        }

        @Override
        public ShardReader open(ShardSnapshot shard, Path shardDir) throws Exception {
            return flaky(super.open(shard, shardDir));
        }

        @Override
        public ShardReader openPruned(ShardSnapshot shard, Path shardDir, JsonNode metadata) throws Exception {
            return flaky(super.openPruned(shard, shardDir, metadata));
        }
    }

    private record LastDocumentFails(ShardReader delegate) implements ShardReader {
        @Override public String runtimeName() { return delegate.runtimeName(); }
        @Override public int docCount() throws java.io.IOException { return delegate.docCount(); }
        @Override public boolean isLive(int docId) throws java.io.IOException { return delegate.isLive(docId); }
        @Override public boolean storedSourceAbsent(int docId) throws java.io.IOException { return delegate.storedSourceAbsent(docId); }
        @Override public void readSource(int docId, SourceConsumer consumer) throws java.io.IOException { delegate.readSource(docId, consumer); }
        @Override public void close() throws java.io.IOException { delegate.close(); }

        @Override
        public de.sloow.gefrierschrank.core.lucene.DocumentMetadata documentMetadata(int docId) throws java.io.IOException {
            int last = delegate.docCount() - 1;
            while (last > 0 && !delegate.isLive(last)) last--;
            if (docId == last) throw new SelectiveShardFixture.InjectedReadFailure();
            return delegate.documentMetadata(docId);
        }

        @Override
        public ShardReader fork() throws java.io.IOException {
            ShardReader fork = delegate.fork();
            return fork == null ? null : new LastDocumentFails(fork);
        }
    }

    /** Rows merged from stored and source-less documents, and an index created with the source mode disabled. */
    @ParameterizedTest
    @ValueSource(strings = {"synthcov-repo/cov_srcoff_merged[1]", "synthcov-repo/cov_srcmode_native[0]",
        "synthcov-repo/cov_srcmode_native[1]", "synthcov-repo/cov_nested_synth[0]", "synth-repo/test_tsdb_synth[0]"})
    void candidatesExportTheTableTheFullDownloadExports(String name, @TempDir Path tmp) throws Exception {
        Fixture fixture = nonCompound(name, tmp);
        Run off = export(fixture, tmp.resolve("off"), 1);
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        Run on = export(fixture, tmp.resolve("on"), 1);
        assertSameTable(off, on);
        assertEquals("selective", on.shard().fetch(), name);
        assertEquals(off.shard().ignoredFieldDocuments(), on.shard().ignoredFieldDocuments());
        for (String blob : on.store().blobsRead()) assertEquals(1, on.store().reads(blob), "read once: " + blob);
        assertTrue(on.shard().bytesFetched() < off.shard().bytesFetched(), name + " skips something");
    }

    /**
     * Shards Elasticsearch wrote without compound files: each keeps what its loaders read and exports
     * the same table. m3_logsdb (ZSTD stored fields) and m3_dense (Elasticsearch's vector formats)
     * cannot be opened by this JVM's ambient reader at all; the CLI's fixture digests cover them.
     */
    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"m3_tsdb,no index structure", "m3_types,no index structure",
        "m3_doc_count,postings", "m3_nested,postings", "m3_sparse,'postings, term_vectors'", "m3_src_off,no index structure"})
    void elasticsearchsNonCompoundShardsKeepWhatTheirLoadersRead(String index, String kept, @TempDir Path tmp) throws Exception {
        Fixture fixture = m3(index, tmp);
        Run off = export(fixture, tmp.resolve("off"), 1);
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        Run on = export(fixture, tmp.resolve("on"), 1);
        assertSameTable(off, on);
        assertEquals("selective", on.shard().fetch());
        assertEquals(1, on.notes("selective fetch kept " + kept + " (").size(), on.notes().toString());
        assertTrue(on.shard().bytesFetched() < off.shard().bytesFetched(), index + " skips something");
        for (String blob : on.store().blobsRead()) assertEquals(1, on.store().reads(blob), "read once: " + blob);
    }

    /**
     * A sample that misses a per-document read: m3_sparse holds sparse vectors only from document
     * 200 on, which Elasticsearch reads through term vectors after a postings match. With the probe's
     * term-vector finding dropped, the plan keeps postings only; the guard trips on the first
     * document with a value and the shard completes in the same run.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void aPerDocumentReadOutsideThePlanCompletesTheShard(int threads, @TempDir Path tmp) throws Exception {
        Fixture fixture = m3("m3_sparse", tmp);
        Run off = export(fixture, tmp.resolve("off"), threads);
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        ShardOpening.prober = (dir, index, metadata) -> {
            SyntheticFetchProbe.Result real = SyntheticFetchProbe.probe(dir, index, metadata);
            Set<String> touched = new java.util.HashSet<>(real.touched());
            touched.remove(Structure.TERM_VECTORS.name());
            return new SyntheticFetchProbe.Result(touched, real.failed());
        };
        Run missed = export(fixture, tmp.resolve("missed"), threads);
        assertSameTable(off, missed);
        assertEquals("completed", missed.shard().fetch());
        assertEquals(1, missed.notes("selective fetch kept postings (2 probe call(s), ").size(), missed.notes().toString());
        assertEquals(fixture.shard().downloadBytes(), missed.shard().bytesFetched());
        for (String blob : missed.store().blobsRead()) assertEquals(1, missed.store().reads(blob), "read once: " + blob);
    }

    /** F2: the fixture's own compound shard has nothing to prune; no probe runs. */
    @Test
    void aCompoundShardIsFetchedInFullWithoutAProbe(@TempDir Path tmp) throws Exception {
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        AtomicInteger probes = new AtomicInteger();
        ShardOpening.prober = (dir, index, metadata) -> {
            probes.incrementAndGet();
            return SyntheticFetchProbe.probe(dir, index, metadata);
        };
        var store = new FileSystemBlobStore(FIXTURES.resolve("synth-deletes-repo"));
        var inspector = new RepositoryInspector(store);
        Path compound = tmp.resolve("compound");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot("test_deletes_nested", 0), compound);
        var fixture = new Fixture(SelectiveShardFixture.served(compound, tmp.resolve("repo")),
            inspector.indexMetadata("test_deletes_nested"));
        Run on = export(fixture, tmp.resolve("on"), 1);
        assertEquals("full", on.shard().fetch());
        assertEquals(0, probes.get());
        assertEquals(List.of("note: shard 0 of idx: fetched in full (below-threshold)"), on.notes("fetched in full"));
    }

    /** F2 at the default threshold: the rewritten fixture shard holds far less than 16 MiB of prunable structures. */
    @Test
    void aSmallShardIsFetchedInFullAtTheDefaultThreshold(@TempDir Path tmp) throws Exception {
        Fixture fixture = nonCompound("synth-deletes-repo/test_deletes_nested[0]", tmp);
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        System.clearProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY);
        Run on = export(fixture, tmp.resolve("on"), 1);
        assertEquals("full", on.shard().fetch());
        assertEquals(List.of("note: shard 0 of idx: fetched in full (below-threshold)"), on.notes("fetched in full"));
    }

    /** F5: a probe that fails with nothing to fetch fetches the shard in full; its export is the normal one. */
    @Test
    void aFailedProbeFetchesInFull(@TempDir Path tmp) throws Exception {
        Fixture fixture = nonCompound("synth-deletes-repo/test_deletes_nested[0]", tmp);
        Run off = export(fixture, tmp.resolve("off"), 1);
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        ShardOpening.prober = (dir, index, metadata) -> new SyntheticFetchProbe.Result(Set.of(), true);
        Run on = export(fixture, tmp.resolve("on"), 1);
        assertSameTable(off, on);
        assertEquals("full", on.shard().fetch());
        assertEquals(fixture.shard().downloadBytes(), on.shard().bytesFetched());
        assertEquals(List.of("note: shard 0 of idx: fetched in full (probe-failed)"), on.notes("fetched in full"));
    }

    /** A failing round that touched a structure is not conclusive: the structure is fetched and the probe asked again. */
    @Test
    void aFailingRoundThatTouchedAStructureIsRetriedWithIt(@TempDir Path tmp) throws Exception {
        Fixture fixture = nonCompound("synth-deletes-repo/test_deletes_nested[0]", tmp);
        Run off = export(fixture, tmp.resolve("off"), 1);
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        AtomicInteger probes = new AtomicInteger();
        ShardOpening.prober = (dir, index, metadata) -> probes.incrementAndGet() == 1
            ? new SyntheticFetchProbe.Result(Set.of("NORMS"), true) : SyntheticFetchProbe.probe(dir, index, metadata);
        Run on = export(fixture, tmp.resolve("on"), 1);
        assertSameTable(off, on);
        assertEquals("selective", on.shard().fetch());
        assertEquals(3, probes.get());
        assertEquals(1, on.notes("selective fetch kept postings, norms (3 probe call(s), ").size(), on.notes().toString());
    }

    /** F3: segments without {@code _primary_term} tell roots from nested children through postings only. */
    @Test
    void legacyNestedMarkersFetchInFull(@TempDir Path tmp) throws Exception {
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var shard = SelectiveShardFixture.shard(tmp.resolve("legacy"), writer -> {
            for (int id = 0; id < 20; id++) {
                Document child = new Document();
                child.add(new StringField("_nested_path", "children", Field.Store.NO));
                Document root = new Document();
                root.add(new StoredField("_id", new BytesRef(("id-" + id).getBytes(StandardCharsets.UTF_8))));
                root.add(new StoredField("_source", new BytesRef(("{\"n\":" + id + "}").getBytes(StandardCharsets.UTF_8))));
                root.add(new TextField("text", "words " + id, Field.Store.NO));
                writer.addDocuments(List.of(child, root));
            }
        });
        var tracker = new ShardOpening.Tracker();
        var opened = ShardOpening.open(new ShardMaterializer(new SelectiveShardFixture.CountingStore(shard.repo())),
            new SelectiveShardFixture.Opener(), shard.snapshot(), tmp.resolve("dir"), "idx", LOGSDB, tracker);
        opened.rawReader().close();
        assertEquals(FetchPlan.Mode.FULL, opened.plan());
        assertEquals(SelectiveFetch.FULL_LEGACY_NESTED, opened.fullReason());
        assertNull(opened.probes());
        assertEquals("full", tracker.fetchMode());
    }

    private static final JsonNode LOGSDB = SelectiveShardFixture.parse("{\"settings\":{\"index.mode\":\"logsdb\"},\"mappings\":{}}");

    /** F1: vectors read from the vector files (here: excluded from a stored source) take the full plan first. */
    @Test
    void vectorsReadFromTheVectorFilesFetchInFull(@TempDir Path tmp) throws Exception {
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        JsonNode vectors = new RepositoryInspector(new FileSystemBlobStore(FIXTURES.resolve("vec-repo"))).indexMetadata("vec_root");
        assertTrue(SelectiveEligibility.vectorsFromVectorFiles("vec_root", vectors));
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("s"));
        var opener = new SelectiveShardFixture.Opener() {
            @Override
            public CommitPoint readCommitPoint(ShardSnapshot snapshot, Path shardDir) throws Exception {
                CommitPoint commit = super.readCommitPoint(snapshot, shardDir);
                // As if the index's _source had been disabled later: a segment with live documents but no _source.
                List<CommitPoint.Segment> segments = commit.segments().stream().map(s -> new CommitPoint.Segment(s.name(),
                    s.compound(), s.maxDoc(), s.delCount(), s.softDelCount(), s.softDeletesFlagged(), s.softDeletesField(),
                    false, s.hasPrimaryTerm(), s.hasNestedPath(), s.hasLegacyType(), s.files())).collect(Collectors.toList());
                return new CommitPoint(commit.segmentsFile(), segments);
            }
        };
        CommitPointPreflight.CoverageCheck previous = CommitPointPreflight.coverage;
        CommitPointPreflight.coverage = (dir, index, metadata, segments) -> {};
        try {
            var opened = ShardOpening.open(new ShardMaterializer(new SelectiveShardFixture.CountingStore(shard.repo())), opener,
                shard.snapshot(), tmp.resolve("dir"), "vec_root", vectors, new ShardOpening.Tracker());
            opened.rawReader().close();
            assertEquals(FetchPlan.Mode.FULL, opened.plan());
            assertEquals(SelectiveFetch.FULL_VECTORS, opened.fullReason());
        } finally {
            CommitPointPreflight.coverage = previous;
        }
    }

    /** Without the flag nothing of this runs: no commit read, no probe, no note. */
    @Test
    void withoutTheFlagNothingChanges(@TempDir Path tmp) throws Exception {
        Fixture fixture = nonCompound("synth-deletes-repo/test_deletes_nested[0]", tmp);
        AtomicInteger probes = new AtomicInteger();
        ShardOpening.prober = (dir, index, metadata) -> {
            probes.incrementAndGet();
            return new SyntheticFetchProbe.Result(Set.of(), true);
        };
        var opener = new SelectiveShardFixture.Opener();
        Run off = export(fixture, tmp.resolve("off"), 1, opener);
        assertEquals("full", off.shard().fetch());
        assertEquals(0, probes.get());
        assertEquals(0, opener.commitReads.get());
        assertEquals(List.of(), off.notes("note: shard 0 of idx: "));
    }

    /** The reader the probe's plan is opened with: the ambient pruned reader, which never reads postings. */
    @Test
    void theAmbientReaderOfAProbedShardIsThePrunedOne(@TempDir Path tmp) throws Exception {
        Fixture fixture = nonCompound("synth-deletes-repo/test_deletes_nested[0]", tmp);
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var tracker = new ShardOpening.Tracker();
        var opener = new SelectiveShardFixture.Opener();
        var opened = ShardOpening.open(new ShardMaterializer(new SelectiveShardFixture.CountingStore(fixture.shard().repo())),
            opener, fixture.shard().snapshot(), tmp.resolve("dir"), "idx", fixture.metadata(), tracker);
        try (ShardReader reader = opened.rawReader()) {
            assertEquals(1, opener.prunedOpens.get());
            assertEquals(Set.of(Structure.POSTINGS), opened.probes().kept());
            assertEquals(2, opened.probes().calls());
            assertNull(opened.fullReason());
            assertEquals("selective", tracker.fetchMode());
            assertTrue(reader.docCount() > 0);
        }
    }
}
