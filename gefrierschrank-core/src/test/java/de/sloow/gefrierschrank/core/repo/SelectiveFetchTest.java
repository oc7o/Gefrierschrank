package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SelectiveFetchTest {

    private String saved;

    @BeforeEach
    void save() {
        saved = System.getProperty(SelectiveFetch.PROPERTY);
    }

    @AfterEach
    void restore() {
        if (saved == null) System.clearProperty(SelectiveFetch.PROPERTY);
        else System.setProperty(SelectiveFetch.PROPERTY, saved);
    }

    @Test
    void theSwitchIsOffByDefaultAndAcceptsOnlyTrueOrFalse() {
        System.clearProperty(SelectiveFetch.PROPERTY);
        assertFalse(SelectiveFetch.enabled());
        assertFalse(SelectiveFetch.preflightEnabled());
        System.setProperty(SelectiveFetch.PROPERTY, " TRUE ");
        assertTrue(SelectiveFetch.enabled());
        assertTrue(SelectiveFetch.preflightEnabled());
        System.setProperty(SelectiveFetch.PROPERTY, "False");
        assertFalse(SelectiveFetch.enabled());
        System.setProperty(SelectiveFetch.PROPERTY, "yes");
        var invalid = assertThrows(SelectiveFetch.InvalidSettingException.class, SelectiveFetch::enabled);
        assertEquals("-Dgefrierschrank.download.selective must be true or false", invalid.getMessage());
        assertFalse(SelectiveFetch.COMMIT_PREFLIGHT);
    }

    @Test
    void namesParseLikeLucenesIndexFileNames() {
        assertEquals("cfs", SelectiveFetch.extension("_0.cfs"));
        assertEquals("dvd", SelectiveFetch.extension("_0_1_Lucene90_0.dvd"));
        assertNull(SelectiveFetch.extension("segments_5"));
        assertEquals("_0", SelectiveFetch.segmentName("_0.cfs"));
        assertEquals("_0", SelectiveFetch.segmentName("_0_1.fnm"));
        assertEquals("_0", SelectiveFetch.segmentName("_0_Lucene90_0.dvd"));
        assertEquals("_0", SelectiveFetch.segmentName("_0_1_Lucene90_0.dvd"));
        assertEquals("_a", SelectiveFetch.segmentName("_a_ES812Postings_0.tim"));
        assertEquals("_1z", SelectiveFetch.segmentName("_1z.liv"));
        assertEquals("segments", SelectiveFetch.segmentName("segments_5"));
    }

    @Test
    void commitFilesAreTheSegmentsFileSegmentInfosFieldInfosAndCompoundFiles() {
        for (String name : List.of("segments_5", "_0.si", "_0.fnm", "_0_1.fnm", "_0.cfs", "_0.cfe"))
            assertTrue(SelectiveFetch.isCommitFile(name), name);
        for (String name : List.of("_0.fdt", "_0.liv", "_0_Lucene90_0.dvd", "_0.tim", "write.lock"))
            assertFalse(SelectiveFetch.isCommitFile(name), name);
    }

    static RepoFileInfo file(String name, long length) {
        return new RepoFileInfo("__" + name, name, length, null, "10.3.2", null);
    }

    static final List<String> SEGMENT_0 = List.of("_0.si", "_0.fnm", "_0.fdt", "_0.fdx", "_0.fdm", "_0.tim", "_0.tip",
        "_0.tmd", "_0.doc", "_0.pos", "_0.kdd", "_0.kdi", "_0.kdm", "_0.nvd", "_0.nvm", "_0.tvd", "_0.tvx", "_0.tvm",
        "_0.vec", "_0.vex", "_0.vem", "_0_Lucene90_0.dvd", "_0_Lucene90_0.dvm", "_0_ES87BloomFilter_0.bfi",
        "_0_1.liv", "_0.unknownext");
    static final List<String> SEGMENT_1 = List.of("_1.si", "_1.cfs", "_1.cfe", "_1_2.liv", "_1_1.fnm", "_1_1_Lucene90_0.dvd");

    static ShardSnapshot shard() {
        var files = new ArrayList<RepoFileInfo>();
        files.add(file("segments_5", 300));
        long length = 10;
        for (String name : SEGMENT_0) files.add(file(name, length++));
        for (String name : SEGMENT_1) files.add(file(name, length++));
        files.add(file("_7.tim", 1000));
        long total = files.stream().mapToLong(RepoFileInfo::length).sum();
        return new ShardSnapshot("repo", "p/", "snap", 1, total, files);
    }

    static CommitPoint.Segment segment(String name, boolean compound, boolean hasSource, long live, boolean primaryTerm,
                                       boolean nestedPath, boolean legacyType) {
        int maxDoc = 10;
        boolean unknown = live == CommitPoint.UNKNOWN;
        int deleted = unknown ? 0 : (int) (maxDoc - live);
        return new CommitPoint.Segment(name, compound, maxDoc, deleted, 0, !unknown, true, hasSource, primaryTerm,
            nestedPath, legacyType, Set.of());
    }

    static CommitPoint commit(CommitPoint.Segment... segments) {
        return new CommitPoint("segments_5", List.of(segments));
    }

    static Set<String> skipped(FetchPlan plan, ShardSnapshot shard) {
        return shard.files().stream().filter(f -> !plan.include().test(f)).map(RepoFileInfo::physicalName).collect(Collectors.toSet());
    }

    @Test
    void theSelectivePlanSkipsPrunedExtensionsOfNonCompoundSegmentsOnly() {
        ShardSnapshot shard = shard();
        FetchPlan plan = SelectiveFetch.plan(commit(segment("_0", false, true, 10, true, false, false),
            segment("_1", true, true, 5, true, false, false)), shard, true);
        assertEquals(FetchPlan.Mode.SELECTIVE, plan.mode());
        assertEquals(Set.of("_0.tim", "_0.tip", "_0.tmd", "_0.doc", "_0.pos", "_0.kdd", "_0.kdi", "_0.kdm", "_0.nvd",
            "_0.nvm", "_0.tvd", "_0.tvx", "_0.tvm", "_0.vec", "_0.vex", "_0.vem", "_0_ES87BloomFilter_0.bfi"), skipped(plan, shard),
            "the compound segment, unknown extensions and a file of no commit segment (_7.tim) are kept");
        long kept = shard.files().stream().filter(plan.include()).mapToLong(RepoFileInfo::length).sum();
        assertEquals(kept, plan.plannedBytes());
        assertEquals(kept, SelectiveFetch.estimateBytes(shard) + 1000, "the estimate drops every pruned extension by name");
    }

    @Test
    void theTruthTableFallsBackToTheFullPlan() {
        ShardSnapshot shard = shard();
        var stored = segment("_0", false, true, 10, true, false, false);
        assertEquals(FetchPlan.Mode.FULL, SelectiveFetch.plan(commit(stored), shard, false).mode(), "not eligible");
        assertTrue(SelectiveFetch.plan(commit(stored), shard, false).include().test(file("_0.tim", 1)));
        assertEquals(FetchPlan.Mode.SELECTIVE, SelectiveFetch.plan(commit(stored), shard, true).mode());

        for (long live : new long[] {1, CommitPoint.UNKNOWN}) {
            var sourceless = segment("_1", false, false, live, true, false, false);
            assertEquals(FetchPlan.Mode.FULL, SelectiveFetch.plan(commit(stored, sourceless), shard, true).mode(),
                "live documents without stored _source are reconstructed: " + live);
        }
        var emptySourceless = segment("_1", false, false, 0, true, false, false);
        assertEquals(FetchPlan.Mode.SELECTIVE, SelectiveFetch.plan(commit(stored, emptySourceless), shard, true).mode(),
            "a fully deleted segment is never read");

        for (boolean nested : new boolean[] {true, false}) {
            var legacy = segment("_1", false, true, 10, false, nested, !nested);
            assertEquals(FetchPlan.Mode.FULL, SelectiveFetch.plan(commit(stored, legacy), shard, true).mode(),
                "legacy nested markers are read from postings");
            var marked = segment("_1", false, true, 10, true, nested, !nested);
            assertEquals(FetchPlan.Mode.SELECTIVE, SelectiveFetch.plan(commit(stored, marked), shard, true).mode());
        }
        var noMarkers = segment("_1", false, true, 10, false, false, false);
        assertEquals(FetchPlan.Mode.SELECTIVE, SelectiveFetch.plan(commit(stored, noMarkers), shard, true).mode());
    }

    @Test
    void everyPrunedExtensionButTheStoredFieldsSidecarBelongsToOneStructure() {
        var mapped = new java.util.HashSet<>(SelectiveFetch.STRUCTURES.keySet());
        mapped.add("sfbf");
        mapped.add("sfbfm");
        assertEquals(SelectiveFetch.PRUNED_EXTENSIONS, mapped);
        assertEquals(Set.of(SelectiveFetch.Structure.values()), Set.copyOf(SelectiveFetch.STRUCTURES.values()));
        assertEquals(SelectiveFetch.Structure.POSTINGS, SelectiveFetch.structureOf("_0_ES87BloomFilter_0.bfi"));
        assertEquals(SelectiveFetch.Structure.POSTINGS, SelectiveFetch.structureOf("_0_Lucene103_0.psm"));
        assertEquals(SelectiveFetch.Structure.NORMS, SelectiveFetch.structureOf("_0.nvd"));
        assertEquals(SelectiveFetch.Structure.VECTORS, SelectiveFetch.structureOf("_0_ES818BinaryQuantizedVectorsFormat_0.veb"));
        for (String name : List.of("_0.fdt", "_0.sfbf", "_0_1.liv", "_0_Lucene90_0.dvd", "segments_3", "_0.cfs"))
            assertNull(SelectiveFetch.structureOf(name), name);
    }

    static final List<String> SYNTHETIC_0 = List.of("_0.si", "_0.fnm", "_0.fdt", "_0.fdx", "_0.fdm", "_0.sfbf", "_0.sfbfm",
        "_0_Lucene103_0.doc", "_0_Lucene103_0.tim", "_0_Lucene103_0.tip", "_0_Lucene103_0.tmd", "_0_Lucene103_0.psm",
        "_0.kdd", "_0.kdi", "_0.kdm", "_0.nvd", "_0.nvm", "_0.tvd", "_0.tvx", "_0.tvm", "_0_Lucene99HnswVectorsFormat_0.vec",
        "_0_Lucene99HnswVectorsFormat_0.vex", "_0_Lucene99HnswVectorsFormat_0.vem", "_0_Lucene90_0.dvd", "_0_Lucene90_0.dvm",
        "_0_1.liv");

    /** One non-compound segment of {@link #SYNTHETIC_0} (100 bytes per file) and the compound {@link #SEGMENT_1}. */
    static ShardSnapshot syntheticShard() {
        var files = new ArrayList<RepoFileInfo>();
        files.add(file("segments_5", 300));
        for (String name : SYNTHETIC_0) files.add(file(name, 100));
        for (String name : SEGMENT_1) files.add(file(name, 100));
        long total = files.stream().mapToLong(RepoFileInfo::length).sum();
        return new ShardSnapshot("repo", "p/", "snap", 1, total, files);
    }

    static CommitPoint syntheticCommit(boolean primaryTerm, long live) {
        return commit(new CommitPoint.Segment("_0", false, 10, (int) (10 - live), 0, true, true, false, primaryTerm, true, false,
                Set.copyOf(SYNTHETIC_0)),
            new CommitPoint.Segment("_1", true, 10, 0, 0, true, true, false, true, false, false, Set.copyOf(SEGMENT_1)));
    }

    @Test
    void theSyntheticPlanKeepsEverythingButTheStructuresOfNonCompoundSegments() {
        String saved = System.getProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY);
        try {
            System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, "1500");
            ShardSnapshot shard = syntheticShard();
            FetchPlan first = SelectiveFetch.syntheticPlan(syntheticCommit(true, 10), shard, false);
            assertEquals(FetchPlan.Mode.SYNTHETIC_CANDIDATE, first.mode());
            assertNull(first.reason());
            Set<String> structures = SYNTHETIC_0.stream().filter(n -> SelectiveFetch.structureOf(n) != null).collect(Collectors.toSet());
            assertEquals(structures, skipped(first, shard), "the stored-fields sidecar, compound files and the rest are kept");
            assertTrue(first.include().test(file("_0.sfbf", 1)) && first.include().test(file("_0.sfbfm", 1)));
            assertEquals(shard.totalSize() - 100L * structures.size(), first.plannedBytes());

            FetchPlan postings = SelectiveFetch.keeping(syntheticCommit(true, 10), shard, Set.of(SelectiveFetch.Structure.POSTINGS));
            assertEquals(FetchPlan.Mode.SELECTIVE, postings.mode());
            assertEquals(structures.stream().filter(n -> SelectiveFetch.structureOf(n) != SelectiveFetch.Structure.POSTINGS)
                .collect(Collectors.toSet()), skipped(postings, shard));
            FetchPlan all = SelectiveFetch.keeping(syntheticCommit(true, 10), shard, Set.of(SelectiveFetch.Structure.values()));
            assertEquals(Set.of(), skipped(all, shard));
        } finally {
            if (saved == null) System.clearProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY);
            else System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, saved);
        }
    }

    @Test
    void theSyntheticPlanFetchesInFullForTheRulesInOrder() {
        String saved = System.getProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY);
        try {
            ShardSnapshot shard = syntheticShard();
            System.clearProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY);
            assertEquals(16L << 20, SelectiveFetch.syntheticProbeMinBytes());
            assertEquals(SelectiveFetch.FULL_BELOW_THRESHOLD, SelectiveFetch.syntheticPlan(syntheticCommit(true, 10), shard, false).reason(),
                "F2: 1.6 KB of structures is below 16 MiB");
            System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, "1600");
            assertEquals(FetchPlan.Mode.SYNTHETIC_CANDIDATE, SelectiveFetch.syntheticPlan(syntheticCommit(true, 10), shard, false).mode());
            System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, "1601");
            assertEquals(SelectiveFetch.FULL_BELOW_THRESHOLD, SelectiveFetch.syntheticPlan(syntheticCommit(true, 10), shard, false).reason());
            System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, "0");
            assertEquals(SelectiveFetch.FULL_BELOW_THRESHOLD, SelectiveFetch.syntheticPlan(syntheticCommit(true, 0), shard, false).reason(),
                "F2: the only non-compound segment has no live document");

            FetchPlan vectors = SelectiveFetch.syntheticPlan(syntheticCommit(false, 10), shard, true);
            assertEquals(FetchPlan.Mode.FULL, vectors.mode());
            assertEquals(SelectiveFetch.FULL_VECTORS, vectors.reason(), "F1 first");
            FetchPlan legacy = SelectiveFetch.syntheticPlan(syntheticCommit(false, 10), shard, false);
            assertEquals(SelectiveFetch.FULL_LEGACY_NESTED, legacy.reason(), "F3: _nested_path without _primary_term");
            assertTrue(legacy.include().test(file("_0.tim", 1)));

            System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, "-1");
            var invalid = assertThrows(SelectiveFetch.InvalidSettingException.class, SelectiveFetch::syntheticProbeMinBytes);
            assertEquals("-Dgefrierschrank.download.selective.syntheticMinBytes must be a number of bytes >= 0", invalid.getMessage());
            System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, "16m");
            assertThrows(SelectiveFetch.InvalidSettingException.class, SelectiveFetch::syntheticProbeMinBytes);
        } finally {
            if (saved == null) System.clearProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY);
            else System.setProperty(SelectiveFetch.SYNTHETIC_PROBE_MIN_BYTES_PROPERTY, saved);
        }
    }

    @Test
    void liveDocumentsCountSoftDeletesOnlyWhenLuceneCountedThem() {
        var flagged = new CommitPoint.Segment("_0", false, 10, 2, 3, true, true, true, true, false, false, Set.of());
        assertEquals(5, flagged.liveDocs());
        var unflagged = new CommitPoint.Segment("_0", false, 10, 2, 0, false, true, true, true, false, false, Set.of());
        assertEquals(CommitPoint.UNKNOWN, unflagged.liveDocs());
        var noSoftDeletes = new CommitPoint.Segment("_0", false, 10, 2, 0, false, false, true, true, false, false, Set.of());
        assertEquals(8, noSoftDeletes.liveDocs());
    }
}
