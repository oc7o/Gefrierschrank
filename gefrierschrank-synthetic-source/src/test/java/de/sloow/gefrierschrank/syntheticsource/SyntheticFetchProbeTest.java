package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch.Structure;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The fetch probe's fixpoint over fixture shards rewritten with non-compound segments (every
 * fixture segment is compound, and compound segments are never pruned): starting from a directory
 * without any structure, each probe call reports what Elasticsearch's reconstruction asked for, the
 * test restores those files, and the plan is the set once a call asks for nothing new. Pins the
 * plan and the number of probe calls per fixture shard, and that a shard reconstruction refuses
 * per segment makes the probe fail with nothing to fetch.
 */
class SyntheticFetchProbeTest {

    record Plan(Set<Structure> kept, int calls, boolean failed) {
        @Override
        public String toString() {
            return new TreeSet<>(kept) + " in " + calls + (failed ? " (failed)" : "");
        }
    }

    record Shard(String index, Path full, Path pruned, JsonNode metadata) {}

    /** {@code name}: repository/index[shard]; the rewritten shard in {@code full}, without its structures in {@code pruned}. */
    static Shard shard(String name, Path tmp) throws Exception {
        String repo = name.substring(0, name.indexOf('/'));
        String index = name.substring(name.indexOf('/') + 1, name.indexOf('['));
        int shardId = Integer.parseInt(name.substring(name.indexOf('[') + 1, name.indexOf(']')));
        var store = new FileSystemBlobStore(Path.of("src/test/resources/fixtures", repo));
        var inspector = new RepositoryInspector(store);
        Path compound = tmp.resolve("compound");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot(index, shardId), compound);
        Path full = tmp.resolve("full");
        // m3-repo's segments are non-compound as Elasticsearch wrote them; every other fixture's are rewritten.
        if (repo.equals("m3-repo")) Files.move(compound, full);
        else rewriteNonCompound(compound, full);
        Path pruned = Files.createDirectories(tmp.resolve("pruned"));
        try (Stream<Path> files = Files.list(full)) {
            for (Path file : files.toList()) {
                if (SelectiveFetch.structureOf(file.getFileName().toString()) == null) Files.copy(file, pruned.resolve(file.getFileName()));
            }
        }
        return new Shard(index, full, pruned, inspector.indexMetadata(index));
    }

    static void rewriteNonCompound(Path from, Path to) throws Exception {
        IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            Class.forName("de.sloow.gefrierschrank.syntheticsource.isolated.NonCompoundShards", true, loader)
                .getMethod("rewrite", String.class, String.class).invoke(null, from.toString(), to.toString());
        } catch (InvocationTargetException e) {
            throw e.getTargetException() instanceof Exception cause ? cause : e;
        } finally {
            thread.setContextClassLoader(previous);
        }
        try (Stream<Path> files = Files.list(to)) {
            assertTrue(files.noneMatch(f -> f.toString().endsWith(".cfs")), "non-compound");
        }
    }

    /** The probe's fixpoint as ShardOpening runs it, "fetching" a structure by copying its files back. */
    static Plan fixpoint(Shard shard) throws IOException {
        Set<Structure> kept = EnumSet.noneOf(Structure.class);
        int calls = 0;
        while (true) {
            SyntheticFetchProbe.Result result = SyntheticFetchProbe.probe(shard.pruned(), shard.index(), shard.metadata());
            calls++;
            Set<String> names = Arrays.stream(Structure.values()).map(Enum::name).collect(Collectors.toSet());
            assertTrue(names.containsAll(result.touched()), "structure names only: " + result.touched());
            Set<Structure> missing = EnumSet.noneOf(Structure.class);
            result.touched().forEach(name -> missing.add(Structure.valueOf(name)));
            missing.removeAll(kept);
            if (missing.isEmpty()) return new Plan(kept, calls, result.failed());
            assertTrue(calls <= Structure.values().length, "converges");
            try (Stream<Path> files = Files.list(shard.full())) {
                for (Path file : files.toList()) {
                    if (missing.contains(SelectiveFetch.structureOf(file.getFileName().toString()))) {
                        Files.copy(file, shard.pruned().resolve(file.getFileName()));
                    }
                }
            }
            kept.addAll(missing);
        }
    }

    private static final Set<Structure> REWRITE = Set.of(Structure.POSTINGS, Structure.NORMS);

    static Stream<Arguments> fixtureShards() {
        return Stream.of(
            Arguments.of("synth-repo/test_plain_synth[0]", Set.of(), 1),
            Arguments.of("synth-repo/test_tsdb_synth[0]", Set.of(), 1),
            Arguments.of("zstd-synth-repo/test_zstd_synth[0]", Set.of(), 1),
            Arguments.of("zstd-synth-repo/test_zstd_logsdb[0]", Set.of(), 1),
            Arguments.of("synth-deletes-repo/test_deletes_lz4[0]", Set.of(), 1),
            Arguments.of("synth-deletes-repo/test_deletes_zstd[0]", Set.of(), 1),
            Arguments.of("synth-deletes-repo/test_deletes_nested[0]", Set.of(Structure.POSTINGS), 2),
            Arguments.of("es93-repo/test_es93_logsdb_binary[0]", Set.of(), 1),
            Arguments.of("es93-repo/test_es93_tsdb_hist[0]", Set.of(), 1),
            Arguments.of("synthcov-repo/cov_nested_synth[0]", Set.of(Structure.POSTINGS), 2),
            Arguments.of("synthcov-repo/cov_nested_logsdb[0]", Set.of(Structure.POSTINGS), 2),
            // Rewrite mode: synthetic-coverage's leaf check reads a text field's norms (round 1, refusing
            // on the stub's "no norms"), then the postings of the field, _field_names and _ignored.
            Arguments.of("synthcov-repo/cov_srcoff_native[0]", REWRITE, 3),
            Arguments.of("synthcov-repo/cov_srcoff_native[1]", REWRITE, 3),
            Arguments.of("synthcov-repo/cov_srcoff_merged[1]", REWRITE, 3),
            Arguments.of("synthcov-repo/cov_srcmode_native[0]", REWRITE, 3),
            Arguments.of("synthcov-repo/cov_srcmode_native[1]", REWRITE, 3),
            Arguments.of("synthcov8-repo/cov8_srcoff_native[1]", REWRITE, 3),
            Arguments.of("synthcov8-repo/cov8_srcmode_native[0]", REWRITE, 3),
            Arguments.of("synthcov8-repo/cov8_nested_synth[0]", Set.of(Structure.POSTINGS), 2),
            Arguments.of("m3-repo/m3_logsdb[0]", Set.of(), 1),
            Arguments.of("m3-repo/m3_logsdb_multi[0]", Set.of(), 1),
            Arguments.of("m3-repo/m3_tsdb[0]", Set.of(), 1),
            Arguments.of("m3-repo/m3_types[0]", Set.of(), 1),
            Arguments.of("m3-repo/m3_doc_count[0]", Set.of(Structure.POSTINGS), 2),
            Arguments.of("m3-repo/m3_nested[0]", Set.of(Structure.POSTINGS), 2),
            Arguments.of("m3-repo/m3_dense[0]", Set.of(Structure.VECTORS), 2),
            Arguments.of("m3-repo/m3_sparse[0]", Set.of(Structure.POSTINGS, Structure.TERM_VECTORS), 3),
            Arguments.of("m3-repo/m3_src_off[0]", Set.of(), 1));
    }

    @ParameterizedTest
    @MethodSource("fixtureShards")
    void theFixpointKeepsWhatReconstructionReads(String name, Set<Structure> expected, int calls, @TempDir Path tmp)
            throws Exception {
        Plan plan = fixpoint(shard(name, tmp));
        assertEquals(new Plan(expected, calls, false).toString(), plan.toString(), name);
    }

    /** cov_srcoff_text shard 1 holds a text field without a native source: the probe fails with nothing to fetch. */
    @Test
    void aShardReconstructionRefusesPerSegmentFailsTheProbe(@TempDir Path tmp) throws Exception {
        Plan plan = fixpoint(shard("synthcov-repo/cov_srcoff_text[1]", tmp));
        assertTrue(plan.failed(), plan.toString());
    }

    /**
     * A mapping refused before any file is read (an {@code enabled: false} object of an index created
     * with the source mode disabled), and one the isolated registry cannot build (m3-repo's
     * token_count and completion name analyzers it lacks): the probe fails with nothing touched.
     */
    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"synthcov-repo/cov_srcmode_mapping[1]", "m3-repo/m3_types_analyzed[0]"})
    void aMappingThatCannotBeReconstructedFailsTheProbeWithoutTouchingAnything(String name, @TempDir Path tmp) throws Exception {
        Shard shard = shard(name, tmp);
        SyntheticFetchProbe.Result result = SyntheticFetchProbe.probe(shard.pruned(), shard.index(), shard.metadata());
        assertTrue(result.failed());
        assertEquals(Set.of(), result.touched());
    }

    @Test
    void aShardWithNothingLeftOutTouchesNothing(@TempDir Path tmp) throws Exception {
        Shard shard = shard("synth-deletes-repo/test_deletes_nested[0]", tmp);
        SyntheticFetchProbe.Result result = SyntheticFetchProbe.probe(shard.full(), shard.index(), shard.metadata());
        assertFalse(result.failed());
        assertEquals(Set.of(), result.touched());
    }

    /**
     * The probe samples the first and last 64 live roots of each leaf: pinned on segments with hard
     * deletes, soft deletes and nested children, with more than 128 roots and between 64 and 128,
     * and on flat segments without {@code _primary_term} (fewer than 64 and more than 128 live
     * documents); equal to the ends of {@code liveRoots}.
     */
    @Test
    void theSampleIsTheFirstAndLastLiveRootsOfEachLeaf(@TempDir Path tmp) throws Exception {
        int[] blocks = {400, 100, 40, 300};
        boolean[] nested = {true, true, false, false};
        IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        java.util.List<int[][]> leaves;
        try {
            @SuppressWarnings("unchecked")
            java.util.List<int[][]> result = (java.util.List<int[][]>) Class.forName(
                    "de.sloow.gefrierschrank.syntheticsource.isolated.ProbeSampleSegments", true, loader)
                .getMethod("sampleAndLiveRoots", String.class, int[].class, boolean[].class)
                .invoke(null, tmp.toString(), blocks, nested);
            leaves = result;
        } catch (InvocationTargetException e) {
            throw e.getTargetException() instanceof Exception cause ? cause : e;
        } finally {
            thread.setContextClassLoader(previous);
        }
        assertEquals(blocks.length, leaves.size());
        int[] rootCounts = new int[blocks.length];
        for (int segment = 0; segment < blocks.length; segment++) {
            java.util.List<Integer> expected = new java.util.ArrayList<>();
            int doc = 0;
            for (int i = 0; i < blocks[segment]; i++) {
                if (nested[segment]) doc += i % 3;
                if (i % 7 != 3 && i % 5 != 1) expected.add(doc);
                doc++;
            }
            int[] roots = expected.stream().mapToInt(Integer::intValue).toArray();
            int[] sample = roots.length <= 128 ? roots
                : java.util.stream.IntStream.concat(Arrays.stream(roots, 0, 64), Arrays.stream(roots, roots.length - 64, roots.length))
                    .toArray();
            assertEquals(Arrays.toString(roots), Arrays.toString(leaves.get(segment)[1]), "live roots of segment " + segment);
            assertEquals(Arrays.toString(sample), Arrays.toString(leaves.get(segment)[0]), "sample of segment " + segment);
            rootCounts[segment] = roots.length;
        }
        assertEquals("[274, 68, 27, 205]", Arrays.toString(rootCounts));
    }
}
