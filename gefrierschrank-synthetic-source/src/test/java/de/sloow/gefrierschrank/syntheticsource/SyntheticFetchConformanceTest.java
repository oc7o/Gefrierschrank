package de.sloow.gefrierschrank.syntheticsource;

import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.IndexOverview;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch.Structure;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Selective fetch's plan for synthetic-source shards against what Elasticsearch's reconstruction
 * really reads, on every shard of the synthetic fixtures (rewritten non-compound, see
 * SyntheticFetchProbeTest) and of m3-repo (non-compound as Elasticsearch wrote it): (a) the probe's
 * fixpoint over the files every plan keeps gives the plan; (b) every live root of the complete
 * shard is reconstructed through recording producers and a recording directory; (c) the structures
 * that reconstruction read are in the plan and every file it opened is fetched; (d) every live root
 * reconstructed from the pruned directory, through the guarded worker, is byte-identical to the
 * complete directory's, and the guard never tripped. A shard whose probe fails (F5, fetched in full)
 * must fail the same way on the complete directory. Runs in-process through the shared isolated
 * loader like the other synthetic-source tests (this module has no shaded jar).
 *
 * <p>{@link #VERIFIED_ES_VERSION} is the Elasticsearch version this was verified against: an
 * upgrade fails the test until it is re-run and the constant bumped, because what the loaders read
 * is Elasticsearch's code, not ours.
 */
class SyntheticFetchConformanceTest {

    static final String VERIFIED_ES_VERSION = "9.3.0";

    private static final List<String> REPOSITORIES = List.of("synth-repo", "synth-deletes-repo", "zstd-synth-repo", "es93-repo",
        "synthcov-repo", "synthcov8-repo", "m3-repo");
    private static final Set<String> SKIPPED = Set.of(
        // stored source with vectors excluded: read by vector rehydration, fetched in full (F1)
        "es93-repo/test_es93_vectors_default", "es93-repo/test_es93_vectors_included");

    @Test
    void theIsolatedElasticsearchIsTheVerifiedVersion() throws Exception {
        assertEquals(VERIFIED_ES_VERSION, isolated("esVersion"));
    }

    static Stream<String> shards() throws Exception {
        List<String> names = new ArrayList<>();
        for (String repository : REPOSITORIES) {
            Path repo = Path.of("src/test/resources/fixtures", repository);
            if (!Files.isDirectory(repo)) continue;
            for (IndexOverview index : new RepositoryInspector(new FileSystemBlobStore(repo)).overview().indices()) {
                if (SKIPPED.contains(repository + "/" + index.name())) continue;
                for (int shard = 0; shard < index.shardGenerations().size(); shard++) {
                    names.add(repository + "/" + index.name() + "[" + shard + "]");
                }
            }
        }
        return names.stream().sorted();
    }

    @ParameterizedTest
    @MethodSource("shards")
    void thePlanCoversWhatReconstructionReads(String name, @TempDir Path tmp) throws Exception {
        SyntheticFetchProbeTest.Shard shard = SyntheticFetchProbeTest.shard(name, tmp);
        SyntheticFetchProbeTest.Plan plan = SyntheticFetchProbeTest.fixpoint(shard);
        SyntheticSourceReconstructor.Inputs inputs = SyntheticSourceReconstructor.reconstructionInputs(shard.metadata());
        int[] roots = (int[]) isolated("liveRoots", shard.full().toString());
        if (plan.failed()) {
            assertThrows(Exception.class, () -> reconstructAll(shard.full(), shard, roots),
                name + ": a shard whose probe fails (fetched in full) fails on the complete directory too");
            return;
        }

        @SuppressWarnings("unchecked")
        List<String> reads = (List<String>) isolated("reads", shard.full().toString(), shard.index(), inputs.mappingJson(),
            inputs.flatSettings(), inputs.rewriteMappingJson());
        Set<Structure> read = reads.stream().filter(r -> r.startsWith("structure:"))
            .map(r -> Structure.valueOf(r.substring("structure:".length()))).collect(Collectors.toSet());
        assertTrue(plan.kept().containsAll(read), name + ": read " + new TreeSet<>(read) + ", planned " + plan);
        Set<String> fetched;
        try (Stream<Path> files = Files.list(shard.pruned())) {
            fetched = files.map(p -> p.getFileName().toString()).collect(Collectors.toSet());
        }
        for (String file : reads.stream().filter(r -> r.startsWith("file:")).map(r -> r.substring(5)).toList()) {
            assertTrue(fetched.contains(file), name + ": opened " + file + ", which the plan leaves out");
        }

        List<byte[]> complete = reconstructAll(shard.full(), shard, roots);
        try (SyntheticSourceReconstructor pruned = SyntheticSourceReconstructor.open(shard.pruned(), shard.index(), shard.metadata())) {
            List<byte[]> fromPruned = new ArrayList<>();
            for (int doc : roots) fromPruned.add(pruned.readSourceBytes(doc));
            assertSame(name, complete, fromPruned);
            assertSame(name + " (batch)", complete, readBatched(pruned, roots));
            assertEquals(false, PrunedWorkerAccess.tripped(pruned), name + ": the guard never tripped");
        }
    }

    /** Every live root of {@code dir}, through both of the worker's read paths, which must agree. */
    private static List<byte[]> reconstructAll(Path dir, SyntheticFetchProbeTest.Shard shard, int[] roots) throws Exception {
        try (SyntheticSourceReconstructor reconstructor = SyntheticSourceReconstructor.open(dir, shard.index(), shard.metadata())) {
            List<byte[]> sources = new ArrayList<>();
            for (int doc : roots) sources.add(reconstructor.readSourceBytes(doc));
            assertSame("batched and per-document reads", sources, readBatched(reconstructor, roots));
            return sources;
        }
    }

    private static List<byte[]> readBatched(SyntheticSourceReconstructor reconstructor, int[] roots) throws Exception {
        List<byte[]> sources = new ArrayList<>();
        int next = 0;
        while (next < roots.length) {
            byte[][] batch = reconstructor.readSourcesBytes(Arrays.copyOfRange(roots, next, roots.length));
            sources.addAll(List.of(batch));
            next += batch.length;
        }
        return sources;
    }

    private static void assertSame(String name, List<byte[]> expected, List<byte[]> actual) {
        assertEquals(expected.size(), actual.size(), name);
        for (int i = 0; i < expected.size(); i++) assertArrayEquals(expected.get(i), actual.get(i), name + " root " + i);
    }

    /** The worker's tripwire, through the isolated test helper. */
    static final class PrunedWorkerAccess {
        static boolean tripped(SyntheticSourceReconstructor reconstructor) throws Exception {
            var field = SyntheticSourceReconstructor.class.getDeclaredField("worker");
            field.setAccessible(true);
            Object worker = field.get(reconstructor);
            return (Boolean) call("de.sloow.gefrierschrank.syntheticsource.isolated.PrunedWorkerProbe", "tripped", worker);
        }
    }

    static Object isolated(String method, Object... args) throws Exception {
        return call("de.sloow.gefrierschrank.syntheticsource.isolated.ConformanceProbe", method, args);
    }

    private static Object call(String type, String method, Object... args) throws Exception {
        IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            for (Method candidate : Class.forName(type, true, loader).getMethods()) {
                if (candidate.getName().equals(method) && candidate.getParameterCount() == args.length) {
                    return candidate.invoke(null, args);
                }
            }
            throw new NoSuchMethodException(type + "." + method);
        } catch (InvocationTargetException e) {
            throw e.getTargetException() instanceof Exception cause ? cause : e;
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
