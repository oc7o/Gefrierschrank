package de.sloow.gefrierschrank.syntheticsource.isolated;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.PrunedFormatAccessException;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch.Structure;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.syntheticsource.SyntheticSourceReconstructor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The isolated pruned open: a structure whose files are missing from a non-compound segment is
 * answered by stubs (GUARD: record, trip, throw; PROBE: record, "no values"), everything else is
 * the segment's own codec, and with nothing missing the open is {@code DirectoryReader.open}. The
 * worker reads through GUARD and checks the tripwire before it returns a document. Runs with
 * assertions enabled (surefire's default), so the copied segment infos pass Lucene's checks.
 */
class PrunedSegmentReadersTest {

    @Test
    void assertionsAreEnabled() {
        boolean enabled = false;
        assert enabled = true;
        assertTrue(enabled, "run under -ea");
    }

    @ParameterizedTest
    @EnumSource(Structure.class)
    void aGuardedAbsentStructureThrowsAndTrips(Structure structure, @TempDir Path tmp) throws Exception {
        checks("aGuardedAbsentStructureThrowsAndTrips", structure.name(), tmp.toString());
    }

    @ParameterizedTest
    @EnumSource(Structure.class)
    void aProbedAbsentStructureAnswersNoValuesAndIsRecorded(Structure structure, @TempDir Path tmp) throws Exception {
        checks("aProbedAbsentStructureAnswersNoValuesAndIsRecorded", structure.name(), tmp.toString());
    }

    @Test
    void storedFieldsDocValuesAndDeletionsReadAsWithEveryFile(@TempDir Path tmp) throws Exception {
        checks("storedFieldsDocValuesAndDeletionsReadAsWithEveryFile", tmp.toString());
    }

    @Test
    void withNothingAbsentTheOpenIsDirectoryReaderOpen(@TempDir Path tmp) throws Exception {
        checks("withNothingAbsentTheOpenIsDirectoryReaderOpen", tmp.toString());
    }

    @Test
    void thePrunedCodecHandsOutTheDelegatesOwnFormats(@TempDir Path tmp) throws Exception {
        checks("thePrunedCodecHandsOutTheDelegatesOwnFormats", tmp.toString());
    }

    /** Files every reader opens: missing, they fail the open as without selective fetch, also beside an absent structure. */
    @ParameterizedTest
    @ValueSource(strings = {"fdt", "fdx", "dvd", "dvm", "fnm", "liv", "si"})
    void aMissingFileOfNoStructureFailsTheOpenLikeANormalOpen(String extension, @TempDir Path tmp) throws Exception {
        checks("aMissingFileOfNoStructureFailsTheOpenLikeANormalOpen", extension, tmp.toString());
    }

    private static void checks(String method, Object... args) throws Exception {
        isolated("PrunedSegmentReadersChecks", method, args);
    }

    static List<String> delete(Path dir, Predicate<String> drop) throws IOException {
        List<String> deleted = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path file : files.toList()) {
                String name = file.getFileName().toString();
                if (drop.test(name)) {
                    Files.delete(file);
                    deleted.add(name);
                }
            }
        }
        return deleted;
    }

    static Predicate<String> of(Structure structure) {
        return name -> SelectiveFetch.structureOf(name) == structure;
    }

    // ---- the worker over a pruned Elasticsearch shard (isolated world) ----

    private static final String PLAIN = "synth-repo/test_plain_synth";
    private static final String NESTED = "synth-deletes-repo/test_deletes_nested";

    record Shard(String index, Path dir, JsonNode metadata) {
        SyntheticSourceReconstructor open() throws IOException {
            return SyntheticSourceReconstructor.open(dir, index, metadata);
        }
    }

    /** A fixture shard rewritten with non-compound segments. */
    static Shard nonCompound(String name, Path tmp) throws Exception {
        String repo = name.substring(0, name.indexOf('/'));
        String index = name.substring(name.indexOf('/') + 1);
        var store = new FileSystemBlobStore(Path.of("src/test/resources/fixtures", repo));
        var inspector = new RepositoryInspector(store);
        Path compound = tmp.resolve("compound");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot(index, 0), compound);
        Path dir = tmp.resolve("noncompound");
        isolated("NonCompoundShards", "rewrite", compound.toString(), dir.toString());
        try (Stream<Path> files = Files.list(dir)) {
            assertTrue(files.noneMatch(f -> f.toString().endsWith(".cfs")), "non-compound");
        }
        return new Shard(index, dir, inspector.indexMetadata(index));
    }

    static List<String> readAll(SyntheticSourceReconstructor reconstructor) throws IOException {
        List<String> docs = new ArrayList<>();
        for (int doc = 0; doc < reconstructor.docCount(); doc++) docs.add(reconstructor.readSource(doc));
        return docs;
    }

    static Object worker(SyntheticSourceReconstructor reconstructor) throws ReflectiveOperationException {
        var field = SyntheticSourceReconstructor.class.getDeclaredField("worker");
        field.setAccessible(true);
        return field.get(reconstructor);
    }

    @Test
    void theWorkerReadsAPrunedShardLikeTheFullOneWithoutTripping(@TempDir Path tmp) throws Exception {
        Shard shard = nonCompound(PLAIN, tmp);
        List<String> full;
        try (var reconstructor = shard.open()) {
            full = readAll(reconstructor);
        }
        assertFalse(delete(shard.dir(), name -> SelectiveFetch.structureOf(name) != null).isEmpty());
        try (var reconstructor = shard.open()) {
            assertEquals(full, readAll(reconstructor));
            assertEquals(false, isolated("PrunedWorkerProbe", "tripped", worker(reconstructor)));
        }
    }

    @Test
    void aSwallowedGuardExceptionStillFailsTheNextRead(@TempDir Path tmp) throws Exception {
        Shard shard = nonCompound(PLAIN, tmp);
        delete(shard.dir(), of(Structure.POSTINGS));
        try (var reconstructor = shard.open()) {
            assertNotNull(reconstructor.readSource(0));
            Object worker = worker(reconstructor);
            assertTrue((Integer) isolated("PrunedWorkerProbe", "swallowPostingsAccess", worker, "_id") > 0);
            assertEquals(true, isolated("PrunedWorkerProbe", "tripped", worker));
            assertThrows(PrunedFormatAccessException.class, () -> reconstructor.readSource(1));
            assertThrows(PrunedFormatAccessException.class, () -> reconstructor.readSourcesBytes(new int[] {1, 2}));
        }
    }

    /** Elasticsearch's nested loader finds a root's children through {@code _nested_path} postings. */
    @Test
    void aNestedRootWithoutPostingsFailsInsteadOfLosingItsChildren(@TempDir Path tmp) throws Exception {
        Shard shard = nonCompound(NESTED, tmp);
        delete(shard.dir(), of(Structure.POSTINGS));
        try (var reconstructor = shard.open()) {
            assertTrue(firstFailingRoot(reconstructor) >= 0);
            assertEquals(true, isolated("PrunedWorkerProbe", "tripped", worker(reconstructor)));
        }
    }

    /** The first document the worker accepts as a root fails (children are refused before anything is read). */
    private static int firstFailingRoot(SyntheticSourceReconstructor reconstructor) {
        for (int doc = 0; doc < reconstructor.docCount(); doc++) {
            try {
                reconstructor.readSource(doc);
                throw new AssertionError("doc " + doc + " read without postings");
            } catch (IllegalArgumentException child) {
                if (!"doc id is not a root document".equals(child.getMessage())) return doc;
            } catch (IOException | RuntimeException expected) {
                return doc;
            }
        }
        throw new AssertionError("no root");
    }

    static Object isolated(String helper, String method, Object... args) throws Exception {
        Method shared = Class.forName("de.sloow.gefrierschrank.syntheticsource.IsolatedEsClassLoader").getDeclaredMethod("shared");
        shared.setAccessible(true);
        ClassLoader loader = (ClassLoader) shared.invoke(null);
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            Class<?> type = Class.forName("de.sloow.gefrierschrank.syntheticsource.isolated." + helper, true, loader);
            for (Method candidate : type.getMethods()) {
                if (candidate.getName().equals(method) && candidate.getParameterCount() == args.length) {
                    try {
                        return candidate.invoke(null, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getTargetException() instanceof Exception cause ? cause : e;
                    }
                }
            }
            throw new NoSuchMethodException(helper + "." + method);
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
