package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The reconstruction reader is memory-mapped by default, honours
 * {@code -Dgefrierschrank.reconstruction.directory=niofs}, opens once more on NIOFS when mapping
 * fails, and rejects any other value. Every way of opening gives the golden text.
 */
class ReconstructionDirectoryTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PROBE = "de.sloow.gefrierschrank.syntheticsource.isolated.ReconstructionDirectoryProbe";
    private static final String PLAIN = "synth-repo/test_plain_synth";

    @AfterEach
    void clearProperty() {
        System.clearProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY);
    }

    @Test
    void memoryMappedByDefault(@TempDir Path tmp) throws Exception {
        var fixture = SyntheticSourceReconstructorTest.Fixture.materialize(PLAIN, tmp);
        try (var reconstructor = fixture.open()) {
            assertEquals("MMapDirectory", directoryOf(reconstructor));
            assertEquals(golden(PLAIN), SyntheticSourceReconstructorTest.readAll(reconstructor));
        }
    }

    @Test
    void niofsPropertyReadsThroughNiofs(@TempDir Path tmp) throws Exception {
        var fixture = SyntheticSourceReconstructorTest.Fixture.materialize(PLAIN, tmp);
        System.setProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY, "niofs");
        try (var reconstructor = fixture.open()) {
            assertEquals("NIOFSDirectory", directoryOf(reconstructor));
            assertEquals(golden(PLAIN), SyntheticSourceReconstructorTest.readAll(reconstructor));
        }
        System.setProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY, "mmap");
        try (var reconstructor = fixture.open()) {
            assertEquals("MMapDirectory", directoryOf(reconstructor));
        }
    }

    @Test
    void anyOtherValueFailsAtOpenWithAFixedMessage(@TempDir Path tmp) throws Exception {
        var fixture = SyntheticSourceReconstructorTest.Fixture.materialize(PLAIN, tmp);
        System.setProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY, "hybridfs-secret");
        IOException failure = assertThrows(IOException.class, fixture::open);
        Throwable cause = assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        assertEquals("-Dgefrierschrank.reconstruction.directory must be mmap or niofs", cause.getMessage());
        for (Throwable t = failure; t != null; t = t.getCause()) {
            assertFalse(String.valueOf(t.getMessage()).contains("hybridfs-secret"), t.getMessage());
            assertFalse(String.valueOf(t.getMessage()).contains("synthetic source"), t.getMessage());
        }
        IOException notSynthetic = assertThrows(IOException.class, () -> openEs7IndexWithoutSource(tmp.resolve("es7")));
        assertInstanceOf(IllegalArgumentException.class, notSynthetic.getCause(), "checked before the mapping");
    }

    /** Lucene reports a failed map as an IOException; older JDKs threw OutOfMemoryError("Map failed"). */
    @ParameterizedTest
    @ValueSource(strings = {"io", "oom"})
    void aMappingFailureFallsBackToNiofsExactlyOnce(String failure, @TempDir Path tmp) throws Exception {
        var fixture = SyntheticSourceReconstructorTest.Fixture.materialize(PLAIN, tmp);
        // A private loader: the injected factory lives in that loader's copy of the worker class.
        try (IsolatedEsClassLoader loader = IsolatedEsClassLoader.create()) {
            call(loader, "failMapping", failure);
            try (var reconstructor = SyntheticSourceReconstructor.open(fixture.shardDir(), fixture.index(),
                     fixture.metadata(), loader)) {
                assertEquals(1, call(loader, "mappingAttempts"));
                assertEquals("NIOFSDirectory", directoryOf(reconstructor));
                assertEquals(golden(PLAIN), SyntheticSourceReconstructorTest.readAll(reconstructor));
            } finally {
                call(loader, "restore");
            }
        }
    }

    /** A segment that is really damaged fails on NIOFS too, with the failure NIOFS alone gives. */
    @Test
    void aCorruptSegmentFailsTheSameWayOnEitherDirectory(@TempDir Path tmp) throws Exception {
        var fixture = SyntheticSourceReconstructorTest.Fixture.materialize(PLAIN, tmp);
        try (var files = Files.list(fixture.shardDir())) {
            for (Path file : files.filter(f -> f.getFileName().toString().endsWith(".si")).toList()) {
                byte[] bytes = Files.readAllBytes(file);
                bytes[bytes.length / 2] ^= 0x5A;
                Files.write(file, bytes);
            }
        }
        System.setProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY, "niofs");
        IOException niofs = assertThrows(IOException.class, fixture::open);
        System.clearProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY);
        IOException mmap = assertThrows(IOException.class, fixture::open);
        assertEquals(niofs.getClass(), mmap.getClass());
        assertEquals(niofs.getMessage(), mmap.getMessage());
        Throwable[] suppressed = mmap.getSuppressed();
        assertEquals(niofs.getSuppressed().length + 1, suppressed.length);
        assertEquals(niofs.getClass(), suppressed[suppressed.length - 1].getClass(),
            "the memory-mapped attempt failed the same way and is kept as suppressed");
    }

    /**
     * An ES 7 index without {@code _source} has no synthetic source to reconstruct. The mapping says
     * so before any segment is opened; its Lucene 8 segments would otherwise fail first with
     * IndexFormatTooOldException, which misnames the problem.
     */
    @Test
    void anEs7IndexWithoutSourceIsNotSyntheticBeforeItsSegmentsAreOpened(@TempDir Path tmp) throws Exception {
        IOException failure = assertThrows(IOException.class, () -> openEs7IndexWithoutSource(tmp));
        assertEquals("SourceNotSyntheticException", failure.getClass().getSimpleName());
        assertFalse(failure.getMessage().contains("synthetic source"), failure.getMessage());
    }

    private static void openEs7IndexWithoutSource(Path tmp) throws IOException {
        var store = new FileSystemBlobStore(Path.of("../gefrierschrank-core/src/test/resources/fixtures/es7-features-repo"));
        var inspector = new RepositoryInspector(store);
        Path shardDir = Files.createDirectories(tmp).resolve("shard");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot("es7_no_source", 0), shardDir);
        SyntheticSourceReconstructor.open(shardDir, "es7_no_source", inspector.indexMetadata("es7_no_source")).close();
    }

    static String directoryOf(SyntheticSourceReconstructor reconstructor) throws Exception {
        var field = SyntheticSourceReconstructor.class.getDeclaredField("worker");
        field.setAccessible(true);
        Object worker = field.get(reconstructor);
        return (String) call(worker.getClass().getClassLoader(), "directoryOf", worker);
    }

    static Object call(ClassLoader loader, String name, Object... args) throws Exception {
        Class<?> probe = Class.forName(PROBE, true, loader);
        for (Method method : probe.getMethods()) {
            if (method.getName().equals(name) && method.getParameterCount() == args.length) {
                return method.invoke(null, args);
            }
        }
        throw new NoSuchMethodException(name);
    }

    static List<String> golden(String fixtureIndex) throws IOException {
        List<String> docs = new ArrayList<>();
        JSON.readTree(SyntheticSourceReconstructorTest.GOLDEN.toFile()).path("reconstructed").path(fixtureIndex)
            .forEach(doc -> docs.add(doc.asText()));
        return docs;
    }
}
