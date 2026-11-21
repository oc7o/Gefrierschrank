package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.codec.ZstdFrameDecoder;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The ZSTD synthetic-source fixtures (ZSTD stored fields, and ES 9.3 ES819 binary doc values with
 * ZSTD blocks) reconstruct to exactly ES's own {@code _search} output with the default and with the
 * forced pure-Java zstd decoder.
 *
 * <p>The pom runs this class a second time in its own surefire JVM with
 * {@code -DZstdNativePath=<missing file>}: zstd-jni then fails to load with the same
 * {@link UnsatisfiedLinkError} a {@code noexec} {@code java.io.tmpdir} produces, which proves the
 * automatic fallback end to end without Docker.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ZstdDecoderFallbackReconstructionTest {

    private static final boolean NATIVE_UNAVAILABLE_RUN = Boolean.getBoolean("gefrierschrank.test.zstdNativeUnavailable");
    private static final String WARNING = "zstd-jni native library unavailable";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final ByteArrayOutputStream CAPTURED_STDERR = new ByteArrayOutputStream();
    private static PrintStream realStderr;

    @BeforeAll
    static void teeStderr() {
        PrintStream real = realStderr = System.err;
        System.setErr(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) {
                real.write(b);
                synchronized (CAPTURED_STDERR) {
                    CAPTURED_STDERR.write(b);
                }
            }
        }, true));
    }

    @AfterAll
    static void restoreStderr() {
        System.setErr(realStderr);
    }

    @AfterEach
    void clearDecoderProperty() {
        System.clearProperty(ZstdFrameDecoder.DECODER_PROPERTY);
    }

    @Order(1)
    @ParameterizedTest(name = "{1} with the {3} decoder")
    @CsvSource({
        "zstd-synth-repo, test_zstd_synth, zstd-synth-expected.json, auto",
        "zstd-synth-repo, test_zstd_synth, zstd-synth-expected.json, java",
        "zstd-synth-repo, test_zstd_logsdb, zstd-logsdb-expected.json, auto",
        "zstd-synth-repo, test_zstd_logsdb, zstd-logsdb-expected.json, java",
        "es93-repo, test_es93_logsdb_binary, es93-logsdb-binary-expected.json, auto",
        "es93-repo, test_es93_logsdb_binary, es93-logsdb-binary-expected.json, java"})
    void reconstructsZstdFixturesExactly(String repoName, String indexName, String expectedFile, String decoder,
                                          @TempDir Path tmp) throws Exception {
        if (decoder.equals("java")) {
            System.setProperty(ZstdFrameDecoder.DECODER_PROPERTY, "java");
        }
        Path repo = Path.of("src/test/resources/fixtures", repoName);
        JsonNode expected = JSON.readTree(Path.of("src/test/resources/fixtures", expectedFile).toFile());
        var store = new FileSystemBlobStore(repo);
        var inspector = new RepositoryInspector(store);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot(indexName, 0), shardDir);

        Set<JsonNode> expectedSources = new HashSet<>();
        expected.forEach(expectedSources::add);
        Set<JsonNode> reconstructed = new HashSet<>();
        try (SyntheticSourceReconstructor reconstructor =
                 SyntheticSourceReconstructor.open(shardDir, indexName, inspector.indexMetadata(indexName))) {
            assertEquals(expected.size(), reconstructor.docCount());
            for (int docId = 0; docId < reconstructor.docCount(); docId++) {
                reconstructed.add(JSON.readTree(reconstructor.readSource(docId)));
            }
        }
        assertEquals(expectedSources, reconstructed);
    }

    @Order(2)
    @Test
    void nativeLibraryIsUsedWhereItLoads() {
        assumeTrue(!NATIVE_UNAVAILABLE_RUN);
        assertTrue(ZstdFrameDecoder.nativeAvailable());
        assertEquals(0, warnings());
    }

    @Order(2)
    @Test
    void missingNativeLibraryFallsBackToJavaAndWarnsExactlyOnce() {
        assumeTrue(NATIVE_UNAVAILABLE_RUN, "runs in the zstd-native-unavailable surefire execution");
        assertFalse(ZstdFrameDecoder.nativeAvailable());
        assertFalse(new ZstdFrameDecoder().usesNative());
        assertEquals(1, warnings(), CAPTURED_STDERR.toString(StandardCharsets.UTF_8));
        String warning = CAPTURED_STDERR.toString(StandardCharsets.UTF_8).lines()
            .filter(line -> line.startsWith(WARNING)).findFirst().orElseThrow();
        assertTrue(warning.contains("(UnsatisfiedLinkError)"), warning);
        assertTrue(warning.contains("-DZstdTempFolder="), warning);
    }

    private static long warnings() {
        synchronized (CAPTURED_STDERR) {
            return CAPTURED_STDERR.toString(StandardCharsets.UTF_8).lines().filter(line -> line.startsWith(WARNING)).count();
        }
    }
}
