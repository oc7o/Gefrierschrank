package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ES 9.3 writes BINARY doc values of logsdb/TSDB indices (wildcard, histogram, binary, ip_range,
 * counted_keyword counts, keyword values over ignore_above) with ES819TSDBDocValuesFormat and
 * ZSTD-compressed blocks, which decode only through the isolated NativeAccess substitution
 * (without it, reading them throws a NullPointerException). The shard must reconstruct to exactly
 * ES's own synthetic {@code _search} output. Fixture: {@code scripts/generate_es93_fixture.py}
 * (real ES 9.3.0).
 */
class Es93BinaryDocValuesReconstructionTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path FIXTURES = Path.of("src/test/resources/fixtures");

    @ParameterizedTest
    @CsvSource({
        "test_es93_logsdb_binary, es93-logsdb-binary-expected.json, url.path",
        "test_es93_tsdb_hist, es93-tsdb-hist-expected.json, latency"})
    void reconstructsEs819ZstdBinaryDocValuesExactly(String indexName, String expectedFile, String binaryField,
                                                      @TempDir Path tmp) throws Exception {
        JsonNode expected = JSON.readTree(FIXTURES.resolve(expectedFile).toFile());
        var store = new FileSystemBlobStore(FIXTURES.resolve("es93-repo"));
        var inspector = new RepositoryInspector(store);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot(indexName, 0), shardDir);

        // Guards against the fixture silently degrading to a format that does not need the substitution.
        try (IsolatedEsClassLoader loader = IsolatedEsClassLoader.create()) {
            IsolatedNativeAccessTest.call(loader, "bootstrap");
            @SuppressWarnings("unchecked")
            List<String> formats = (List<String>) IsolatedNativeAccessTest.call(loader, "binaryDocValuesFormats", shardDir);
            assertTrue(formats.contains(binaryField + "=ES819TSDB"), formats.toString());
        }

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
        assertEquals(expectedSources.size(), reconstructed.size());
        assertEquals(expectedSources, reconstructed);
    }
}
