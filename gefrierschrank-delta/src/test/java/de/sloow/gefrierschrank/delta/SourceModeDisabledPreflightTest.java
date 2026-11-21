package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An ES 8+ index created with {@code index.mapping.source.mode: disabled} is no longer
 * refused before download by itself; {@link ExportPreflight#require} fails it only when the
 * mapping-level coverage rule refuses, with exactly what {@link SyntheticCoverage#requireMappingReconstructable}
 * (and a shard's worker) throws. With {@code --source-disabled-accept unmapped-values} the
 * {@code dynamic:*} refusal gives way to one content-free warning per index. These cases need the
 * isolated world, so they run in a child JVM laid out like the shaded CLI jar.
 */
class SourceModeDisabledPreflightTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String DISABLED = "{\"index.mapping.source.mode\":\"disabled\",\"index.version.created\":\"8521000\","
        + "\"index.number_of_shards\":\"1\",\"index.number_of_replicas\":\"0\"}";

    @TempDir
    static Path work;
    private static final Map<String, String[]> RESULTS = new HashMap<>();

    @BeforeAll
    static void runInChildJvm() throws Exception {
        List<String> command = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Djdk.util.jar.enableMultiRelease=false",
            "-cp", String.join(File.pathSeparator, Es93BinaryDocValuesPipelineTest.productionOrderedClassPath()),
            ChildMain.class.getName()));
        Path out = work.resolve("child.out");
        Path err = work.resolve("child.err");
        Process process = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile()).start();
        assertTrue(process.waitFor(5, TimeUnit.MINUTES));
        assertEquals(0, process.exitValue(), Files.readString(out) + Files.readString(err));
        for (String line : Files.readAllLines(out)) {
            String[] parts = line.split("\t", -1);
            if (parts[0].equals("RESULT")) RESULTS.put(parts[1], parts);
        }
    }

    @Test
    void anIndexTheMappingAllowsPassesBeforeDownload() {
        assertEquals("OK", RESULTS.get("native es8")[2]);
        assertEquals("OK", RESULTS.get("native es8")[3]);
        assertEquals("OK", RESULTS.get("native unknown snapshot")[2]);
        assertEquals("DISABLED_FROM_CREATION", RESULTS.get("native es8")[5]);
    }

    @Test
    void aMappingLevelRefusalFailsTheIndexWithTheWorkersClassAndMessage() {
        String[] result = RESULTS.get("enabled false object");
        assertEquals("UnreconstructableSourceException:documents without stored _source cannot be reconstructed from this index's "
            + "doc values and stored fields: object:enabled-false", result[2]);
        assertEquals(result[2], result[3], "require and requireMappingReconstructable throw the same");
    }

    @Test
    void dynamicFalseRefusesUnlessAcceptedThenWarnsOncePerIndex() {
        String[] refused = RESULTS.get("dynamic false");
        assertTrue(refused[2].endsWith(": dynamic:false"), refused[2]);
        assertEquals("", refused[4], "no warning without the opt-in");
        String[] accepted = RESULTS.get("dynamic false accepted");
        assertEquals("OK", accepted[2]);
        assertEquals("OK", accepted[3]);
        assertEquals("warning: index idx: --source-disabled-accept unmapped-values is set; values sent under dynamic:false or "
            + "dynamic:runtime objects were kept only in the disabled _source and are missing, uncounted, from documents "
            + "reconstructed without stored _source (their shard reports carry unrecoverableUnmappedValues: true)", accepted[4]);
        assertFalse(accepted[4].contains("lost-field"), "content-free");
        assertTrue(RESULTS.get("disabled later dynamic false accepted")[4].startsWith("warning: index idx:"),
            "_source disabled later warns too");
        assertEquals("", RESULTS.get("accepted without dynamic")[4], "no warning when no dynamic:false/runtime object exists");
    }

    @Test
    void theKillSwitchRestoresTheRefusalBeforeDownload() {
        assertEquals("SourceDisabledException", RESULTS.get("kill switch")[2].substring(0, RESULTS.get("kill switch")[2].indexOf(':')));
    }

    /** Prints RESULT, case, require outcome, requireMappingReconstructable outcome, stderr of require, retention. */
    public static final class ChildMain {
        public static void main(String[] args) throws Exception {
            String keyword = "{\"properties\":{\"name\":{\"type\":\"keyword\"}}}";
            run("native es8", DISABLED, keyword, "u8", false, false);
            run("native unknown snapshot", DISABLED, keyword, "u0", false, false);
            run("enabled false object", DISABLED,
                "{\"properties\":{\"name\":{\"type\":\"keyword\"},\"blob\":{\"type\":\"object\",\"enabled\":false}}}", "u8", false, false);
            String dynamicFalse = "{\"properties\":{\"name\":{\"type\":\"keyword\"},"
                + "\"lost-field\":{\"type\":\"object\",\"dynamic\":false,\"properties\":{\"x\":{\"type\":\"keyword\"}}}}}";
            run("dynamic false", DISABLED, dynamicFalse, "u8", false, false);
            run("dynamic false accepted", DISABLED, dynamicFalse, "u8", true, false);
            run("disabled later dynamic false accepted",
                "{\"index.version.created\":\"8521000\",\"index.number_of_shards\":\"1\",\"index.number_of_replicas\":\"0\"}",
                dynamicFalse.replaceFirst("\\{", "{\"_source\":{\"enabled\":false},"), "u8", true, false);
            run("accepted without dynamic", DISABLED, keyword, "u8", true, false);
            run("kill switch", DISABLED, keyword, "u8", false, true);
        }

        private static void run(String name, String settings, String mapping, String snapshot, boolean accept, boolean killSwitch)
                throws IOException {
            if (accept) System.setProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY, SyntheticCoverage.ACCEPT_UNMAPPED_VALUES);
            else System.clearProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY);
            String killProperty = de.sloow.gefrierschrank.syntheticsource.SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY;
            if (killSwitch) System.setProperty(killProperty, "false");
            else System.clearProperty(killProperty);
            JsonNode metadata = JSON.createObjectNode()
                .<com.fasterxml.jackson.databind.node.ObjectNode>set("settings", JSON.readTree(settings))
                .set("mappings", JSON.readTree("[{\"_doc\":" + mapping + "}]"));
            PrintStream previous = System.err;
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            String require;
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
            try {
                ExportPreflight.require("idx", new RepositorySources.Resolved(SOURCE, "idx", snapshot, metadata,
                    List.of("primary/" + snapshot + "/idx")));
                require = "OK";
            } catch (IOException failure) {
                require = failure.getClass().getSimpleName() + ":" + failure.getMessage();
            } finally {
                System.setErr(previous);
            }
            String mappingCheck;
            try {
                SyntheticCoverage.requireMappingReconstructable("idx", metadata);
                mappingCheck = "OK";
            } catch (IOException failure) {
                mappingCheck = failure.getClass().getSimpleName() + ":" + failure.getMessage();
            }
            String retention = ExportPreflight.SourceRetention.of(metadata, snapshot.equals("u8") ? 8 : -1).name();
            System.out.println(String.join("\t", "RESULT", name, require, mappingCheck,
                captured.toString(StandardCharsets.UTF_8).strip(), retention));
        }
    }

    /** One index in an ES 8 ("8.11.0") snapshot and in one without a version. */
    private static final RepositorySources.Source SOURCE = new RepositorySources.Source("primary", new MemoryStore(Map.of(
        "index.latest", new byte[] {0, 0, 0, 0, 0, 0, 0, 0},
        "index-0", """
            {"snapshots":[
              {"name":"es8","uuid":"u8","state":1,"version":"8.11.0","index_version":8521000,"start_time_millis":2},
              {"name":"legacy","uuid":"u0","state":1}],
             "indices":{"idx":{"id":"idx-id","snapshots":["u8","u0"],"shard_generations":["g"]}}}
            """.getBytes(StandardCharsets.UTF_8))));

    private record MemoryStore(Map<String, byte[]> blobs) implements BlobStore {
        @Override
        public Optional<BlobMetadata> stat(String key) {
            return Optional.ofNullable(blobs.get(key)).map(b -> new BlobMetadata(key, b.length));
        }

        @Override
        public InputStream read(String key) throws IOException {
            byte[] blob = blobs.get(key);
            if (blob == null) throw new NoSuchFileException(key);
            return new ByteArrayInputStream(blob);
        }

        @Override
        public List<String> list(String prefix) {
            return blobs.keySet().stream().filter(k -> k.startsWith(prefix)).sorted().toList();
        }

        @Override
        public List<BlobMetadata> listWithSizes(String prefix) {
            return list(prefix).stream().map(k -> new BlobMetadata(k, blobs.get(k).length)).toList();
        }

        @Override
        public void put(String key, byte[] content) {
            throw new UnsupportedOperationException();
        }
    }
}
