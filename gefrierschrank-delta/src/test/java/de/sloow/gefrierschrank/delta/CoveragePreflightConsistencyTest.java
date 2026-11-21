package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.IndexOverview;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The residual commit-point preflight fails exactly the shards a full download's reconstruction
 * quarantines, with the same class and message, and passes every shard that commits: for every
 * shard of synth-deletes-repo, synthcov-repo, synthcov8-repo and MixedSourceShardTest's mixed shard,
 * {@link SyntheticCoverage#requireReconstructable} on a directory holding only the commit files
 * (segments_N, .si, .fnm, compound pair) is compared with the full export's outcome and with the
 * first failure of reconstructing every live document of the full shard. Indices created with
 * {@code index.mapping.source.mode: disabled} compare {@link SyntheticCoverage#requireMappingReconstructable}
 * with the export's failure before download. cov_srcoff_dynamic runs without and with the opt-in.
 * Runs in a child JVM laid out like the shaded CLI jar (vector formats, see MixedSourceShardTest).
 */
class CoveragePreflightConsistencyTest {

    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    static final List<String> REPOSITORIES = List.of("synth-deletes-repo", "synthcov-repo", "synthcov8-repo");

    @TempDir
    static Path work;
    private static final List<String[]> CASES = new ArrayList<>();

    @BeforeAll
    static void compareInChildJvm() throws Exception {
        List<String> command = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Djdk.util.jar.enableMultiRelease=false",
            "-cp", String.join(File.pathSeparator, Es93BinaryDocValuesPipelineTest.productionOrderedClassPath()),
            ChildMain.class.getName(), work.toString()));
        Path out = work.resolve("child.out");
        Path err = work.resolve("child.err");
        Process process = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile()).start();
        assertTrue(process.waitFor(10, TimeUnit.MINUTES));
        assertEquals(0, process.exitValue(), Files.readString(out) + Files.readString(err));
        for (String line : Files.readAllLines(out)) {
            if (line.startsWith("CASE\t")) {
                CASES.add(line.split("\t", -1));
                System.out.println(line);
            }
        }
    }

    @Test
    void thePreflightFailsExactlyTheShardsReconstructionQuarantines() {
        List<String> mismatches = new ArrayList<>();
        int refusing = 0;
        for (String[] c : CASES) {
            String name = c[1];
            String export = c[2];
            String direct = c[3];
            String preflight = c[4];
            if (export.startsWith("COMMITTED")) {
                if (!preflight.equals("OK")) mismatches.add(name + ": commits but the preflight throws " + preflight);
                if (!direct.equals("OK")) mismatches.add(name + ": commits but reconstruction throws " + direct);
            } else {
                refusing++;
                if (!preflight.equals(direct)) mismatches.add(name + ": preflight " + preflight + " != reconstruction " + direct);
                String exportClass = export.substring(export.indexOf(':') + 1);
                if (!direct.startsWith(exportClass + ":")) mismatches.add(name + ": export " + export + " != reconstruction " + direct);
            }
        }
        assertTrue(CASES.size() >= 4, "cases " + CASES.size());
        assertTrue(refusing >= 1, "at least one refusing shard, so the comparison is not vacuous");
        assertEquals(List.of(), mismatches);
    }

    /** Prints one tab-separated CASE line per shard: name, export outcome, reconstruction failure, preflight failure. */
    public static final class ChildMain {
        public static void main(String[] args) throws Exception {
            Path tmp = Path.of(args[0]);
            for (String repository : REPOSITORIES) {
                Path repo = FIXTURES.resolve(repository);
                if (!Files.isDirectory(repo)) continue;
                var store = new FileSystemBlobStore(repo);
                var inspector = new RepositoryInspector(store);
                for (IndexOverview index : inspector.overview().indices()) {
                    JsonNode metadata = inspector.indexMetadata(index.name());
                    boolean dynamic = index.name().equals("cov_srcoff_dynamic");
                    for (boolean optIn : dynamic ? new boolean[]{false, true} : new boolean[]{false}) {
                        if (optIn) System.setProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY, SyntheticCoverage.ACCEPT_UNMAPPED_VALUES);
                        else System.clearProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY);
                        compareIndex(repository, store, inspector, index, metadata, tmp.resolve(repository + "-" + index.name() + optIn),
                            optIn ? " (opt-in)" : "");
                    }
                    System.clearProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY);
                }
            }
            JsonNode synthetic = new RepositoryInspector(new FileSystemBlobStore(FIXTURES.resolve("synth-repo"))).indexMetadata("test_plain_synth");
            Path mixed = MixedSourceShardTest.mixedIndex(false, tmp.resolve("mixed"));
            System.out.println(String.join("\t", "CASE", "mixed STORED_PLUS_DISABLED", "QUARANTINED:" + className(direct(mixed,
                "test_plain_synth", sourceDisabled(synthetic))), direct(mixed, "test_plain_synth", sourceDisabled(synthetic)),
                preflight(mixed, "test_plain_synth", sourceDisabled(synthetic))));
        }

        private static void compareIndex(String repository, FileSystemBlobStore store, RepositoryInspector inspector, IndexOverview index,
                                         JsonNode metadata, Path tmp, String suffix) throws Exception {
            var sources = new RepositorySources(new RepositorySources.Source("primary", store));
            Map<Integer, String> outcomes = new LinkedHashMap<>();
            String indexFailure = null;
            try {
                RunReport report = new FullIndexExporter(sources, new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir))
                    .run(index.name(), tmp.resolve("table").toString(), RunReportListener.NO_OP);
                for (var shard : report.shards()) {
                    outcomes.put(shard.shardId(), shard.outcome() + (shard.errorClass() == null ? "" : ":" + shard.errorClass()));
                }
            } catch (IOException failure) {
                indexFailure = describe(failure);
            }
            if (indexFailure != null) {
                String mapping;
                try {
                    SyntheticCoverage.requireMappingReconstructable(index.name(), metadata);
                    mapping = "OK";
                } catch (IOException refused) {
                    mapping = describe(refused);
                }
                System.out.println(String.join("\t", "CASE", repository + "/" + index.name() + suffix,
                    "FAILED:" + className(indexFailure), indexFailure, mapping));
                return;
            }
            for (int shard = 0; shard < index.shardGenerations().size(); shard++) {
                Path shardDir = tmp.resolve("shard" + shard);
                Files.createDirectories(shardDir.getParent());
                new ShardMaterializer(store).materialize(inspector.shardSnapshot(index.name(), shard), shardDir);
                System.out.println(String.join("\t", "CASE", repository + "/" + index.name() + "[" + shard + "]" + suffix,
                    outcomes.get(shard), direct(shardDir, index.name(), metadata), preflight(shardDir, index.name(), metadata)));
            }
        }

        /** The first failure of reconstructing every live document of the full shard, or OK. */
        static String direct(Path shardDir, String index, JsonNode metadata) throws IOException {
            try (ShardReader reader = SyntheticSourceFallbackShardReader.wrap(new Lucene10ShardReader(shardDir), shardDir, index, metadata)) {
                for (int doc = 0; doc < reader.docCount(); doc++) {
                    if (reader.isLive(doc)) reader.readSource(doc, (bytes, length) -> {});
                }
                return "OK";
            } catch (IOException | RuntimeException failure) {
                return describe(failure);
            }
        }

        /** The preflight on a copy of the shard that holds only its commit files, or OK. */
        static String preflight(Path shardDir, String index, JsonNode metadata) throws IOException {
            Path commitOnly = shardDir.resolveSibling(shardDir.getFileName() + "-commit");
            Files.createDirectories(commitOnly);
            try (var files = Files.list(shardDir)) {
                for (Path file : files.toList()) {
                    String name = file.getFileName().toString();
                    if (name.startsWith("segments_") || name.endsWith(".si") || name.endsWith(".fnm")
                        || name.endsWith(".cfs") || name.endsWith(".cfe")) {
                        Files.copy(file, commitOnly.resolve(name));
                    }
                }
            }
            try {
                SyntheticCoverage.requireReconstructable(commitOnly, index, metadata, sourcelessLiveSegments(shardDir));
                return "OK";
            } catch (IOException | RuntimeException failure) {
                return describe(failure);
            }
        }

        /** As selective-fetch's caller computes them: segments with live documents and no {@code _source} field. */
        static Set<String> sourcelessLiveSegments(Path shardDir) throws IOException {
            Set<String> names = new TreeSet<>();
            try (Directory directory = new NIOFSDirectory(shardDir); DirectoryReader reader = DirectoryReader.open(directory)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    SegmentReader segment = (SegmentReader) FilterLeafReader.unwrap(leaf.reader());
                    SegmentCommitInfo info = segment.getSegmentInfo();
                    int live = info.info.maxDoc() - info.getDelCount() - info.getSoftDelCount();
                    if (live > 0 && segment.getFieldInfos().fieldInfo("_source") == null) names.add(info.info.name);
                }
            }
            return names;
        }

        private static String describe(Throwable failure) {
            return failure.getClass().getSimpleName() + ":" + failure.getMessage();
        }

        private static String className(String described) {
            return described.substring(0, described.indexOf(':'));
        }

        /** MixedSourceShardTest's STORED_PLUS_DISABLED metadata: the synthetic mapping with {@code _source.enabled: false}. */
        private static JsonNode sourceDisabled(JsonNode synthetic) {
            ObjectNode metadata = synthetic.deepCopy();
            ((ObjectNode) metadata.path("settings")).remove("index.mapping.source.mode");
            JsonNode mappings = metadata.path("mappings");
            if (mappings.isArray()) mappings = mappings.path(0);
            ObjectNode mapping = (ObjectNode) (mappings.has("_doc") ? mappings.path("_doc") : mappings);
            mapping.putObject("_source").put("enabled", false);
            return metadata;
        }
    }
}
