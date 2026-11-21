package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.SetTransaction;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every committed fixture repository exported by dump-to-delta --all-indices with and without
 * selective fetch, at one and at four shard threads: the same exit codes, tables, AddFile tags,
 * transaction ids and shard outcomes. Each configuration runs in a child JVM laid out like the
 * shaded jar (multi-release jars disabled), so Elasticsearch's vector classes never load.
 */
class SelectiveFetchEquivalenceTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    static final List<String> REPOS = List.of(
        "../gefrierschrank-cli/src/test/resources/fixtures/ilm-shell-repo",
        "../gefrierschrank-core/src/test/resources/fixtures/demo-repo",
        "../gefrierschrank-core/src/test/resources/fixtures/es7-best-compression-repo",
        "../gefrierschrank-core/src/test/resources/fixtures/es7-features-repo",
        "../gefrierschrank-core/src/test/resources/fixtures/es7-upgraded-repo",
        "../gefrierschrank-core/src/test/resources/fixtures/frozen-repo",
        "../gefrierschrank-core/src/test/resources/fixtures/retired-shell-repo/before",
        "../gefrierschrank-core/src/test/resources/fixtures/retired-shell-repo/after",
        "../gefrierschrank-core/src/test/resources/fixtures/reused-index-repo",
        "../gefrierschrank-synthetic-source/src/test/resources/fixtures/es93-repo",
        "../gefrierschrank-synthetic-source/src/test/resources/fixtures/synth-deletes-repo",
        "../gefrierschrank-synthetic-source/src/test/resources/fixtures/synth-repo",
        "../gefrierschrank-synthetic-source/src/test/resources/fixtures/zstd-synth-repo");

    @TempDir
    static Path runs;

    record Config(String name, boolean selective, int threads) {}

    static final List<Config> CONFIGS = List.of(new Config("off-1", false, 1), new Config("on-1", true, 1),
        new Config("off-4", false, 4), new Config("on-4", true, 4));
    static final Map<String, Map<String, Integer>> EXIT_CODES = new TreeMap<>();

    /** Runs in the child JVM: exports every repository given, printing one exit code line per repository. */
    public static final class ChildMain {
        public static void main(String[] args) {
            Path root = Path.of(args[0]);
            for (int i = 1; i < args.length; i++) {
                String name = "repo" + i;
                int exit = new CommandLine(new DumpToDeltaCommand()).execute(args[i], "--all-indices",
                    "-o", root.resolve("tables").resolve(name).toString(), "--report", root.resolve("reports").resolve(name).toString());
                System.out.println("EXIT " + name + " " + exit);
            }
            System.exit(0);
        }
    }

    @BeforeAll
    static void exportEveryRepositoryInEveryConfiguration() throws Exception {
        for (Config config : CONFIGS) {
            Path root = Files.createDirectories(runs.resolve(config.name()));
            List<String> command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djdk.util.jar.enableMultiRelease=false",
                "-Dgefrierschrank.download.selective=" + config.selective(),
                "-Dgefrierschrank.shard.threads=" + config.threads(),
                "-Dgefrierschrank.shard.threads.minRangeLiveDocs=3",
                "-cp", String.join(File.pathSeparator, productionOrderedClassPath()),
                ChildMain.class.getName(), root.toString()));
            for (String repo : REPOS) command.add(Path.of(repo).toAbsolutePath().normalize().toString());
            Path out = root.resolve("child.out");
            Path err = root.resolve("child.err");
            Process process = new ProcessBuilder(command).directory(Files.createDirectories(root.resolve("cwd")).toFile())
                .redirectOutput(out.toFile()).redirectError(err.toFile()).start();
            assertTrue(process.waitFor(10, TimeUnit.MINUTES), config.name());
            assertEquals(0, process.exitValue(), Files.readString(err));
            Map<String, Integer> codes = new TreeMap<>();
            for (String line : Files.readAllLines(out)) {
                if (line.startsWith("EXIT ")) codes.put(line.split(" ")[1], Integer.parseInt(line.split(" ")[2]));
            }
            assertEquals(REPOS.size(), codes.size(), Files.readString(out));
            EXIT_CODES.put(config.name(), codes);
        }
    }

    record Shard(String outcome, String errorClass, String fetch, long bytesFetched, long rows) {}

    static Map<String, Shard> shards(String config) throws IOException {
        Map<String, Shard> shards = new TreeMap<>();
        try (var files = Files.walk(runs.resolve(config).resolve("reports"))) {
            for (Path report : files.filter(p -> p.toString().endsWith(".json") && !p.endsWith("manifest.json")).toList()) {
                JsonNode json = JSON.readTree(report.toFile());
                String index = runs.resolve(config).resolve("reports").relativize(report).toString();
                for (JsonNode shard : json.path("shards")) {
                    shards.put(index + "#" + shard.path("shardId").asInt(), new Shard(shard.path("outcome").asText(),
                        shard.path("errorClass").asText(null), shard.path("fetch").asText(null),
                        shard.path("bytesFetched").asLong(-1), shard.path("rowsWritten").asLong()));
                }
            }
        }
        return shards;
    }

    static List<Path> tables(String config) throws IOException {
        Path root = runs.resolve(config).resolve("tables");
        try (var files = Files.walk(root)) {
            return files.filter(p -> p.getFileName().toString().equals("_delta_log")).map(p -> root.relativize(p.getParent()))
                .sorted().toList();
        }
    }

    static List<String> commitShape(Path table) {
        var log = DeltaLog.forTable(new Configuration(), table.toString());
        var shape = new ArrayList<String>();
        for (AddFile file : log.update().getAllFiles()) shape.add("tags=" + new TreeMap<>(file.getTags()));
        var changes = log.getChanges(0, false);
        while (changes.hasNext()) {
            for (var action : changes.next().getActions()) {
                if (action instanceof SetTransaction txn) shape.add("txn=" + txn.getAppId() + "@" + txn.getVersion());
            }
        }
        shape.sort(null);
        return shape;
    }

    @Test
    void theFlagChangesNoExitCodeTableOrShardOutcome() throws Exception {
        for (String threads : List.of("1", "4")) {
            String off = "off-" + threads;
            String on = "on-" + threads;
            assertEquals(EXIT_CODES.get(off), EXIT_CODES.get(on), threads);
            List<Path> tables = tables(off);
            assertTrue(tables.size() > 40, "every fixture table: " + tables.size());
            assertEquals(tables, tables(on));
            for (Path table : tables) {
                Path a = runs.resolve(off).resolve("tables").resolve(table);
                Path b = runs.resolve(on).resolve("tables").resolve(table);
                assertEquals(DownloadConcurrencyExportTest.rows(a), DownloadConcurrencyExportTest.rows(b), table.toString());
                assertEquals(commitShape(a), commitShape(b), table.toString());
            }
            Map<String, Shard> offShards = shards(off);
            Map<String, Shard> onShards = shards(on);
            assertEquals(offShards.keySet(), onShards.keySet());
            for (var entry : offShards.entrySet()) {
                Shard full = entry.getValue();
                Shard selective = onShards.get(entry.getKey());
                assertEquals(full.outcome() + " " + full.errorClass() + " " + full.rows(),
                    selective.outcome() + " " + selective.errorClass() + " " + selective.rows(), entry.getKey());
                assertEquals("full", full.fetch(), entry.getKey());
                assertNotEquals("completed", selective.fetch(), entry.getKey());
                assertTrue(selective.bytesFetched() <= full.bytesFetched(), entry.getKey());
            }
        }
    }

    @Test
    void storedSourceShardsTakeTheSelectivePlanAndSyntheticOnesTheFullPlan() throws Exception {
        Map<String, Shard> on = shards("on-4");
        long selective = on.values().stream().filter(s -> "selective".equals(s.fetch())).count();
        assertTrue(selective >= 25, "selective shards: " + selective);
        on.forEach((key, shard) -> {
            boolean synthetic = key.contains("synth") || key.contains("logsdb") || key.contains("tsdb")
                || key.contains("vectors_default") || key.contains("test_deletes");
            if (synthetic) assertEquals("full", shard.fetch(), key);
            else assertEquals("selective", shard.fetch(), key);
        });
        assertFalse(on.isEmpty());
    }

    /** This JVM's class path (expanding surefire's manifest-only jar), gefrierschrank-lucene10 first, dnsjava absent. */
    static List<String> productionOrderedClassPath() throws IOException {
        List<String> entries = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            entries.add(entry);
            Path path = Path.of(entry);
            if (entry.endsWith(".jar") && Files.isRegularFile(path)) {
                try (JarFile jar = new JarFile(path.toFile())) {
                    var manifest = jar.getManifest();
                    String classPath = manifest == null ? null : manifest.getMainAttributes().getValue("Class-Path");
                    if (classPath != null) {
                        URI base = path.toAbsolutePath().getParent().toUri();
                        for (String reference : classPath.trim().split("\\s+")) entries.add(Path.of(base.resolve(reference)).toString());
                    }
                }
            }
        }
        List<String> ordered = new ArrayList<>();
        entries.stream().filter(e -> e.contains("gefrierschrank-lucene10")).forEach(ordered::add);
        entries.stream().filter(e -> !e.contains("gefrierschrank-lucene10") && !e.contains("dnsjava")).forEach(ordered::add);
        return ordered;
    }
}
