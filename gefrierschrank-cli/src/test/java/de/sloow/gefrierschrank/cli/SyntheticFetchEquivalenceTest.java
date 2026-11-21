package de.sloow.gefrierschrank.cli;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * m3-repo (synthetic-source shards Elasticsearch wrote without compound files) and synthcov-repo
 * exported by dump-to-delta --all-indices with and without selective fetch, the probe threshold at
 * 0, at one and four shard threads, each in a child JVM laid out like the shaded jar (see
 * SelectiveFetchEquivalenceTest): the same exit codes, tables, AddFile tags, transaction ids and
 * shard outcomes; no shard completes in a second read; every non-compound m3-repo shard whose
 * mapping can be reconstructed is fetched selectively, the compound control in full, and the one
 * whose mapping cannot be built is refused by the commit-point preflight.
 */
class SyntheticFetchEquivalenceTest {

    static final List<String> REPOS = List.of(
        "../gefrierschrank-synthetic-source/src/test/resources/fixtures/m3-repo",
        "../gefrierschrank-synthetic-source/src/test/resources/fixtures/synthcov-repo");

    @TempDir
    static Path runs;

    static final Map<String, Map<String, Integer>> EXIT_CODES = new TreeMap<>();

    @BeforeAll
    static void exportInEveryConfiguration() throws Exception {
        for (SelectiveFetchEquivalenceTest.Config config : SelectiveFetchEquivalenceTest.CONFIGS) {
            Path root = Files.createDirectories(runs.resolve(config.name()));
            List<String> command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Djdk.util.jar.enableMultiRelease=false",
                "-Dgefrierschrank.download.selective=" + config.selective(),
                "-Dgefrierschrank.download.selective.syntheticMinBytes=0",
                "-Dgefrierschrank.shard.threads=" + config.threads(),
                "-Dgefrierschrank.shard.threads.minRangeLiveDocs=3",
                "-cp", String.join(File.pathSeparator, SelectiveFetchEquivalenceTest.productionOrderedClassPath()),
                SelectiveFetchEquivalenceTest.ChildMain.class.getName(), root.toString()));
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

    private static Map<String, SelectiveFetchEquivalenceTest.Shard> shards(String config) throws Exception {
        Path reports = runs.resolve(config).resolve("reports");
        Map<String, SelectiveFetchEquivalenceTest.Shard> shards = new TreeMap<>();
        try (var files = Files.walk(reports)) {
            for (Path report : files.filter(p -> p.toString().endsWith(".json") && !p.endsWith("manifest.json")).toList()) {
                var json = new com.fasterxml.jackson.databind.ObjectMapper().readTree(report.toFile());
                for (var shard : json.path("shards")) {
                    shards.put(reports.relativize(report) + "#" + shard.path("shardId").asInt(), new SelectiveFetchEquivalenceTest.Shard(
                        shard.path("outcome").asText(), shard.path("errorClass").asText(null), shard.path("fetch").asText(null),
                        shard.path("bytesFetched").asLong(-1), shard.path("rowsWritten").asLong()));
                }
            }
        }
        return shards;
    }

    private static List<Path> tables(String config) throws Exception {
        Path root = runs.resolve(config).resolve("tables");
        try (var files = Files.walk(root)) {
            return files.filter(p -> p.getFileName().toString().equals("_delta_log")).map(p -> root.relativize(p.getParent()))
                .sorted().toList();
        }
    }

    @Test
    void theFlagChangesNoExitCodeTableOrShardOutcome() throws Exception {
        for (String threads : List.of("1", "4")) {
            String off = "off-" + threads;
            String on = "on-" + threads;
            assertEquals(EXIT_CODES.get(off), EXIT_CODES.get(on), threads);
            List<Path> tables = tables(off);
            assertTrue(tables.size() >= 18, "every exported table: " + tables);
            assertEquals(tables, tables(on));
            for (Path table : tables) {
                Path a = runs.resolve(off).resolve("tables").resolve(table);
                Path b = runs.resolve(on).resolve("tables").resolve(table);
                assertEquals(DownloadConcurrencyExportTest.rows(a), DownloadConcurrencyExportTest.rows(b), table.toString());
                assertEquals(SelectiveFetchEquivalenceTest.commitShape(a), SelectiveFetchEquivalenceTest.commitShape(b), table.toString());
            }
            var offShards = shards(off);
            var onShards = shards(on);
            assertEquals(offShards.keySet(), onShards.keySet());
            for (var entry : offShards.entrySet()) {
                var full = entry.getValue();
                var selective = onShards.get(entry.getKey());
                assertEquals(full.outcome() + " " + full.errorClass() + " " + full.rows(),
                    selective.outcome() + " " + selective.errorClass() + " " + selective.rows(), entry.getKey());
                assertNotEquals("completed", selective.fetch(), entry.getKey());
                assertTrue(selective.bytesFetched() <= full.bytesFetched(), entry.getKey());
            }
        }
    }

    @Test
    void nonCompoundShardsAreFetchedSelectivelyAndTheCompoundControlInFull() throws Exception {
        var on = shards("on-4");
        on.forEach((key, shard) -> {
            if (!key.startsWith("repo1/")) return;
            String expected;
            if (key.contains("m3_compound")) expected = "full";
            // The isolated registry cannot build this mapping: the commit-point preflight refuses the
            // shard with the class and message the full download's quarantine gives.
            else if (key.contains("m3_types_analyzed")) expected = "preflight";
            else expected = "selective";
            assertEquals(expected, shard.fetch(), key);
        });
        assertTrue(on.keySet().stream().filter(k -> k.startsWith("repo1/")).count() >= 11, on.keySet().toString());
    }
}
