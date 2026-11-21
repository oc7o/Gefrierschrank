package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.repo.IndexOverview;
import de.sloow.gefrierschrank.delta.DumpToDeltaPipeline;
import de.sloow.gefrierschrank.delta.ExportSettings;
import de.sloow.gefrierschrank.delta.RunReport;
import de.sloow.gefrierschrank.delta.SnapshotSelection;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * --mounted-shells skip-if-original on ilm-shell-repo (ES 9.3): applogs-a/-b each have a mounted
 * shell partial-applogs-a/-b of their latest (ILM) snapshot, listed before them; plain-c has none.
 */
class MountedShellsExportTest {

    static final Path ILM = Path.of("src/test/resources/fixtures/ilm-shell-repo");
    static final List<String> LISTING = List.of("plain-c", "partial-applogs-b", "partial-applogs-a", "applogs-b", "applogs-a");
    static final List<String> ORIGINALS = List.of("plain-c", "applogs-b", "applogs-a");
    static final String SLM_HOT = "mDXOEnwNQaiCSH1LnmXm-Q";
    static final String APPLOGS_A_ID = "P61tQ2wkTb-kUfdsfrlg5w";
    static final ObjectMapper JSON = new ObjectMapper();

    @ParameterizedTest(name = "--parallelism {0}")
    @ValueSource(strings = {"1", "4"})
    void eachShellIsAnAliasOfItsOriginalAndTheTablesEqualTheDefaultRun(String parallelism, @TempDir Path tmp)
            throws Exception {
        Path aliasRoot = tmp.resolve("alias");
        Path defaultRoot = tmp.resolve("default");
        var out = new ByteArrayOutputStream();
        assertEquals(0, captured(out, () -> dumpToDelta(ILM, "--all-indices", "-o", aliasRoot.toString(),
            "--report", tmp.resolve("alias-reports").toString(), "--parallelism", parallelism,
            "--mounted-shells", "skip-if-original")), out.toString());
        assertEquals(0, dumpToDelta(ILM, "--all-indices", "-o", defaultRoot.toString(),
            "--report", tmp.resolve("default-reports").toString(), "--parallelism", parallelism));

        assertEquals(sorted(ORIGINALS), tables(aliasRoot), "one table per group, no lock file or log for a shell");
        assertFalse(Files.exists(aliasRoot.resolve("partial-applogs-a")));
        assertFalse(Files.exists(aliasRoot.resolve("partial-applogs-b")));
        assertFalse(Files.exists(tmp.resolve("alias-reports").resolve("partial-applogs-a.json")));

        var manifest = manifest(tmp.resolve("alias-reports"));
        assertEquals(LISTING, IndexConcurrencyTest.indexNames(manifest));
        for (String shell : List.of("partial-applogs-a", "partial-applogs-b")) {
            var entry = entry(manifest, shell);
            assertEquals("ALIAS_OF", entry.path("outcome").asText());
            assertEquals(shell.substring("partial-".length()), entry.path("aliasOf").asText());
        }
        for (String original : ORIGINALS) assertEquals("COMPLETED", entry(manifest, original).path("outcome").asText());
        assertEquals(2, manifest.path("aliasCount").asInt());
        assertEquals(3, manifest.path("completedCount").asInt());
        assertTrue(manifest.path("complete").asBoolean());
        assertEquals(List.of(), IndexConcurrencyTest.pending(manifest));
        assertTrue(out.toString().contains("Mounted shells: 2 exact-identity group(s); 2 shell(s) are written as aliases"),
            out.toString());
        assertTrue(out.toString().contains("Index partial-applogs-a: alias of applogs-a"), out.toString());

        for (String original : ORIGINALS) assertSameTable(defaultRoot.resolve(original), aliasRoot.resolve(original));
    }

    @Test
    void exportIsTheDefaultModeAndChangesNothing(@TempDir Path tmp) throws Exception {
        var outputs = new ArrayList<String>();
        for (String run : List.of("implicit", "explicit")) {
            var args = new ArrayList<>(List.of("--all-indices", "-o", tmp.resolve(run).toString(),
                "--report", tmp.resolve(run + "-reports").toString()));
            if (run.equals("explicit")) args.addAll(List.of("--mounted-shells", "export"));
            var out = new ByteArrayOutputStream();
            assertEquals(0, captured(out, () -> dumpToDelta(ILM, args.toArray(String[]::new))));
            outputs.add(out.toString().replace(tmp.resolve(run).toString(), "<root>").replaceAll("in \\d+ms", "in <t>ms"));
        }
        assertEquals(outputs.get(0), outputs.get(1));
        assertFalse(outputs.get(0).contains("Mounted shells"), outputs.get(0));
        assertFalse(outputs.get(0).contains("aliases"), outputs.get(0));
        assertEquals(Files.readString(tmp.resolve("implicit-reports/manifest.json")),
            Files.readString(tmp.resolve("explicit-reports/manifest.json")), "byte for byte");
        assertFalse(Files.readString(tmp.resolve("implicit-reports/manifest.json")).contains("alias"));
        for (String index : LISTING) {
            assertEquals(withoutTimings(tmp.resolve("implicit-reports").resolve(index + ".json")),
                withoutTimings(tmp.resolve("explicit-reports").resolve(index + ".json")), index);
            assertSameTable(tmp.resolve("implicit").resolve(index), tmp.resolve("explicit").resolve(index));
        }
    }

    @Test
    void shellTablesOfAnEarlierDefaultRunAreResumedNotAliased(@TempDir Path tmp) throws Exception {
        Path root = tmp.resolve("root");
        assertEquals(0, dumpToDelta(ILM, "--all-indices", "-o", root.toString()));
        assertEquals(sorted(LISTING), tables(root));
        Path reports = tmp.resolve("reports");
        var out = new ByteArrayOutputStream();
        assertEquals(0, captured(out, () -> dumpToDelta(ILM, "--all-indices", "-o", root.toString(), "--report",
            reports.toString(), "--mounted-shells", "skip-if-original")));

        var manifest = manifest(reports);
        assertEquals(0, manifest.path("aliasCount").asInt());
        assertEquals(5, manifest.path("completedCount").asInt());
        for (String index : LISTING) {
            var report = JSON.readTree(reports.resolve(index + ".json").toFile());
            assertEquals(report.path("totalShards").asInt(), report.path("skippedCount").asInt(), index);
        }
        assertTrue(out.toString().contains("Mounted shell partial-applogs-a already has a table; exporting it"), out.toString());
        assertTrue(out.toString().contains("Mounted shells: 0 exact-identity group(s); 0 shell(s)"), out.toString());

        Path fresh = tmp.resolve("fresh");
        assertEquals(0, dumpToDelta(ILM, "--all-indices", "-o", fresh.toString(), "--mounted-shells", "skip-if-original"));
        assertEquals(sorted(ORIGINALS), tables(fresh));
        Path defaultReports = tmp.resolve("default-reports");
        assertEquals(0, dumpToDelta(ILM, "--all-indices", "-o", fresh.toString(), "--report", defaultReports.toString()));
        assertEquals(sorted(LISTING), tables(fresh));
        for (String index : LISTING) {
            var report = JSON.readTree(defaultReports.resolve(index + ".json").toFile());
            boolean shell = index.startsWith("partial-");
            assertEquals(shell ? 0 : report.path("totalShards").asInt(), report.path("skippedCount").asInt(), index);
            assertEquals(shell ? report.path("totalShards").asInt() : 0, report.path("committedCount").asInt(), index);
            assertSameTable(root.resolve(index), fresh.resolve(index));
        }
    }

    /**
     * A default run killed while it committed the shell's _export_metadata leaves the directory with
     * a lock file, a staged Parquet file and a temporary log entry, but no commit. Resumed with
     * skip-if-original, the shell is exported into that directory, not aliased past it.
     */
    @Test
    void aShellDirectoryAKilledRunLeftWithoutACommitIsResumedNotAliased(@TempDir Path tmp) throws Exception {
        Path reference = tmp.resolve("reference");
        assertEquals(0, dumpToDelta(ILM, "--all-indices", "-o", reference.toString()));
        Path root = tmp.resolve("root");
        Path shell = root.resolve("partial-applogs-a");
        Files.createDirectories(shell.resolve("_delta_log"));
        Files.createDirectories(shell.resolve("_export_metadata/_delta_log"));
        Files.createFile(shell.resolve("_gefrierschrank.lock"));
        Files.write(shell.resolve("_export_metadata/part-00000-0b9f1c3e-staged.snappy.parquet"), new byte[]{'P', 'A', 'R', '1'});
        Files.writeString(shell.resolve("_export_metadata/_delta_log/.00000000000000000000.json.0b9f1c3e.tmp"), "{\"txn\"");

        Path reports = tmp.resolve("reports");
        var out = new ByteArrayOutputStream();
        assertEquals(0, captured(out, () -> dumpToDelta(ILM, "--all-indices", "-o", root.toString(), "--report",
            reports.toString(), "--mounted-shells", "skip-if-original", "--parallelism", "4")));

        var manifest = manifest(reports);
        assertTrue(manifest.path("complete").asBoolean());
        assertEquals("COMPLETED", entry(manifest, "partial-applogs-a").path("outcome").asText());
        assertEquals("ALIAS_OF", entry(manifest, "partial-applogs-b").path("outcome").asText());
        assertEquals(1, manifest.path("aliasCount").asInt());
        assertTrue(out.toString().contains("Mounted shell partial-applogs-a already has a table; exporting it"), out.toString());
        assertEquals(sorted(List.of("plain-c", "partial-applogs-a", "applogs-b", "applogs-a")), tables(root));
        for (String index : List.of("plain-c", "partial-applogs-a", "applogs-b", "applogs-a"))
            assertSameTable(reference.resolve(index), root.resolve(index));
    }

    /**
     * applogs-a's table is pinned to the hot SLM snapshot; its shell mounts the ILM snapshot. Under
     * --snapshot auto the two export different snapshots, so both get a table.
     */
    @Test
    void anOriginalPinnedToAnotherSnapshotDoesNotGroupWithItsShell(@TempDir Path tmp) throws Exception {
        Path root = tmp.resolve("root");
        assertEquals(0, dumpToDelta(ILM, "--index", "applogs-a", "--snapshot", "slm-2026.09.20", "-o",
            root.resolve("applogs-a").toString()));
        Path reports = tmp.resolve("reports");
        assertEquals(0, dumpToDelta(ILM, "--all-indices", "-o", root.toString(), "--report", reports.toString(),
            "--mounted-shells", "skip-if-original"));

        var manifest = manifest(reports);
        assertEquals(1, manifest.path("behindLatestCount").asInt());
        var original = entry(manifest, "applogs-a");
        assertEquals("COMPLETED", original.path("outcome").asText());
        assertEquals(SLM_HOT, original.path("snapshotUuid").asText());
        assertTrue(original.path("newerSnapshotAvailable").asBoolean());
        assertEquals("COMPLETED", entry(manifest, "partial-applogs-a").path("outcome").asText());
        assertEquals("ALIAS_OF", entry(manifest, "partial-applogs-b").path("outcome").asText());
        assertEquals(1, manifest.path("aliasCount").asInt());
        assertTrue(Files.isDirectory(root.resolve("partial-applogs-a")));
        assertFalse(Files.exists(root.resolve("partial-applogs-b")));
    }

    @Test
    void aQuarantinedOriginalKeepsItsShellAnAliasAndExitsThreeUntilItIsComplete(@TempDir Path tmp) throws Exception {
        Path root = tmp.resolve("root");
        Path reports = tmp.resolve("reports");
        DumpToDeltaPipeline.ShardReaderOpener failingApplogsA = (shard, dir) -> {
            if (shard.shardPrefix().contains(APPLOGS_A_ID)) throw new IOException("simulated shard failure");
            return new Lucene10ShardReader(dir);
        };
        assertEquals(3, aliasRun(root, reports, failingApplogsA, 1));
        var manifest = manifest(reports);
        assertEquals("COMPLETED_WITH_QUARANTINE", entry(manifest, "applogs-a").path("outcome").asText());
        assertEquals("ALIAS_OF", entry(manifest, "partial-applogs-a").path("outcome").asText());
        assertFalse(manifest.path("complete").asBoolean());
        assertFalse(Files.exists(root.resolve("partial-applogs-a")));

        assertEquals(0, dumpToDelta(ILM, "--all-indices", "-o", root.toString(), "--report", reports.toString(),
            "--mounted-shells", "skip-if-original"));
        manifest = manifest(reports);
        assertEquals("COMPLETED", entry(manifest, "applogs-a").path("outcome").asText());
        assertEquals("ALIAS_OF", entry(manifest, "partial-applogs-a").path("outcome").asText());
        assertTrue(manifest.path("complete").asBoolean());
        assertEquals(sorted(ORIGINALS), tables(root));
    }

    /** The command's alias-mode wiring with an in-process opener (planning, identities, schedule). */
    static int aliasRun(Path root, Path reports, DumpToDeltaPipeline.ShardReaderOpener opener, int parallelism)
            throws Exception {
        var sources = FullExportTest.sources(ILM);
        var exporter = FullExportTest.exporter(sources, opener);
        exporter.configure(new ExportSettings(SnapshotSelection.DEFAULT, parallelism, 0));
        List<String> names = sources.primary().inspector().overview().indices().stream().map(IndexOverview::name).toList();
        var shells = DumpToDeltaCommand.planMountedShells(names, sources, exporter, root.toString(),
            DumpToDeltaCommand.PLAN_THREADS);
        DumpToDeltaCommand.IndexProcessor processor = new DumpToDeltaCommand.IndexProcessor() {
            @Override
            public RunReport process(String name) throws Exception {
                return processWithIdentity(name).report();
            }

            @Override
            public DumpToDeltaCommand.IndexOutcome processWithIdentity(String name) throws Exception {
                var exported = exporter.runWithIdentity(name, DumpToDeltaCommand.resolveIndexTableUri(root.toString(), name),
                    new JsonReportWriter(DumpToDeltaCommand.resolvePerIndexReportPath(reports, name)));
                return new DumpToDeltaCommand.IndexOutcome(exported.report(), exported.identity());
            }
        };
        return new DumpToDeltaCommand().runAllIndices(names, processor, reports, parallelism, (name, report) -> {}, shells);
    }

    static void assertSameTable(Path expected, Path actual) throws IOException {
        String[] columns = FullExportTest.columns(expected.toString());
        assertEquals(List.of(columns), List.of(FullExportTest.columns(actual.toString())), actual.toString());
        assertEquals(FullExportTest.rows(expected.toString(), columns), FullExportTest.rows(actual.toString(), columns),
            actual + ": every column of every row");
        String metadata = expected.resolve("_export_metadata").toString();
        String[] metadataColumns = FullExportTest.columns(metadata);
        assertEquals(FullExportTest.rows(metadata, metadataColumns),
            FullExportTest.rows(actual.resolve("_export_metadata").toString(), metadataColumns), actual + " metadata");
    }

    static List<String> tables(Path root) throws IOException {
        try (Stream<Path> children = Files.list(root)) {
            return children.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    static List<String> sorted(List<String> names) {
        return names.stream().sorted().toList();
    }

    static JsonNode manifest(Path reports) throws IOException {
        return JSON.readTree(reports.resolve("manifest.json").toFile());
    }

    static JsonNode entry(JsonNode manifest, String name) {
        for (var entry : manifest.path("indices")) if (entry.path("indexName").asText().equals(name)) return entry;
        throw new AssertionError(name + " not in the manifest");
    }

    static JsonNode withoutTimings(Path report) throws IOException {
        var node = (ObjectNode) JSON.readTree(report.toFile());
        node.remove(List.of("startedAt", "asOf", "elapsedMillis"));
        for (var shard : node.path("shards")) ((ObjectNode) shard).remove(List.of("finishedAt", "elapsedMillis"));
        return node;
    }

    static int dumpToDelta(Path repo, String... args) {
        var arguments = new ArrayList<String>();
        arguments.add(repo.toString());
        arguments.addAll(List.of(args));
        return new CommandLine(new DumpToDeltaCommand()).execute(arguments.toArray(String[]::new));
    }

    interface Body {
        int call() throws Exception;
    }

    static int captured(ByteArrayOutputStream out, Body body) throws Exception {
        PrintStream previous = System.out;
        System.setOut(new PrintStream(out, true));
        try {
            return body.call();
        } finally {
            System.setOut(previous);
        }
    }
}
