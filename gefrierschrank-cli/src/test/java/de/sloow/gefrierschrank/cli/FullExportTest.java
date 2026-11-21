package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import de.sloow.gefrierschrank.core.repo.*;
import de.sloow.gefrierschrank.delta.*;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.nio.ByteBuffer;
import java.io.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class FullExportTest {
    static final Path FIXTURE = Path.of("../gefrierschrank-core/src/test/resources/fixtures/reused-index-repo");
    static final String OLD = "LmBebsKtQ3eqx1gDzGM-3w";
    static final String NEW = "w7LYyWy7RX-dXdTtQthVMw";
    static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void realElasticsearchMountedSnapshotMatchesItsSearchResults(@TempDir Path tmp) throws Exception {
        Path root = Path.of("../gefrierschrank-core/src/test/resources/fixtures");
        var expected = JSON.readTree(root.resolve("frozen-expected.json").toFile());
        var sources = sources(root.resolve("frozen-repo"));
        String mounted = expected.path("mounted_index").asText();
        String original = expected.path("index").asText();
        var exporter = exporter(sources);
        var report = exporter.run(mounted, tmp.resolve("mounted").toString(), RunReportListener.NO_OP);
        assertEquals(2, report.totalShards());
        assertEquals(2, report.rowsWrittenThisRun());
        assertEquals(0, report.quarantinedCount());
        var expectedById = new HashMap<String, com.fasterxml.jackson.databind.JsonNode>();
        for (var doc : expected.path("mounted_documents")) expectedById.put(doc.path("_id").asText(), doc.path("_source"));
        var actual = new HashMap<String, com.fasterxml.jackson.databind.JsonNode>();
        var log = io.delta.standalone.DeltaLog.forTable(new Configuration(), tmp.resolve("mounted").toString());
        try (var rows = log.snapshot().open()) {
            while (rows.hasNext()) {
                var row = rows.next();
                assertNull(actual.put(row.getString("_id"), JSON.readTree(row.getString("_source"))), "duplicate root ID");
                if (row.getString("_id").equals("a")) assertEquals("tenant", row.getString("_routing"));
            }
        }
        assertEquals(expectedById, actual, "exact logical source comparison against Elasticsearch _search");
        assertEquals(3, exporter.run(original, tmp.resolve("latest").toString(), RunReportListener.NO_OP).rowsWrittenThisRun());
        assertEquals(2, exporter.run(mounted, tmp.resolve("mounted").toString(), RunReportListener.NO_OP).skippedCount());
    }

    @Test
    void latestMetadataComesFromSameSnapshotAsDocuments() throws Exception {
        var inspector = new RepositoryInspector(new FileSystemBlobStore(FIXTURE));
        assertEquals(inspector.indexMetadata("reused_name", NEW), inspector.indexMetadata("reused_name"));
        assertNotEquals(inspector.indexMetadata("reused_name", OLD), inspector.indexMetadata("reused_name", NEW));
    }

    @Test
    void mountedIndexUsesExactBackingSnapshotAndResumesWithoutDuplicates(@TempDir Path tmp) throws Exception {
        Path repo = mountedRepository(tmp.resolve("repo"), true, OLD);
        var sources = sources(repo);
        var resolved = sources.resolve("partial-reused", "mount-snapshot");
        assertEquals(OLD, resolved.snapshotUuid(), "must not substitute the backing index's newer snapshot");
        assertEquals(2, resolved.referenceChain().size());
        String table = tmp.resolve("delta").toString();
        var exporter = exporter(sources);
        var first = exporter.run("partial-reused", table, RunReportListener.NO_OP);
        assertEquals(1, first.rowsWrittenThisRun());
        assertEquals(0, first.quarantinedCount());
        var second = exporter.run("partial-reused", table, RunReportListener.NO_OP);
        assertEquals(0, second.rowsWrittenThisRun());
        assertEquals(1, second.skippedCount());
        var log = io.delta.standalone.DeltaLog.forTable(new Configuration(), table);
        try (var rows = log.snapshot().open()) {
            assertTrue(rows.hasNext());
            var row = rows.next();
            assertEquals("partial-reused", row.getString("_index"));
            assertEquals(OLD, row.getString("_source_snapshot_uuid"));
            assertEquals("mount-snapshot", row.getString("_snapshot_uuid"));
            assertTrue(JSON.readTree(row.getString("_source")).isObject());
            assertNotNull(row.getString("_id"));
            assertFalse(rows.hasNext());
        }
        assertTrue(io.delta.standalone.DeltaLog.forTable(new Configuration(), table + "/_export_metadata").tableExists());
        var latest = exporter.run("reused_name", tmp.resolve("latest").toString(), RunReportListener.NO_OP);
        assertEquals(2, latest.rowsWrittenThisRun(), "only latest state, never a union of old versions");
    }

    @Test
    void missingExternalRepositoryFailsAndCanBeResolvedByRegisteringIt(@TempDir Path tmp) throws Exception {
        Path repo = mountedRepository(tmp.resolve("shell"), false, OLD);
        var sources = sources(repo);
        var failure = assertThrows(RepositorySources.UnresolvedSnapshotException.class,
            () -> exporter(sources).run("partial-reused", tmp.resolve("out").toString(), RunReportListener.NO_OP));
        assertTrue(failure.getMessage().contains("--repository"));
        assertFalse(Files.exists(tmp.resolve("out/_delta_log")));
        sources.add(new RepositorySources.Source("frozen", new FileSystemBlobStore(FIXTURE)));
        assertEquals(1, exporter(sources).run("partial-reused", tmp.resolve("out").toString(),
            RunReportListener.NO_OP).rowsWrittenThisRun());
    }

    @Test
    void allIndicesReportsMissingBackingDataAsFailureAndContinues(@TempDir Path tmp) throws Exception {
        Path repo = mountedRepository(tmp.resolve("shell"), false, OLD);
        Path reports = tmp.resolve("reports");
        int exit = new picocli.CommandLine(new DumpToDeltaCommand()).execute(repo.toString(),
            "--all-indices", "-o", tmp.resolve("delta").toString(), "--report", reports.toString());
        assertEquals(4, exit);
        var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        assertEquals(1, manifest.path("failedCount").asInt());
        assertTrue(manifest.toString().contains("UnresolvedSnapshotException"));
        int success = new picocli.CommandLine(new DumpToDeltaCommand()).execute(repo.toString(),
            "--all-indices", "-o", tmp.resolve("delta").toString(), "--report", reports.toString(),
            "--repository", "frozen=" + FIXTURE.toAbsolutePath());
        assertEquals(0, success);
    }

    @Test
    void cycleAndWrongSnapshotFailInsteadOfChoosingLatest(@TempDir Path tmp) throws Exception {
        var bad = sources(mountedRepository(tmp.resolve("missing"), true, "missing-snapshot"));
        assertThrows(RepositorySources.UnresolvedSnapshotException.class,
            () -> bad.resolve("partial-reused", "mount-snapshot"));
        Path repo = mountedRepository(tmp.resolve("cycle"), true, OLD);
        ObjectNode metadata = (ObjectNode) new RepositoryInspector(new FileSystemBlobStore(repo))
            .indexMetadata("partial-reused", "mount-snapshot");
        ObjectNode settings = (ObjectNode) metadata.get("settings");
        settings.put("index.store.snapshot.index_name", "partial-reused");
        settings.put("index.store.snapshot.index_uuid", "shell");
        settings.put("index.store.snapshot.snapshot_uuid", "mount-snapshot");
        writeMetadata(repo, metadata);
        assertThrows(RepositorySources.UnresolvedSnapshotException.class,
            () -> sources(repo).resolve("partial-reused", "mount-snapshot"));
    }

    @Test
    void refusesAppendingNewSnapshotToAnExistingLatestStateTable(@TempDir Path tmp) throws Exception {
        Path repo = mountedRepository(tmp.resolve("repo"), true, OLD);
        String table = tmp.resolve("out").toString();
        exporter(sources(repo)).run("partial-reused", table, RunReportListener.NO_OP);
        ObjectNode metadata = (ObjectNode) new RepositoryInspector(new FileSystemBlobStore(repo))
            .indexMetadata("partial-reused", "mount-snapshot");
        ((ObjectNode) metadata.get("settings")).put("index.store.snapshot.snapshot_uuid", NEW);
        writeMetadata(repo, metadata);
        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class,
            () -> exporter(sources(repo)).run("partial-reused", table, RunReportListener.NO_OP));
    }

    @Test
    void readsMixedFlatAndNestedSettings() throws Exception {
        var metadata = JSON.readTree("""
            {"settings":{"index":{"store.snapshot":{"snapshot_uuid":"snap"},"number_of_shards":2}}}
            """);
        assertEquals("snap", RepositorySources.setting(metadata, "index.store.snapshot.snapshot_uuid"));
        assertEquals("2", RepositorySources.setting(metadata, "index.number_of_shards"));
    }

    // --- snapshot pinning: a table is completed from the snapshot it started from ---

    static final Path FROZEN = Path.of("../gefrierschrank-core/src/test/resources/fixtures/frozen-repo");
    static final String FROZEN_INDEX = "frozen-docs-f5ffca09";
    static final String BACKING = "-qDma32ZRT-aOibqhqfTMw";
    static final String FROZEN_LATEST = "Iz1_FIhaSFWl0VsW-cC8MA";

    @Test
    void resumeAfterANewerSnapshotCompletesTheTablesOwnSnapshot(@TempDir Path tmp) throws Exception {
        Path repo = frozenBeforeLatest(tmp.resolve("repo"));
        String table = tmp.resolve("table").toString();
        var first = exporter(sources(repo), failingShard(1)).run(FROZEN_INDEX, table, RunReportListener.NO_OP);
        assertEquals(RunReport.Outcome.COMMITTED, first.shards().get(0).outcome());
        assertEquals(RunReport.Outcome.QUARANTINED, first.shards().get(1).outcome());
        assertEquals(new RunReport.SnapshotInfo("auto", BACKING, "backing", BACKING, false), first.snapshot());

        writeCatalog(repo, 5, frozenCatalog());
        var listened = new ArrayList<RunReport>();
        var second = exporter(sources(repo)).run(FROZEN_INDEX, table, listened::add);

        assertEquals(List.of(RunReport.Outcome.SKIPPED_ALREADY_COMMITTED, RunReport.Outcome.COMMITTED),
            second.shards().stream().map(RunReport.ShardReport::outcome).toList());
        assertEquals(0, second.quarantinedCount());
        assertEquals(new RunReport.SnapshotInfo("auto", BACKING, "backing", FROZEN_LATEST, true), second.snapshot());
        assertTrue(listened.stream().allMatch(r -> second.snapshot().equals(r.snapshot())),
            "intermediate reports carry the snapshot too");

        var exporter = exporter(sources(repo));
        exporter.configure(new ExportSettings(SnapshotSelection.explicit("backing"), 1, 0));
        String fresh = tmp.resolve("fresh").toString();
        exporter.run(FROZEN_INDEX, fresh, RunReportListener.NO_OP);
        assertEquals(rows(fresh, "_shard", "_id", "_source"), rows(table, "_shard", "_id", "_source"));
        var ids = rows(table, "_shard", "_id");
        assertEquals(new HashSet<>(ids).size(), ids.size(), "no duplicate (_shard, _id)");
        assertEquals(List.of(List.of(BACKING)), rows(table + "/_export_metadata", "snapshot_uuid"));
    }

    @Test
    void latestSelectionRefusesATableStartedFromAnOlderSnapshotWithoutWriting(@TempDir Path tmp) throws Exception {
        Path repo = frozenBeforeLatest(tmp.resolve("repo"));
        String table = tmp.resolve("table").toString();
        exporter(sources(repo), failingShard(1)).run(FROZEN_INDEX, table, RunReportListener.NO_OP);
        writeCatalog(repo, 5, frozenCatalog());
        long docVersion = logVersion(table);
        long metadataVersion = logVersion(table + "/_export_metadata");

        var exporter = exporter(sources(repo));
        exporter.configure(new ExportSettings(SnapshotSelection.LATEST, 1, 0));
        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class,
            () -> exporter.run(FROZEN_INDEX, table, RunReportListener.NO_OP));
        assertEquals(docVersion, logVersion(table));
        assertEquals(metadataVersion, logVersion(table + "/_export_metadata"));
    }

    @Test
    void aSnapshotRemovedByRetentionStillConflicts(@TempDir Path tmp) throws Exception {
        Path repo = frozenBeforeLatest(tmp.resolve("repo"));
        String table = tmp.resolve("table").toString();
        exporter(sources(repo), failingShard(1)).run(FROZEN_INDEX, table, RunReportListener.NO_OP);
        ObjectNode retained = frozenCatalog();
        var snapshots = (com.fasterxml.jackson.databind.node.ArrayNode)
            retained.path("indices").path(FROZEN_INDEX).path("snapshots");
        for (int i = snapshots.size() - 1; i >= 0; i--) if (snapshots.get(i).asText().equals(BACKING)) snapshots.remove(i);
        writeCatalog(repo, 6, retained);

        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class,
            () -> exporter(sources(repo)).run(FROZEN_INDEX, table, RunReportListener.NO_OP));
    }

    @Test
    void aCrashAfterTheMetadataCommitPinsTheSnapshotFromExportMetadata(@TempDir Path tmp) throws Exception {
        Path repo = frozenBeforeLatest(tmp.resolve("repo"));
        String table = tmp.resolve("table").toString();
        assertThrows(SimulatedCrash.class, () -> exporter(sources(repo), (shard, dir) -> {
            throw new SimulatedCrash();
        }).run(FROZEN_INDEX, table, RunReportListener.NO_OP));
        assertFalse(io.delta.standalone.DeltaLog.forTable(new Configuration(), table).tableExists());
        writeCatalog(repo, 5, frozenCatalog());

        var report = exporter(sources(repo)).run(FROZEN_INDEX, table, RunReportListener.NO_OP);

        assertEquals(BACKING, report.snapshot().snapshotUuid());
        assertEquals(2, report.committedCount());
        assertEquals(List.of(List.of(BACKING)), rows(table + "/_export_metadata", "snapshot_uuid"));
    }

    @Test
    void latestSelectionRefusesAForeignMetadataRowBeforeAnyShardDownload(@TempDir Path tmp) throws Exception {
        Path repo = frozenBeforeLatest(tmp.resolve("repo"));
        String table = tmp.resolve("table").toString();
        assertThrows(SimulatedCrash.class, () -> exporter(sources(repo), (shard, dir) -> {
            throw new SimulatedCrash();
        }).run(FROZEN_INDEX, table, RunReportListener.NO_OP));
        writeCatalog(repo, 5, frozenCatalog());

        var store = new CountingBlobStore(new FileSystemBlobStore(repo));
        var exporter = exporter(new RepositorySources(new RepositorySources.Source("primary", store)));
        exporter.configure(new ExportSettings(SnapshotSelection.LATEST, 1, 0));
        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class,
            () -> exporter.run(FROZEN_INDEX, table, RunReportListener.NO_OP));
        assertEquals(0, store.dataBlobReads(), "refused before any shard file is fetched");
        assertEquals(List.of(List.of(BACKING)), rows(table + "/_export_metadata", "snapshot_uuid"));
    }

    /**
     * 1.0.0 shape: a first run quarantined every shard (metadata row for 'backing' only), and the
     * next run came after 'latest' and exported the empty table from it in place, appending a
     * second metadata row. The document table is complete and consistent with 'latest', so it
     * must keep completing and rerunning instead of being refused for the stale row.
     */
    @Test
    void aTableWithA1_0_0SecondMetadataRowCompletesAndReruns(@TempDir Path tmp) throws Exception {
        Path repo = frozenBeforeLatest(tmp.resolve("repo"));
        Path root = tmp.resolve("delta");
        String table = DumpToDeltaCommand.resolveIndexTableUri(root.toString(), FROZEN_INDEX);
        DumpToDeltaPipeline.ShardReaderOpener failEveryShard = (shard, dir) -> {
            throw new IOException("simulated shard failure");
        };
        assertEquals(2, exporter(sources(repo), failEveryShard).run(FROZEN_INDEX, table, RunReportListener.NO_OP)
            .quarantinedCount());
        writeCatalog(repo, 5, frozenCatalog());
        String latestMetadataOnly = tmp.resolve("latest-metadata-only").toString();
        var latest = exporter(sources(repo), failEveryShard);
        latest.configure(new ExportSettings(SnapshotSelection.LATEST, 1, 0));
        latest.run(FROZEN_INDEX, latestMetadataOnly, RunReportListener.NO_OP);
        appendMetadataRow(latestMetadataOnly + "/_export_metadata", table + "/_export_metadata");
        var twoRows = List.of(List.of(BACKING), List.of(FROZEN_LATEST));
        assertEquals(twoRows, rows(table + "/_export_metadata", "snapshot_uuid"));

        var completed = exporter(sources(repo)).run(FROZEN_INDEX, table, RunReportListener.NO_OP);
        assertEquals(2, completed.committedCount());
        assertEquals(FROZEN_LATEST, completed.snapshot().snapshotUuid());

        var rerun = exporter(sources(repo)).run(FROZEN_INDEX, table, RunReportListener.NO_OP);
        assertEquals(2, rerun.skippedCount());
        assertEquals(new RunReport.SnapshotInfo("auto", FROZEN_LATEST, "latest", FROZEN_LATEST, false), rerun.snapshot());
        var strict = exporter(sources(repo));
        strict.configure(new ExportSettings(SnapshotSelection.LATEST, 1, 0));
        assertEquals(2, strict.run(FROZEN_INDEX, table, RunReportListener.NO_OP).skippedCount());
        Path reports = tmp.resolve("reports");
        assertEquals(0, allIndices(repo, root, reports, ExportSettings.DEFAULT));
        assertEquals(2, JSON.readTree(reports.resolve(FROZEN_INDEX + ".json").toFile()).path("skippedCount").asInt());
        assertEquals(twoRows, rows(table + "/_export_metadata", "snapshot_uuid"), "no further metadata row");
    }

    /**
     * The README's remedy for a refused table: deleting just that index's directory. Retention
     * of 'backing' also breaks the mounted index, so the --all-indices loop runs over this index.
     */
    @Test
    void deletingARefusedTablesDirectoryReexportsJustThatIndex(@TempDir Path tmp) throws Exception {
        Path repo = frozenBeforeLatest(tmp.resolve("repo"));
        Path root = tmp.resolve("delta");
        Path reports = tmp.resolve("reports");
        String table = DumpToDeltaCommand.resolveIndexTableUri(root.toString(), FROZEN_INDEX);
        assertEquals(2, exporter(sources(repo), (shard, dir) -> {
            throw new IOException("simulated shard failure");
        }).run(FROZEN_INDEX, table, RunReportListener.NO_OP).quarantinedCount());
        ObjectNode retained = frozenCatalog();
        var snapshots = (com.fasterxml.jackson.databind.node.ArrayNode)
            retained.path("indices").path(FROZEN_INDEX).path("snapshots");
        for (int i = snapshots.size() - 1; i >= 0; i--) if (snapshots.get(i).asText().equals(BACKING)) snapshots.remove(i);
        writeCatalog(repo, 6, retained);

        var only = List.of(FROZEN_INDEX);
        assertEquals(4, allIndices(repo, root, reports, ExportSettings.DEFAULT, only));
        var refused = JSON.readTree(reports.resolve("manifest.json").toFile()).path("indices").get(0);
        assertEquals("FAILED", refused.path("outcome").asText());
        assertEquals("ArchiveTargetConflictException", refused.path("errorClass").asText());

        try (var paths = Files.walk(Path.of(table))) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
        }
        assertEquals(0, allIndices(repo, root, reports, ExportSettings.DEFAULT, only));
        assertTrue(JSON.readTree(reports.resolve("manifest.json").toFile()).path("complete").asBoolean());
        var report = JSON.readTree(reports.resolve(FROZEN_INDEX + ".json").toFile());
        assertEquals(2, report.path("committedCount").asInt());
        assertEquals(FROZEN_LATEST, report.path("snapshot").path("snapshotUuid").asText());
    }

    /** Copies the only row of one _export_metadata table, under its own key, into another. */
    static void appendMetadataRow(String from, String to) throws IOException {
        var conf = new Configuration();
        var files = io.delta.standalone.DeltaLog.forTable(conf, from).snapshot().getAllFiles();
        assertEquals(1, files.size());
        String key = files.get(0).getTags().get("gefrierschrank.shard");
        // The writer adds its own _unmapped_fields column; the row carries the metadata fields only.
        String[] columns = Arrays.stream(columns(from)).filter(c -> !c.startsWith("_")).toArray(String[]::new);
        var row = JSON.createObjectNode();
        var properties = JSON.createObjectNode();
        var values = rows(from, columns);
        assertEquals(1, values.size());
        for (int i = 0; i < columns.length; i++) {
            row.put(columns[i], values.get(0).get(i));
            properties.putObject(columns[i]).put("type", "keyword");
        }
        new DeltaTableWriter(to).write(conf, List.of(row.toString()).iterator(), new IndexMapping(properties), key);
    }

    @Test
    void allIndicesRerunAfterANewerSnapshotStaysCompleteAndCountsTablesBehindLatest(@TempDir Path tmp) throws Exception {
        Path repo = frozenBeforeLatest(tmp.resolve("repo"));
        Path root = tmp.resolve("delta");
        Path reports = tmp.resolve("reports");
        assertEquals(0, allIndices(repo, root, reports, ExportSettings.DEFAULT));
        assertEquals(0, JSON.readTree(reports.resolve("manifest.json").toFile()).path("behindLatestCount").asInt());
        writeCatalog(repo, 5, frozenCatalog());

        assertEquals(0, allIndices(repo, root, reports, ExportSettings.DEFAULT));

        var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        assertTrue(manifest.path("complete").asBoolean());
        assertEquals(1, manifest.path("behindLatestCount").asInt());
        for (var entry : manifest.path("indices")) {
            boolean behind = entry.path("indexName").asText().equals(FROZEN_INDEX);
            assertEquals(behind, entry.path("newerSnapshotAvailable").asBoolean(), entry.toString());
            if (behind) assertEquals(BACKING, entry.path("snapshotUuid").asText());
        }
        assertTrue(JSON.readTree(reports.resolve(FROZEN_INDEX + ".json").toFile())
            .path("snapshot").path("newerSnapshotAvailable").asBoolean(), "the per-index report says so too");
        assertEquals(4, allIndices(repo, root, reports, new ExportSettings(SnapshotSelection.LATEST, 1, 0)),
            "--snapshot latest restores the strict refusal for the table behind latest");
    }

    /**
     * The --all-indices loop exactly as the command wires it, but reading shards through a plain
     * in-process Lucene 10 opener instead of the command's own.
     */
    static int allIndices(Path repo, Path root, Path reports, ExportSettings settings) throws IOException {
        return allIndices(repo, root, reports, settings, sources(repo).primary().inspector().overview().indices()
            .stream().map(IndexOverview::name).toList());
    }

    static int allIndices(Path repo, Path root, Path reports, ExportSettings settings, List<String> names) {
        var sources = sources(repo);
        var exporter = exporter(sources);
        exporter.configure(settings);
        return new DumpToDeltaCommand().runAllIndices(names, name -> exporter.run(name,
            DumpToDeltaCommand.resolveIndexTableUri(root.toString(), name),
            new JsonReportWriter(DumpToDeltaCommand.resolvePerIndexReportPath(reports, name))), reports);
    }

    // --- concurrent shards produce exactly the serial result ---

    @Test
    void parallelExportIsRowIdenticalToSerial(@TempDir Path tmp) throws Exception {
        Path fixtures = Path.of("../gefrierschrank-core/src/test/resources/fixtures");
        var cases = List.of(
            List.of("frozen-repo", FROZEN_INDEX),
            List.of("frozen-repo", "partial-frozen-docs-f5ffca09"),
            List.of("demo-repo", "test_data_8_17_0"),
            List.of("demo-repo", "test_data_9_3_0"));
        for (var c : cases) {
            Path repo = fixtures.resolve(c.get(0));
            String index = c.get(1);
            String serialTable = tmp.resolve("serial-" + index).toString();
            String parallelTable = tmp.resolve("parallel-" + index).toString();
            var serial = exporter(sources(repo)).run(index, serialTable, RunReportListener.NO_OP);
            var parallelExporter = exporter(sources(repo));
            parallelExporter.configure(new ExportSettings(SnapshotSelection.DEFAULT, 4, 1024));
            var parallel = parallelExporter.run(index, parallelTable, RunReportListener.NO_OP);

            assertEquals(0, serial.quarantinedCount(), index);
            assertEquals(serial.committedCount(), parallel.committedCount(), index);
            assertEquals(serial.rowsWrittenThisRun(), parallel.rowsWrittenThisRun(), index);
            assertEquals(outcomes(serial), outcomes(parallel), index);
            String[] columns = columns(serialTable);
            assertEquals(List.of(columns), List.of(columns(parallelTable)), index);
            assertEquals(rows(serialTable, columns), rows(parallelTable, columns), index + ": every column of every row");
            assertEquals(rows(serialTable + "/_export_metadata", "index_name", "snapshot_uuid", "source_snapshot_uuid"),
                rows(parallelTable + "/_export_metadata", "index_name", "snapshot_uuid", "source_snapshot_uuid"), index);

            var resumed = parallelExporter.run(index, parallelTable, RunReportListener.NO_OP);
            assertEquals(resumed.totalShards(), resumed.skippedCount(), index + ": a resume skips every shard");
            assertEquals(0, resumed.rowsWrittenThisRun());
        }
    }

    @Test
    void allIndicesWithParallelismGivesTheSerialExitCodeAndManifestCounts(@TempDir Path tmp) throws Exception {
        Path repo = frozenBeforeLatest(tmp.resolve("repo"));
        int serial = allIndices(repo, tmp.resolve("serial"), tmp.resolve("serial-reports"), ExportSettings.DEFAULT);
        int parallel = allIndices(repo, tmp.resolve("parallel"), tmp.resolve("parallel-reports"),
            new ExportSettings(SnapshotSelection.DEFAULT, 4, 0));
        assertEquals(0, serial);
        assertEquals(serial, parallel);
        assertEquals(manifestCounts(tmp.resolve("serial-reports")), manifestCounts(tmp.resolve("parallel-reports")));

        // Through the real command line and its own runtime opener as well.
        int serialCli = new picocli.CommandLine(new DumpToDeltaCommand()).execute(repo.toString(), "--all-indices",
            "-o", tmp.resolve("cli-serial").toString(), "--report", tmp.resolve("cli-serial-reports").toString());
        int parallelCli = new picocli.CommandLine(new DumpToDeltaCommand()).execute(repo.toString(), "--all-indices",
            "-o", tmp.resolve("cli-parallel").toString(), "--report", tmp.resolve("cli-parallel-reports").toString(),
            "--parallelism", "4");
        assertEquals(0, serialCli);
        assertEquals(0, parallelCli);
        assertEquals(manifestCounts(tmp.resolve("serial-reports")), manifestCounts(tmp.resolve("cli-serial-reports")));
        assertEquals(manifestCounts(tmp.resolve("serial-reports")), manifestCounts(tmp.resolve("cli-parallel-reports")));
    }

    /**
     * --parallelism above 1 also runs indices concurrently. Every table, every manifest entry and
     * every per-index report must equal a sequential run's, apart from timings.
     */
    @Test
    void allIndicesRunConcurrentlyGivesTheSequentialTablesManifestAndReports(@TempDir Path tmp) throws Exception {
        Path fixtures = Path.of("../gefrierschrank-core/src/test/resources/fixtures");
        for (String name : List.of("demo-repo", "frozen-repo", "reused-index-repo")) {
            Path repo = fixtures.resolve(name);
            var runs = new ArrayList<Path>();
            for (String parallelism : List.of("1", "4")) {
                Path run = tmp.resolve(name + "-p" + parallelism);
                runs.add(run);
                assertEquals(0, new picocli.CommandLine(new DumpToDeltaCommand()).execute(repo.toString(), "--all-indices",
                    "-o", run.resolve("root").toString(), "--report", run.resolve("reports").toString(),
                    "--parallelism", parallelism), name + " p" + parallelism);
            }
            var serialManifest = JSON.readTree(runs.get(0).resolve("reports/manifest.json").toFile());
            assertEquals(serialManifest, JSON.readTree(runs.get(1).resolve("reports/manifest.json").toFile()), name);
            for (var entry : serialManifest.path("indices")) {
                String index = entry.path("indexName").asText();
                String serial = runs.get(0).resolve("root").resolve(index).toString();
                String concurrent = runs.get(1).resolve("root").resolve(index).toString();
                String[] columns = columns(serial);
                assertEquals(List.of(columns), List.of(columns(concurrent)), index);
                assertEquals(rows(serial, columns), rows(concurrent, columns), index + ": every column of every row");
                assertEquals(withoutTimings(runs.get(0).resolve("reports").resolve(index + ".json")),
                    withoutTimings(runs.get(1).resolve("reports").resolve(index + ".json")), index);
            }
        }
    }

    private static com.fasterxml.jackson.databind.JsonNode withoutTimings(Path report) throws IOException {
        var node = (ObjectNode) JSON.readTree(report.toFile());
        node.remove(List.of("startedAt", "asOf", "elapsedMillis"));
        for (var shard : node.path("shards")) ((ObjectNode) shard).remove(List.of("finishedAt", "elapsedMillis"));
        return node;
    }

    static List<Object> manifestCounts(Path reports) throws IOException {
        var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        var counts = new ArrayList<Object>();
        for (String field : List.of("totalIndices", "completedCount", "completedWithQuarantineCount", "failedCount",
                "behindLatestCount"))
            counts.add(manifest.path(field).asInt());
        counts.add(manifest.path("complete").asBoolean());
        counts.add(manifest.path("pendingIndices").toString());
        return counts;
    }

    static List<String> outcomes(RunReport report) {
        return report.shards().stream().map(s -> s.shardId() + ":" + s.outcome() + ":" + s.rowsWritten()).toList();
    }

    static String[] columns(String table) {
        return io.delta.standalone.DeltaLog.forTable(new Configuration(), table).snapshot().getMetadata()
            .getSchema().getFieldNames();
    }

    /**
     * Every shard commit must go through the writer instance whose requireArchiveSnapshot vetted
     * the table, so any guard that call arms covers the commits too.
     */
    @Test
    void theExporterCommitsThroughTheWriterThatCheckedTheTable() throws Exception {
        // Whitespace is collapsed, so line breaks and indentation do not matter.
        String source = Files.readString(Path.of(
            "../gefrierschrank-delta/src/main/java/de/sloow/gefrierschrank/delta/FullIndexExporter.java"))
            .replaceAll("\\s+", " ").replaceAll(" ?([(),.;=]) ?", "$1");
        var declaration = java.util.regex.Pattern.compile("var (\\w+)=new DeltaTableWriter\\(tableUri\\);")
            .matcher(source);
        assertTrue(declaration.find(), "one writer for the document table");
        String writer = declaration.group(1);
        assertFalse(declaration.find(), "exactly one writer for the document table");
        assertTrue(source.contains(writer + ".requireArchiveSnapshot(conf,context,shardCount);"));
        var pipelines = java.util.regex.Pattern.compile("new DumpToDeltaPipeline\\(([^;]*?),conf,opener\\)")
            .matcher(source);
        assertTrue(pipelines.find(), "the exporter builds one pipeline");
        assertTrue(pipelines.group(1).endsWith("," + writer), "the pipeline receives that same writer");
        assertFalse(pipelines.find(), "and only one");
        assertEquals(1, source.split("new DeltaTableWriter\\(tableUri\\)", -1).length - 1,
            "no second writer for the document table, declared or inline");
    }

    static final class SimulatedCrash extends Error {}

    static DumpToDeltaPipeline.ShardReaderOpener failingShard(int shardId) {
        return (shard, dir) -> {
            if (shard.shardPrefix().endsWith("/" + shardId + "/")) throw new IOException("simulated shard failure");
            return new Lucene10ShardReader(dir);
        };
    }

    /** A private copy of frozen-repo whose current generation predates the 'latest' snapshot. */
    static Path frozenBeforeLatest(Path repo) throws Exception {
        try (var paths = Files.walk(FROZEN)) {
            for (Path path : paths.toList()) {
                Path target = repo.resolve(FROZEN.relativize(path).toString());
                if (Files.isDirectory(path)) Files.createDirectories(target);
                else Files.copy(path, target);
            }
        }
        ObjectNode catalog = frozenCatalog();
        var snapshots = (com.fasterxml.jackson.databind.node.ArrayNode) catalog.path("snapshots");
        for (int i = snapshots.size() - 1; i >= 0; i--)
            if (snapshots.get(i).path("uuid").asText().equals(FROZEN_LATEST)) snapshots.remove(i);
        var indexSnapshots = (com.fasterxml.jackson.databind.node.ArrayNode)
            catalog.path("indices").path(FROZEN_INDEX).path("snapshots");
        for (int i = indexSnapshots.size() - 1; i >= 0; i--)
            if (indexSnapshots.get(i).asText().equals(FROZEN_LATEST)) indexSnapshots.remove(i);
        writeCatalog(repo, 4, catalog);
        return repo;
    }

    static ObjectNode frozenCatalog() throws IOException {
        return (ObjectNode) JSON.readTree(Files.readString(FROZEN.resolve("index-3")));
    }

    static void writeCatalog(Path repo, long generation, ObjectNode catalog) throws IOException {
        Files.writeString(repo.resolve("index-" + generation), catalog.toString());
        Files.write(repo.resolve("index.latest"), ByteBuffer.allocate(8).putLong(generation).array());
    }

    static long logVersion(String table) {
        return io.delta.standalone.DeltaLog.forTable(new Configuration(), table).snapshot().getVersion();
    }

    /** Sorted rows of the named columns, as strings; a multiset comparison when compared whole. */
    static List<List<String>> rows(String table, String... columns) throws IOException {
        var result = new ArrayList<List<String>>();
        try (var it = io.delta.standalone.DeltaLog.forTable(new Configuration(), table).snapshot().open()) {
            while (it.hasNext()) {
                var row = it.next();
                var values = new ArrayList<String>();
                for (String column : columns) values.add(row.isNullAt(column) ? null : String.valueOf(value(row, column)));
                result.add(values);
            }
        }
        result.sort(java.util.Comparator.comparing(Object::toString));
        return result;
    }

    private static Object value(io.delta.standalone.data.RowRecord row, String column) {
        return row.getSchema().get(column).getDataType() instanceof io.delta.standalone.types.LongType
            ? row.getLong(column) : row.getString(column);
    }

    /** Counts reads of shard data blobs ({@code __<uuid>} files); delegates everything. */
    static final class CountingBlobStore implements BlobStore {
        private final BlobStore delegate;
        private final java.util.concurrent.atomic.AtomicInteger dataBlobReads = new java.util.concurrent.atomic.AtomicInteger();

        CountingBlobStore(BlobStore delegate) { this.delegate = delegate; }

        int dataBlobReads() { return dataBlobReads.get(); }

        @Override public Optional<BlobMetadata> stat(String key) { return delegate.stat(key); }
        @Override public InputStream read(String key) throws IOException {
            if (key.substring(key.lastIndexOf('/') + 1).startsWith("__")) dataBlobReads.incrementAndGet();
            return delegate.read(key);
        }
        @Override public List<String> list(String prefix) { return delegate.list(prefix); }
        @Override public List<BlobMetadata> listWithSizes(String prefix) { return delegate.listWithSizes(prefix); }
        @Override public void put(String key, byte[] content) { delegate.put(key, content); }
    }

    static RepositorySources sources(Path path) {
        return new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(path)));
    }
    static FullIndexExporter exporter(RepositorySources sources) {
        return new FullIndexExporter(sources, new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
    }
    static FullIndexExporter exporter(RepositorySources sources, DumpToDeltaPipeline.ShardReaderOpener opener) {
        return new FullIndexExporter(sources, new Configuration(), opener);
    }

    static Path mountedRepository(Path repo, boolean includeBacking, String backingSnapshot) throws Exception {
        Files.createDirectories(repo);
        if (includeBacking) {
            try (var paths = Files.walk(FIXTURE)) {
                for (Path path : paths.toList()) {
                    Path target = repo.resolve(FIXTURE.relativize(path));
                    if (Files.isDirectory(path)) Files.createDirectories(target);
                    else Files.copy(path, target);
                }
            }
        }
        ObjectNode catalog = (ObjectNode) JSON.readTree(Files.readString(FIXTURE.resolve("index-3")));
        if (!includeBacking) {
            catalog.putObject("indices");
            catalog.putArray("snapshots");
            catalog.putObject("index_metadata_identifiers");
            catalog.put("uuid", "shell-repository");
        }
        var index = catalog.withObject("indices").putObject("partial-reused");
        index.put("id", "shell");
        index.putArray("snapshots").add("mount-snapshot");
        index.putArray("shard_generations").add("shell-generation");
        var snap = catalog.withArray("snapshots").addObject();
        snap.put("name", "mount-backup").put("uuid", "mount-snapshot").put("state", 1);
        snap.put("start_time_millis", 9999999999999L);
        Files.writeString(repo.resolve("index-4"), catalog.toString());
        Files.write(repo.resolve("index.latest"), ByteBuffer.allocate(8).putLong(4).array());
        var metadata = JSON.createObjectNode();
        metadata.putObject("mappings").putObject("properties");
        var settings = metadata.putObject("settings");
        settings.put("index.number_of_shards", 1);
        settings.put("index.store.type", "snapshot");
        settings.put("index.store.snapshot.repository_name", "frozen");
        settings.put("index.store.snapshot.repository_uuid", "xJ3qJ05pRauBZlK5BNWGNQ");
        settings.put("index.store.snapshot.index_name", "reused_name");
        settings.put("index.store.snapshot.index_uuid", "DNjPaXmgTWiaT_swArrkDg");
        settings.put("index.store.snapshot.snapshot_uuid", backingSnapshot);
        writeMetadata(repo, metadata);
        return repo;
    }

    static void writeMetadata(Path repo, ObjectNode metadata) throws Exception {
        ObjectNode root = JSON.createObjectNode().set("partial-reused", metadata);
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(0x3FD76C17); out.writeByte(4); out.writeBytes("meta"); out.writeInt(1);
        out.write(new ObjectMapper(new SmileFactory()).writeValueAsBytes(root));
        out.writeInt(0xC02893E8); out.writeInt(0);
        var crc = new java.util.zip.CRC32(); crc.update(bytes.toByteArray()); out.writeLong(crc.getValue());
        Path file = repo.resolve("indices/shell/meta-mount-snapshot.dat");
        Files.createDirectories(file.getParent()); Files.write(file, bytes.toByteArray());
    }
}
