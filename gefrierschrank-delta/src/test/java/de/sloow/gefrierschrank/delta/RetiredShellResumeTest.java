package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Resuming a mounted index's table after retention deleted its shell snapshot, on a copy of the
 * frozen fixture: the table was written from the shell snapshot P ('mounted-backup'); then SLM
 * took a newer snapshot of the shell and retention deleted P. The backing snapshot the shell
 * references is unchanged, so AUTO completes the table as of P.
 */
class RetiredShellResumeTest {

    static final Path FROZEN = Path.of("../gefrierschrank-core/src/test/resources/fixtures/frozen-repo");
    static final String SHELL = "partial-frozen-docs-f5ffca09";
    static final String SHELL_ID = "3mTrURh1QoaPM4x1_NW8wA";
    static final String RETIRED = "-rer5a87RcOTof-rahhpvg";
    static final String RETIRED_NAME = "mounted-backup";
    static final String NEWER = "slm2SnapshotOfTheShell";
    static final String BACKING = "-qDma32ZRT-aOibqhqfTMw";
    static final String ORIGINAL_LATEST = "Iz1_FIhaSFWl0VsW-cC8MA";
    static final ObjectMapper JSON = new ObjectMapper();
    static final Configuration CONF = new Configuration();

    @Test
    void aCompleteTableIsSkippedWithoutWritingAnything(@TempDir Path tmp) throws Exception {
        Path repo = copy(tmp.resolve("repo"));
        Path table = tmp.resolve("table");
        assertEquals(2, exporter(repo, null).run(SHELL, table.toString(), RunReportListener.NO_OP).committedCount());
        Map<String, Long> before = files(table);
        retire(repo, BACKING);

        var out = new ByteArrayOutputStream();
        var report = captured(out, () -> exporter(repo, null).run(SHELL, table.toString(), RunReportListener.NO_OP));

        assertEquals(2, report.skippedCount());
        assertEquals(0, report.committedCount() + report.quarantinedCount());
        assertEquals(new RunReport.SnapshotInfo("auto", RETIRED, RETIRED_NAME, NEWER, true), report.snapshot());
        assertEquals(before, files(table), "no new or changed file, the lock file aside");
        assertTrue(out.toString().contains("Completing " + SHELL + " as of snapshot " + RETIRED_NAME
            + ", which retention has deleted; its backing snapshot " + BACKING), out.toString());
    }

    @Test
    void anIncompleteTableResumesWithRowsEqualToAnExportBeforeRetention(@TempDir Path tmp) throws Exception {
        Path repo = copy(tmp.resolve("repo"));
        String reference = tmp.resolve("reference").toString();
        exporter(repo, null).run(SHELL, reference, RunReportListener.NO_OP);
        String table = tmp.resolve("table").toString();
        var first = exporter(repo, failingShard(1)).run(SHELL, table, RunReportListener.NO_OP);
        assertEquals(1, first.committedCount());
        assertEquals(1, first.quarantinedCount());
        retire(repo, BACKING);

        var resumed = exporter(repo, null).run(SHELL, table, RunReportListener.NO_OP);

        assertEquals(List.of(RunReport.Outcome.SKIPPED_ALREADY_COMMITTED, RunReport.Outcome.COMMITTED),
            resumed.shards().stream().map(RunReport.ShardReport::outcome).toList());
        assertEquals(RETIRED, resumed.snapshot().snapshotUuid());
        String[] columns = io.delta.standalone.DeltaLog.forTable(CONF, reference).snapshot().getMetadata()
            .getSchema().getFieldNames();
        assertEquals(rows(reference, columns), rows(table, columns), "every column, lineage included, of every row");
        assertEquals(List.of(List.of(RETIRED, RETIRED_NAME)), rows(table, "_snapshot_uuid", "_snapshot").stream().distinct().toList());
        assertEquals(List.of(List.of(RETIRED, BACKING)), rows(table + "/_export_metadata", "snapshot_uuid", "source_snapshot_uuid"),
            "still exactly one metadata row");
    }

    @Test
    void aTableWithOnlyItsMetadataRowResumes(@TempDir Path tmp) throws Exception {
        Path repo = copy(tmp.resolve("repo"));
        String table = tmp.resolve("table").toString();
        assertThrows(SimulatedCrash.class, () -> exporter(repo, (shard, dir) -> {
            throw new SimulatedCrash();
        }).run(SHELL, table, RunReportListener.NO_OP));
        assertFalse(io.delta.standalone.DeltaLog.forTable(CONF, table).tableExists());
        retire(repo, BACKING);

        var report = exporter(repo, null).run(SHELL, table, RunReportListener.NO_OP);

        assertEquals(2, report.committedCount());
        assertEquals(List.of(List.of(RETIRED)), rows(table, "_snapshot_uuid").stream().distinct().toList());
    }

    @Test
    void latestAndExplicitSelectionsStillRefuseWithoutWriting(@TempDir Path tmp) throws Exception {
        Path repo = copy(tmp.resolve("repo"));
        String table = tmp.resolve("table").toString();
        exporter(repo, failingShard(1)).run(SHELL, table, RunReportListener.NO_OP);
        retire(repo, BACKING);
        long version = logVersion(table);
        for (var selection : List.of(SnapshotSelection.LATEST, SnapshotSelection.explicit(NEWER))) {
            var exporter = exporter(repo, null);
            exporter.configure(new ExportSettings(selection, 1, 0));
            assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class,
                () -> exporter.run(SHELL, table, RunReportListener.NO_OP), selection.label());
        }
        assertEquals(version, logVersion(table));
    }

    @Test
    void aShellNowMountedFromAnotherBackingSnapshotIsRefused(@TempDir Path tmp) throws Exception {
        Path repo = copy(tmp.resolve("repo"));
        String table = tmp.resolve("table").toString();
        exporter(repo, failingShard(1)).run(SHELL, table, RunReportListener.NO_OP);
        retire(repo, ORIGINAL_LATEST);
        long version = logVersion(table);
        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class,
            () -> exporter(repo, null).run(SHELL, table, RunReportListener.NO_OP));
        assertEquals(version, logVersion(table));
    }

    @Test
    void anIndexWithItsOwnDataWhoseSnapshotWasDeletedIsRefused(@TempDir Path tmp) throws Exception {
        Path repo = copy(tmp.resolve("repo"));
        String table = tmp.resolve("table").toString();
        String original = "frozen-docs-f5ffca09";
        var exporter = exporter(repo, failingShard(1));
        exporter.configure(new ExportSettings(SnapshotSelection.explicit(BACKING), 1, 0));
        exporter.run(original, table, RunReportListener.NO_OP);
        ObjectNode catalog = catalog(repo);
        removeAll((ArrayNode) catalog.path("indices").path(original).path("snapshots"), BACKING);
        writeCatalog(repo, catalog);
        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class,
            () -> exporter(repo, null).run(original, table, RunReportListener.NO_OP));
    }

    static final class SimulatedCrash extends Error {}

    /**
     * SLM snapshots the shell again as NEWER, whose shell metadata references {@code backing};
     * retention then deletes RETIRED with its blobs.
     */
    static void retire(Path repo, String backing) throws Exception {
        ObjectNode catalog = catalog(repo);
        var snapshots = (ArrayNode) catalog.path("snapshots");
        ObjectNode retired = null;
        for (var snapshot : snapshots) if (snapshot.path("uuid").asText().equals(RETIRED)) retired = (ObjectNode) snapshot;
        ObjectNode newer = retired.deepCopy();
        newer.put("name", "slm-2").put("uuid", NEWER).put("start_time_millis", retired.path("start_time_millis").asLong() + 86_400_000L);
        if (!backing.equals(BACKING)) {
            ObjectNode metadata = (ObjectNode) new RepositoryInspector(new FileSystemBlobStore(repo)).indexMetadata(SHELL, RETIRED);
            ((ObjectNode) metadata.get("settings")).put("index.store.snapshot.snapshot_uuid", backing);
            writeShellMetadata(repo, "remountedShellMeta", metadata);
            ((ObjectNode) catalog.path("index_metadata_identifiers")).put("remounted-lookup", "remountedShellMeta");
            ((ObjectNode) newer.path("index_metadata_lookup")).put(SHELL_ID, "remounted-lookup");
        }
        snapshots.add(newer);
        for (int i = snapshots.size() - 1; i >= 0; i--) if (snapshots.get(i).path("uuid").asText().equals(RETIRED)) snapshots.remove(i);
        var shellSnapshots = (ArrayNode) catalog.path("indices").path(SHELL).path("snapshots");
        removeAll(shellSnapshots, RETIRED);
        shellSnapshots.add(NEWER);
        writeCatalog(repo, catalog);
        for (String shard : List.of("0", "1")) {
            Path dir = repo.resolve("indices").resolve(SHELL_ID).resolve(shard);
            Files.move(dir.resolve("snap-" + RETIRED + ".dat"), dir.resolve("snap-" + NEWER + ".dat"));
        }
        Files.move(repo.resolve("snap-" + RETIRED + ".dat"), repo.resolve("snap-" + NEWER + ".dat"));
        Files.move(repo.resolve("meta-" + RETIRED + ".dat"), repo.resolve("meta-" + NEWER + ".dat"));
    }

    static void removeAll(ArrayNode array, String value) {
        for (int i = array.size() - 1; i >= 0; i--) if (array.get(i).asText().equals(value)) array.remove(i);
    }

    static ObjectNode catalog(Path repo) throws IOException {
        long generation = ByteBuffer.wrap(Files.readAllBytes(repo.resolve("index.latest"))).getLong();
        return (ObjectNode) JSON.readTree(Files.readString(repo.resolve("index-" + generation)));
    }

    static void writeCatalog(Path repo, ObjectNode catalog) throws IOException {
        long generation = ByteBuffer.wrap(Files.readAllBytes(repo.resolve("index.latest"))).getLong() + 1;
        Files.writeString(repo.resolve("index-" + generation), catalog.toString());
        Files.write(repo.resolve("index.latest"), ByteBuffer.allocate(8).putLong(generation).array());
    }

    static void writeShellMetadata(Path repo, String blobId, ObjectNode metadata) throws IOException {
        ObjectNode root = JSON.createObjectNode().set(SHELL, metadata);
        var bytes = new ByteArrayOutputStream();
        var out = new DataOutputStream(bytes);
        out.writeInt(0x3FD76C17); out.writeByte(4); out.writeBytes("meta"); out.writeInt(1);
        out.write(new ObjectMapper(new SmileFactory()).writeValueAsBytes(root));
        out.writeInt(0xC02893E8); out.writeInt(0);
        var crc = new java.util.zip.CRC32(); crc.update(bytes.toByteArray()); out.writeLong(crc.getValue());
        Files.write(repo.resolve("indices").resolve(SHELL_ID).resolve("meta-" + blobId + ".dat"), bytes.toByteArray());
    }

    static Path copy(Path repo) throws IOException {
        try (var paths = Files.walk(FROZEN)) {
            for (Path path : paths.toList()) {
                Path target = repo.resolve(FROZEN.relativize(path).toString());
                if (Files.isDirectory(path)) Files.createDirectories(target);
                else Files.copy(path, target);
            }
        }
        return repo;
    }

    static FullIndexExporter exporter(Path repo, DumpToDeltaPipeline.ShardReaderOpener opener) {
        var sources = new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(repo)));
        return new FullIndexExporter(sources, CONF, opener != null ? opener : (shard, dir) -> new Lucene10ShardReader(dir));
    }

    static DumpToDeltaPipeline.ShardReaderOpener failingShard(int shardId) {
        return (shard, dir) -> {
            if (shard.shardPrefix().endsWith("/" + shardId + "/")) throw new IOException("simulated shard failure");
            return new Lucene10ShardReader(dir);
        };
    }

    static long logVersion(String table) {
        return io.delta.standalone.DeltaLog.forTable(CONF, table).update().getVersion();
    }

    /** Every file of the table with its size, except the lock file. */
    static Map<String, Long> files(Path table) throws IOException {
        var files = new TreeMap<String, Long>();
        try (var paths = Files.walk(table)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                if (!path.getFileName().toString().equals(TableWriteLock.FILE_NAME))
                    files.put(table.relativize(path).toString(), Files.size(path));
            }
        }
        return files;
    }

    /** Sorted rows of the named columns, as strings. */
    static List<List<String>> rows(String table, String... columns) throws IOException {
        var result = new ArrayList<List<String>>();
        try (var it = io.delta.standalone.DeltaLog.forTable(CONF, table).update().open()) {
            while (it.hasNext()) {
                var row = it.next();
                var values = new ArrayList<String>();
                for (String column : columns) {
                    boolean isLong = row.getSchema().get(column).getDataType() instanceof io.delta.standalone.types.LongType;
                    values.add(row.isNullAt(column) ? null : isLong ? String.valueOf(row.getLong(column)) : row.getString(column));
                }
                result.add(values);
            }
        }
        result.sort(java.util.Comparator.comparing(Object::toString));
        return result;
    }

    private interface Body<T> {
        T call() throws Exception;
    }

    private static <T> T captured(ByteArrayOutputStream out, Body<T> body) throws Exception {
        var previous = System.out;
        System.setOut(new java.io.PrintStream(out, true));
        try {
            return body.call();
        } finally {
            System.setOut(previous);
        }
    }
}
