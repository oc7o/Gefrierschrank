package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.delta.FullIndexExporter;
import de.sloow.gefrierschrank.delta.RunReportListener;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Resuming a mounted index's table after retention deleted the snapshot it was written from, on a
 * repository written by Elasticsearch 9.3 (see the fixture's generate.py): logs-a was snapshotted
 * as ilm-backing, mounted as partial-logs-a and deleted. 'before' is the repository while the
 * shell's SLM snapshot slm-1 exists; in 'after' SLM took slm-2 and retention deleted slm-1. A
 * table of partial-logs-a written from slm-1 must stay usable under --snapshot auto.
 */
class RetiredShellResumeExportTest {

    static final Path FIXTURE = Path.of("../gefrierschrank-core/src/test/resources/fixtures/retired-shell-repo");
    static final Path BEFORE = FIXTURE.resolve("before");
    static final Path AFTER = FIXTURE.resolve("after");
    static final String SHELL = "partial-logs-a";
    static final String SLM_1 = "APDNjFX8RsuYqAySZtubaw";
    static final String SLM_2 = "BKIf-b4dQtOF9AucctyeXA";
    static final String BACKING = "zkZ8dIMZRxer1iayOGfb5w";
    static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void aCompleteMountedTableIsSkippedAfterRetentionWithExitZeroAndNoNewFiles(@TempDir Path tmp) throws Exception {
        Path root = tmp.resolve("root");
        assertEquals(0, dumpToDelta(BEFORE, "--all-indices", "-o", root.toString(), "--report", tmp.resolve("r1").toString()));
        Map<String, Long> before = files(root);

        Path reports = tmp.resolve("r2");
        assertEquals(0, dumpToDelta(AFTER, "--all-indices", "-o", root.toString(), "--report", reports.toString()));

        assertEquals(before, files(root), "nothing written, the lock files aside");
        var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        assertTrue(manifest.path("complete").asBoolean());
        assertEquals(1, manifest.path("behindLatestCount").asInt());
        var report = JSON.readTree(reports.resolve(SHELL + ".json").toFile());
        assertEquals(2, report.path("skippedCount").asInt());
        assertEquals(SLM_1, report.path("snapshot").path("snapshotUuid").asText());
        assertEquals("slm-1", report.path("snapshot").path("snapshotName").asText());
        assertEquals(SLM_2, report.path("snapshot").path("latestSnapshotUuid").asText());
        assertTrue(report.path("snapshot").path("newerSnapshotAvailable").asBoolean());
    }

    @Test
    void anIncompleteMountedTableResumesWithRowsByteEqualToTheReference(@TempDir Path tmp) throws Exception {
        String reference = tmp.resolve("reference").toString();
        assertEquals(0, dumpToDelta(BEFORE, "--index", SHELL, "-o", reference));
        String table = tmp.resolve("table").toString();
        var sources = new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(BEFORE)));
        var partial = new FullIndexExporter(sources, new Configuration(), (shard, dir) -> {
            if (shard.shardPrefix().endsWith("/1/")) throw new IOException("simulated shard failure");
            return new Lucene10ShardReader(dir);
        }).run(SHELL, table, RunReportListener.NO_OP);
        assertEquals(1, partial.committedCount());

        var out = new ByteArrayOutputStream();
        int exit = captured(out, () -> dumpToDelta(AFTER, "--index", SHELL, "-o", table));

        assertEquals(0, exit, out.toString());
        assertTrue(out.toString().contains("Completing " + SHELL + " as of snapshot slm-1, which retention has deleted"),
            out.toString());
        String[] columns = FullExportTest.columns(reference);
        assertEquals(List.of(columns), List.of(FullExportTest.columns(table)));
        assertEquals(FullExportTest.rows(reference, columns), FullExportTest.rows(table, columns),
            "every column of every row, the lineage naming slm-1 included");
        assertEquals(List.of(List.of(SLM_1, "slm-1", BACKING)),
            FullExportTest.rows(table, "_snapshot_uuid", "_snapshot", "_source_snapshot_uuid").stream().distinct().toList());
        assertEquals(List.of(List.of(SLM_1, BACKING)),
            FullExportTest.rows(table + "/_export_metadata", "snapshot_uuid", "source_snapshot_uuid"));
    }

    @Test
    void snapshotLatestStillRefusesTheRetiredShellsTable(@TempDir Path tmp) throws Exception {
        Path root = tmp.resolve("root");
        assertEquals(0, dumpToDelta(BEFORE, "--all-indices", "-o", root.toString()));
        Map<String, Long> before = files(root);
        var err = new ByteArrayOutputStream();
        int single = capturedErr(err, () -> dumpToDelta(AFTER, "--index", SHELL, "-o", root.resolve(SHELL).toString(),
            "--snapshot", "latest"));
        assertEquals(1, single);
        assertTrue(err.toString().contains("ArchiveTargetConflictException"), err.toString());
        Path reports = tmp.resolve("reports");
        assertEquals(4, dumpToDelta(AFTER, "--all-indices", "-o", root.toString(), "--report", reports.toString(),
            "--snapshot", "latest"));
        var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        for (var entry : manifest.path("indices")) {
            boolean shell = entry.path("indexName").asText().equals(SHELL);
            assertEquals(shell ? "FAILED" : "COMPLETED", entry.path("outcome").asText(), entry.toString());
        }
        assertEquals(before, files(root));
    }

    static int dumpToDelta(Path repo, String... args) {
        var arguments = new java.util.ArrayList<String>();
        arguments.add(repo.toString());
        arguments.addAll(List.of(args));
        return new CommandLine(new DumpToDeltaCommand()).execute(arguments.toArray(String[]::new));
    }

    /** Every file under {@code root} with its size, except table lock files. */
    static Map<String, Long> files(Path root) throws IOException {
        var files = new TreeMap<String, Long>();
        try (var paths = Files.walk(root)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                if (!path.getFileName().toString().equals("_gefrierschrank.lock"))
                    files.put(root.relativize(path).toString(), Files.size(path));
            }
        }
        return files;
    }

    private interface Body {
        int call() throws Exception;
    }

    private static int captured(ByteArrayOutputStream out, Body body) throws Exception {
        PrintStream previous = System.out;
        System.setOut(new PrintStream(out, true));
        try {
            return body.call();
        } finally {
            System.setOut(previous);
        }
    }

    private static int capturedErr(ByteArrayOutputStream err, Body body) throws Exception {
        PrintStream previous = System.err;
        System.setErr(new PrintStream(err, true));
        try {
            return body.call();
        } finally {
            System.setErr(previous);
        }
    }
}
