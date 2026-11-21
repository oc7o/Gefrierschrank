package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.delta.RunReport;
import de.sloow.gefrierschrank.delta.RunReportListener;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static de.sloow.gefrierschrank.cli.MountedShellsExportTest.ILM;
import static org.junit.jupiter.api.Assertions.*;

/** FullIndexExporter.plan and runWithIdentity on ilm-shell-repo. */
class FullIndexExporterPlanTest {

    @Test
    void anOriginalAndItsShellPlanTheIdentitySurveyReports(@TempDir Path tmp) throws Exception {
        var sources = FullExportTest.sources(ILM);
        var exporter = FullExportTest.exporter(sources);
        var original = exporter.plan("applogs-a", tmp.resolve("applogs-a").toString());
        var shell = exporter.plan("partial-applogs-a", tmp.resolve("partial-applogs-a").toString());

        assertEquals(original.identity(), shell.identity());
        assertFalse(original.mounted());
        assertTrue(shell.mounted());
        assertFalse(original.tableStarted());
        assertFalse(shell.tableStarted());
        var inspector = sources.primary().inspector();
        assertEquals(RepositorySources.identity(sources.resolve("partial-applogs-a", inspector.latestSnapshotUuid("partial-applogs-a"))),
            shell.identity(), "the survey's identity, on the latest snapshot");
        assertEquals("primary", original.identity().source());
        assertEquals("applogs-a", original.identity().index());
        assertEquals(1, original.identity().shardCount());
        assertNotEquals(original.identity(), exporter.plan("applogs-b", tmp.resolve("applogs-b").toString()).identity());
        assertFalse(Files.exists(tmp.resolve("applogs-a")), "planning creates no table directory");
        assertFalse(Files.exists(tmp.resolve("partial-applogs-a")));
    }

    @Test
    void runWithIdentityExportsThePlannedIdentity(@TempDir Path tmp) throws Exception {
        var exporter = FullExportTest.exporter(FullExportTest.sources(ILM));
        for (String name : new String[]{"applogs-a", "partial-applogs-a", "plain-c"}) {
            String table = tmp.resolve(name).toString();
            var planned = exporter.plan(name, table);
            var exported = exporter.runWithIdentity(name, table, RunReportListener.NO_OP);
            assertEquals(planned.identity(), exported.identity(), name);
            assertEquals(0, exported.report().quarantinedCount(), name);
            assertTrue(exporter.plan(name, table).tableStarted(), name);
            RunReport again = exporter.run(name, table, RunReportListener.NO_OP);
            assertEquals(again.totalShards(), again.skippedCount(), name + ": run() is runWithIdentity's report");
        }
    }

    @Test
    void aTableHoldingOnlyItsExportMetadataCountsAsStarted(@TempDir Path tmp) throws Exception {
        String table = tmp.resolve("partial-applogs-a").toString();
        var failing = FullExportTest.exporter(FullExportTest.sources(ILM), (shard, dir) -> {
            throw new IOException("simulated shard failure");
        });
        assertFalse(failing.plan("partial-applogs-a", table).tableStarted());
        var report = failing.run("partial-applogs-a", table, RunReportListener.NO_OP);
        assertEquals(report.totalShards(), report.quarantinedCount());
        assertTrue(Files.isDirectory(Path.of(table, "_export_metadata", "_delta_log")));
        assertTrue(failing.plan("partial-applogs-a", table).tableStarted());
    }

    @Test
    void aTableDirectoryWithoutAnyCommitCountsAsStarted(@TempDir Path tmp) throws Exception {
        Path table = tmp.resolve("partial-applogs-a");
        Files.createDirectories(table);
        Files.createFile(table.resolve("_gefrierschrank.lock"));
        assertTrue(FullExportTest.exporter(FullExportTest.sources(ILM)).plan("partial-applogs-a", table.toString())
            .tableStarted(), "what a run killed before its first commit leaves");
    }

    @Test
    void anUnknownNameFailsToPlan(@TempDir Path tmp) {
        var exporter = FullExportTest.exporter(FullExportTest.sources(ILM));
        assertThrows(IOException.class, () -> exporter.plan("no-such-index", tmp.resolve("t").toString()));
        assertFalse(Files.exists(tmp.resolve("t")));
    }
}
