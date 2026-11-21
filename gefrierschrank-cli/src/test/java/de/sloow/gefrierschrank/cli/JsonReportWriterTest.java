package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.delta.RunReport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JsonReportWriterTest {

    @Test
    void writesAValidJsonReportAfterEachCallback(@TempDir Path tmp) throws Exception {
        Path reportPath = tmp.resolve("report.json");
        var writer = new JsonReportWriter(reportPath);

        writer.onShardProcessed(new RunReport("test_idx", "t0", "t1", 10, 3, 1, 0, 0, 20, 200, List.of()));
        assertTrue(Files.exists(reportPath));
        RunReport first = new ObjectMapper().readValue(reportPath.toFile(), RunReport.class);
        assertEquals("test_idx", first.indexName());
        assertEquals(1, first.committedCount());

        writer.onShardProcessed(new RunReport("test_idx", "t0", "t2", 20, 3, 2, 0, 0, 40, 400, List.of()));
        RunReport second = new ObjectMapper().readValue(reportPath.toFile(), RunReport.class);
        assertEquals(2, second.committedCount(), "the second write overwrites the first - the report always reflects the latest state");

        assertEquals(1, Files.list(tmp).count(), "no leftover temp files after an atomic move");
    }

    @Test
    void snapshotSelectionRoundTripsAndIsAbsentFromReportsWithoutIt(@TempDir Path tmp) throws Exception {
        Path reportPath = tmp.resolve("report.json");
        var writer = new JsonReportWriter(reportPath);
        var info = new RunReport.SnapshotInfo("auto", "old-uuid", "old", "new-uuid", true);
        writer.onShardProcessed(new RunReport("idx", "t0", "t1", 10, 1, 1, 0, 0, 5, 50, List.of()).withSnapshot(info));
        assertEquals(info, new ObjectMapper().readValue(reportPath.toFile(), RunReport.class).snapshot());

        writer.onShardProcessed(new RunReport("idx", "t0", "t1", 10, 1, 1, 0, 0, 5, 50, List.of()));
        assertNull(new ObjectMapper().readValue(reportPath.toFile(), RunReport.class).snapshot());
    }

    /**
     * A failing report write must not abort the index.
     * With --all-indices the listener fires on the first shard of the first index, before any
     * manifest write has created the report directory, so the writer must create it itself.
     */
    @Test
    void createsTheReportDirectoryWhenItDoesNotExistYet(@TempDir Path tmp) throws Exception {
        Path reportPath = tmp.resolve("not-created-yet").resolve("idx.json");
        var writer = new JsonReportWriter(reportPath);

        writer.onShardProcessed(new RunReport("idx", "t0", "t1", 10, 1, 1, 0, 0, 5, 50, List.of()));

        assertTrue(Files.exists(reportPath), "the writer creates missing parent directories");
        assertEquals("idx", new ObjectMapper().readValue(reportPath.toFile(), RunReport.class).indexName());
    }

    /**
     * An unwritable report must never abort the extraction it reports on: the writer swallows
     * its own failure, so no index loses good data for the sake of a log file.
     */
    @Test
    void anUnwritableReportPathDoesNotThrow(@TempDir Path tmp) throws Exception {
        // A regular file where the report's parent directory should be: createDirectories fails.
        Path blocker = tmp.resolve("blocker");
        Files.writeString(blocker, "not a directory");
        var writer = new JsonReportWriter(blocker.resolve("idx.json"));

        assertDoesNotThrow(() ->
            writer.onShardProcessed(new RunReport("idx", "t0", "t1", 10, 1, 1, 0, 0, 5, 50, List.of())));
    }
}
