package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.delta.DocumentMemoryGate;
import de.sloow.gefrierschrank.delta.ShardRanges;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** {@code --shard-threads auto|1-64}: validated before any repository access; the default {@code auto} reads a shard below the per-range minimum as one range. */
class IntraShardOptionsTest {

    private static final Path DEMO_REPO = Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo");
    private static final String MIN_RANGE = "gefrierschrank.shard.threads.minRangeLiveDocs";

    private String previousThreads;
    private String previousMinimum;

    @BeforeEach
    void clearProperties() {
        previousThreads = System.clearProperty(ShardRanges.SHARD_THREADS_KEY);
        previousMinimum = System.clearProperty(MIN_RANGE);
    }

    @AfterEach
    void restoreProperties() {
        restore(ShardRanges.SHARD_THREADS_KEY, previousThreads);
        restore(MIN_RANGE, previousMinimum);
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }

    @Test
    void acceptsAutoAndOneToSixtyFour() {
        for (String value : List.of("auto", "AUTO", "1", "2", "64")) {
            var command = new DumpToDeltaCommand();
            new CommandLine(command).parseArgs("repo", "--index", "i", "-o", "out", "--shard-threads", value);
            assertEquals(value, command.intraShard.shardThreads);
            command.intraShard.apply();
            assertEquals(value, System.getProperty(ShardRanges.SHARD_THREADS_KEY));
        }
    }

    @Test
    void anythingElseExitsTwoBeforeTheRepositoryIsRead(@TempDir Path tmp) {
        String missingRepo = tmp.resolve("no-such-repo").toString();
        for (String value : List.of("0", "65", "-1", "many", "1.5", "")) {
            var err = new StringWriter();
            int exit = new CommandLine(new DumpToDeltaCommand()).setErr(new PrintWriter(err))
                .execute(missingRepo, "--index", "i", "-o", tmp.resolve("out").toString(), "--shard-threads", value);
            assertEquals(CommandLine.ExitCode.USAGE, exit, value);
            assertTrue(err.toString().contains("--shard-threads must be auto or between 1 and 64"), err.toString());
            assertNull(System.getProperty(ShardRanges.SHARD_THREADS_KEY), "a rejected command line changes nothing");
        }
        System.setProperty(ShardRanges.SHARD_THREADS_KEY, "zero");
        var err = new StringWriter();
        int exit = new CommandLine(new DumpToDeltaCommand()).setErr(new PrintWriter(err))
            .execute(missingRepo, "--index", "i", "-o", tmp.resolve("out").toString());
        assertEquals(CommandLine.ExitCode.USAGE, exit, "an invalid JVM property is a usage error too, not a failed read");
        assertTrue(err.toString().contains("--shard-threads must be auto or between 1 and 64"), err.toString());
    }

    /** The document memory gate's factor is validated with the options, not when the first large document arrives. */
    @Test
    void anInvalidReserveMultiplierExitsTwoBeforeTheRepositoryIsRead(@TempDir Path tmp) {
        String missingRepo = tmp.resolve("no-such-repo").toString();
        String previous = System.getProperty(DocumentMemoryGate.RESERVE_MULTIPLIER_PROPERTY);
        try {
            for (String value : List.of("abc", "0", "-2", "NaN", "Infinity")) {
                System.setProperty(DocumentMemoryGate.RESERVE_MULTIPLIER_PROPERTY, value);
                var err = new StringWriter();
                int exit = new CommandLine(new DumpToDeltaCommand()).setErr(new PrintWriter(err))
                    .execute(missingRepo, "--index", "i", "-o", tmp.resolve("out").toString());
                assertEquals(CommandLine.ExitCode.USAGE, exit, value);
                assertTrue(err.toString().contains("gefrierschrank.document.reserveMultiplier must be a number above 0"),
                    err.toString());
            }
        } finally {
            restore(DocumentMemoryGate.RESERVE_MULTIPLIER_PROPERTY, previous);
        }
    }

    /** The reconstruction reader's directory is validated with the options, not by quarantining every synthetic shard. */
    @Test
    void anInvalidReconstructionDirectoryIsAUsageErrorForDumpToDeltaAndAnErrorForDump(@TempDir Path tmp) throws Exception {
        String missingRepo = tmp.resolve("no-such-repo").toString();
        String property = de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker.DIRECTORY_PROPERTY;
        String previous = System.getProperty(property);
        try {
            for (String value : List.of("MMAP", "nio", "")) {
                System.setProperty(property, value);
                var err = new StringWriter();
                int exit = new CommandLine(new DumpToDeltaCommand()).setErr(new PrintWriter(err))
                    .execute(missingRepo, "--index", "i", "-o", tmp.resolve("out").toString());
                assertEquals(CommandLine.ExitCode.USAGE, exit, value);
                assertTrue(err.toString().contains("-D" + property + " must be mmap or niofs"), err.toString());

                var store = new DumpCommandTest.RecordingBlobStore(new de.sloow.gefrierschrank.core.repo.FileSystemBlobStore(DEMO_REPO));
                var dump = CommandRuns.captureStderr(() -> DumpCommandTest.dump(store, "test_data_9_3_0", tmp.resolve("o.jsonl")));
                assertEquals(1, dump.exit());
                assertTrue(dump.stderr().contains("error: InvalidDumpRequestException: -D" + property + " must be mmap or niofs"),
                    dump.stderr());
                assertEquals(List.of(), store.touched, "the repository is not even opened");
            }
            for (String value : List.of("mmap", "niofs")) {
                System.setProperty(property, value);
                assertNull(IntraShardOptions.invalidReconstructionDirectory(), value);
            }
        } finally {
            restore(property, previous);
        }
    }

    @Test
    void theDefaultReadsASmallShardAsOneRangeAndFourThreadsExportTheSameRows(@TempDir Path tmp) throws Exception {
        Path serial = tmp.resolve("serial");
        assertEquals(0, new CommandLine(new DumpToDeltaCommand())
            .execute(DEMO_REPO.toString(), "--index", "test_data_7_17_9", "-o", serial.toString()));
        assertNull(System.getProperty(ShardRanges.SHARD_THREADS_KEY), "no option, no property");
        assertEquals(1, parquetFiles(serial), "below the per-range minimum: one range, one file");

        System.setProperty(MIN_RANGE, "1");
        Path ranged = tmp.resolve("ranged");
        var run = CommandRuns.captureStderr(() -> new CommandLine(new DumpToDeltaCommand())
            .execute(DEMO_REPO.toString(), "--index", "test_data_7_17_9", "-o", ranged.toString(), "--shard-threads", "4"));
        assertEquals(0, run.exit(), run.stderr());
        assertFalse(run.stderr().contains("fs.s3a.buffer.dir"), "a local target buffers no uploads: " + run.stderr());
        assertEquals(4, parquetFiles(ranged), "one file per range");
        assertEquals(rows(serial), rows(ranged));
    }

    /** An already committed shard reports as skipped with exit 0, whichever writer committed it. */
    @Test
    void aSkippedShardKeepsExitZero() {
        var skipped = new de.sloow.gefrierschrank.delta.RunReport.ShardReport(0, "snap",
            de.sloow.gefrierschrank.delta.RunReport.Outcome.SKIPPED_ALREADY_COMMITTED, 0, 1234, "now", 5, null, null);
        var report = new de.sloow.gefrierschrank.delta.RunReport("i", "start", "end", 5, 1, 0, 1, 0, 0, 1234, List.of(skipped));
        assertEquals(0, DumpToDeltaCommand.exitCode(report));
    }

    private static long parquetFiles(Path table) throws Exception {
        try (var files = Files.list(table)) {
            return files.filter(f -> f.getFileName().toString().endsWith(".parquet")).count();
        }
    }

    private static List<String> rows(Path table) throws Exception {
        var rows = new ArrayList<String>();
        try (var it = io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).update().open()) {
            while (it.hasNext()) {
                var row = it.next();
                var values = new StringBuilder();
                for (String column : row.getSchema().getFieldNames()) {
                    values.append(column).append('=').append(row.isNullAt(column) ? "null"
                        : column.equals("_shard") || column.equals("_doc") ? Long.toString(row.getLong(column))
                        : row.getString(column)).append(';');
                }
                rows.add(values.toString());
            }
        }
        rows.sort(null);
        return rows;
    }
}
