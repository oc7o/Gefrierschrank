package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.delta.FullIndexExporter;
import de.sloow.gefrierschrank.delta.RunReport;
import de.sloow.gefrierschrank.delta.RunReportListener;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** --download-concurrency changes how shards are fetched, never what is exported. */
class DownloadConcurrencyExportTest {

    static final Path FIXTURES = Path.of("../gefrierschrank-core/src/test/resources/fixtures");

    private String previousProperty;

    @BeforeEach
    void rememberProperty() {
        previousProperty = System.getProperty(ShardMaterializer.CONCURRENCY_PROPERTY);
    }

    @AfterEach
    void restoreProperty() {
        if (previousProperty == null) System.clearProperty(ShardMaterializer.CONCURRENCY_PROPERTY);
        else System.setProperty(ShardMaterializer.CONCURRENCY_PROPERTY, previousProperty);
    }

    /**
     * Goes through FullIndexExporter with a plain in-process Lucene 10 opener rather than the CLI's
     * runtime opener.
     */
    @Test
    void sequentialAndParallelDownloadsExportIdenticalRows(@TempDir Path tmp) throws Exception {
        var exports = List.of(
            new String[]{"frozen-repo", "frozen-docs-f5ffca09"},
            new String[]{"frozen-repo", "partial-frozen-docs-f5ffca09"},
            new String[]{"demo-repo", "test_data_8_17_0"},
            new String[]{"demo-repo", "test_data_9_3_0"});
        for (String concurrency : List.of("1", "8")) {
            System.setProperty(ShardMaterializer.CONCURRENCY_PROPERTY, concurrency);
            for (String[] export : exports) {
                var sources = new RepositorySources(new RepositorySources.Source("primary",
                    new FileSystemBlobStore(FIXTURES.resolve(export[0]))));
                var exporter = new FullIndexExporter(sources, new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
                RunReport report = exporter.run(export[1], tmp.resolve(concurrency).resolve(export[1]).toString(), RunReportListener.NO_OP);
                assertEquals(0, report.quarantinedCount(), export[1]);
                assertEquals(report.totalShards(), report.committedCount(), export[1]);
            }
        }
        for (String[] export : exports) {
            List<String> sequential = rows(tmp.resolve("1").resolve(export[1]));
            assertFalse(sequential.isEmpty(), export[1]);
            assertEquals(sequential, rows(tmp.resolve("8").resolve(export[1])), export[1]);
        }
    }

    @Test
    void theOptionSetsTheDownloadConcurrencyForTheRun(@TempDir Path tmp) {
        assertEquals(1, dumpToDelta(FIXTURES.resolve("demo-repo").toString(), "--index", "no-such-index",
            "-o", tmp.resolve("out").toString(), "--download-concurrency", "3"));
        assertEquals("3", System.getProperty(ShardMaterializer.CONCURRENCY_PROPERTY));
        assertEquals(1, new CommandLine(new DumpCommand()).execute(FIXTURES.resolve("demo-repo").toString(),
            "--index", "no-such-index", "-o", tmp.resolve("out.jsonl").toString(), "--download-concurrency", "5"));
        assertEquals("5", System.getProperty(ShardMaterializer.CONCURRENCY_PROPERTY));
    }

    @Test
    void rejectsConcurrencyOutsideOneToThirtyTwo(@TempDir Path tmp) {
        for (String invalid : List.of("0", "33", "-1", "many")) {
            assertEquals(2, dumpToDelta(FIXTURES.resolve("demo-repo").toString(), "--index", "test_data_9_3_0",
                "-o", tmp.resolve("out").toString(), "--download-concurrency", invalid), invalid);
            assertEquals(2, new CommandLine(new DumpCommand()).execute(FIXTURES.resolve("demo-repo").toString(),
                "--index", "test_data_9_3_0", "-o", tmp.resolve("out.jsonl").toString(), "--download-concurrency", invalid), invalid);
        }
        assertEquals(previousProperty, System.getProperty(ShardMaterializer.CONCURRENCY_PROPERTY));
        assertFalse(Files.exists(tmp.resolve("out")));
        assertFalse(Files.exists(tmp.resolve("out.jsonl")));
    }

    private static int dumpToDelta(String... args) {
        return new CommandLine(new DumpToDeltaCommand()).execute(args);
    }

    /** Every column of every row, sorted, so tables can be compared regardless of file layout. */
    static List<String> rows(Path table) throws Exception {
        var snapshot = DeltaLog.forTable(new Configuration(), table.toString()).snapshot();
        StructField[] fields = snapshot.getMetadata().getSchema().getFields();
        var rows = new ArrayList<String>();
        try (var iterator = snapshot.open()) {
            while (iterator.hasNext()) {
                var row = iterator.next();
                var line = new StringBuilder();
                for (StructField field : fields) {
                    line.append(field.getName()).append('=');
                    if (row.isNullAt(field.getName())) line.append("null");
                    else if (field.getDataType() instanceof StringType) line.append(row.getString(field.getName()));
                    else if (field.getDataType() instanceof LongType) line.append(row.getLong(field.getName()));
                    else fail("unexpected column type " + field.getDataType().getTypeName() + " of " + field.getName());
                    line.append('\u0001');
                }
                rows.add(line.toString());
            }
        }
        rows.sort(null);
        return rows;
    }
}
