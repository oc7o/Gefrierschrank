package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.delta.FullIndexExporter;
import de.sloow.gefrierschrank.delta.RunReport;
import de.sloow.gefrierschrank.delta.RunReportListener;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code dump} gets nested and source-disabled reconstruction through the same reader stack as
 * dump-to-delta: its JSONL lines are exactly the table's {@code _source} values, and the root
 * reader's close prints the content-free {@code _ignored} note.
 */
class SyntheticCoverageDumpTest {

    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final ObjectMapper JSON = new ObjectMapper();

    @ParameterizedTest
    @CsvSource({"synth-deletes-repo,test_deletes_nested,0", "synthcov-repo,cov_srcoff_native,1"})
    void dumpLinesEqualTheExportedRows(String repository, String index, int notedShards, @TempDir Path tmp) throws Exception {
        Path output = tmp.resolve("out.jsonl");
        PrintStream previous = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        int exit;
        try {
            exit = new picocli.CommandLine(new DumpCommand()).execute(FIXTURES.resolve(repository).toString(), "--index", index,
                "-o", output.toString());
        } finally {
            System.setErr(previous);
        }
        String stderr = captured.toString(StandardCharsets.UTF_8);
        assertEquals(0, exit, stderr);
        List<String> dumped = new ArrayList<>(Files.readAllLines(output, StandardCharsets.UTF_8));

        Path table = tmp.resolve("table");
        RunReport report = new FullIndexExporter(new RepositorySources(new RepositorySources.Source("primary",
            new FileSystemBlobStore(FIXTURES.resolve(repository)))), new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir))
            .run(index, table.toString(), RunReportListener.NO_OP);
        assertEquals(report.shards().size(), report.committedCount(), report.shards().toString());
        List<String> rows = new ArrayList<>();
        try (var it = io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).update().open()) {
            while (it.hasNext()) rows.add(it.next().getString("_source"));
        }
        dumped.sort(null);
        rows.sort(null);
        assertEquals(rows, dumped);

        List<String> notes = stderr.lines().filter(line -> line.contains("list fields in _ignored")).toList();
        assertEquals(notedShards, notes.size(), stderr);
        for (String note : notes) {
            assertTrue(note.matches("note: \\d+ document\\(s\\) of index " + index + " list fields in _ignored; .*"), note);
        }
        assertTrue(rows.stream().allMatch(row -> row.startsWith("{")) && JSON.readTree(rows.get(0)).isObject());
    }
}
