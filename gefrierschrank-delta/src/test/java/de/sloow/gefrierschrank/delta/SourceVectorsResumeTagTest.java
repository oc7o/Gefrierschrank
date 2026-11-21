package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Gefrierschrank 1.0.0 exported indices with excluded vectors without them. Such a table (shard
 * files without the vectors tag) must not be completed with rehydrated rows: the index fails after
 * the snapshot checks and the metadata step, before any shard is read or committed.
 */
class SourceVectorsResumeTagTest {

    private static final Path FIXTURES = VectorExportJvm.FIXTURES;
    private static final String INDEX = "test_es93_vectors_default";

    @Test
    void aTableWithUntaggedShardFilesFailsTheIndexBeforeAnyShardIsRead(@TempDir Path tmp) throws Exception {
        VectorExportJvm.export(FIXTURES.resolve("es93-repo"), tmp, List.of(INDEX), 1, 1, Map.of());
        Path table = tmp.resolve(INDEX);
        for (Map<String, String> tags : VectorExportJvm.fileTags(table)) {
            assertEquals(SourceVectorsPlan.FILE_TAG_VALUE, tags.get(SourceVectorsPlan.FILE_TAG));
        }
        stripVectorsTag(table);
        assertTrue(VectorExportJvm.fileTags(table).stream().noneMatch(tags -> tags.containsKey(SourceVectorsPlan.FILE_TAG)));
        long documentVersion = version(table);
        long metadataVersion = version(table.resolve("_export_metadata"));

        String stdout = runExpectingVectorlessTable(table);
        assertTrue(stdout.contains("Exporting " + INDEX), stdout);
        assertEquals(documentVersion, version(table), "no document-table commit");
        assertEquals(metadataVersion, version(table.resolve("_export_metadata")), "the snapshot's metadata row existed");

        // A 1.0.0 table without its metadata row gets that row, and nothing else, before the refusal.
        deleteRecursively(table.resolve("_export_metadata"));
        runExpectingVectorlessTable(table);
        assertEquals(documentVersion, version(table), "no document-table commit");
        assertEquals(1, rowCount(table.resolve("_export_metadata")));
    }

    @Test
    void theWriterTagsEveryStagedFileAndFindsUntaggedOnes(@TempDir Path tmp) throws Exception {
        var conf = new Configuration();
        Path table = tmp.resolve("t");
        var writer = new DeltaTableWriter(table.toString());
        writer.requireTaggedFiles(conf, SourceVectorsPlan.FILE_TAG, SourceVectorsPlan.FILE_TAG_VALUE);
        writer.withFileTags(Map.of(SourceVectorsPlan.FILE_TAG, SourceVectorsPlan.FILE_TAG_VALUE));
        writer.write(conf, List.of("{\"a\":1}").iterator(), null, "shard-0");
        assertEquals(List.of(Map.of(ShardKey.TAG, "shard-0", SourceVectorsPlan.FILE_TAG, SourceVectorsPlan.FILE_TAG_VALUE)),
            VectorExportJvm.fileTags(table));
        writer.requireTaggedFiles(conf, SourceVectorsPlan.FILE_TAG, SourceVectorsPlan.FILE_TAG_VALUE);

        new DeltaTableWriter(table.toString()).write(conf, List.of("{\"a\":2}").iterator(), null, "shard-1");
        var failure = assertThrows(VectorlessTableException.class, () -> new DeltaTableWriter(table.toString())
            .requireTaggedFiles(conf, SourceVectorsPlan.FILE_TAG, SourceVectorsPlan.FILE_TAG_VALUE));
        assertEquals("the table holds rows exported without their vectors by Gefrierschrank 1.0.0; "
            + "export the index to a new output root", failure.getMessage());
        assertTrue(failure instanceof de.sloow.gefrierschrank.core.FailureDiagnostics.SafeMessage);
        assertThrows(IllegalArgumentException.class, () -> writer.withFileTags(Map.of(ShardKey.TAG, "x")));
    }

    private static String runExpectingVectorlessTable(Path table) throws Exception {
        var store = new ExcludedSourceVectorsExportTest.RecordingBlobStore(new FileSystemBlobStore(FIXTURES.resolve("es93-repo")));
        var sources = new RepositorySources(new RepositorySources.Source("primary", store));
        var exporter = new FullIndexExporter(sources, new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
        PrintStream previous = System.out;
        var captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            assertThrows(VectorlessTableException.class, () -> exporter.run(INDEX, table.toString(), RunReportListener.NO_OP));
        } finally {
            System.setOut(previous);
        }
        assertTrue(store.touched.stream().noneMatch(key -> key.contains("/__")), "no shard data blob may be read: " + store.touched);
        return captured.toString(StandardCharsets.UTF_8);
    }

    /** What a 1.0.0 export left: the same AddFiles without the vectors tag. */
    private static void stripVectorsTag(Path table) throws Exception {
        try (Stream<Path> commits = Files.list(table.resolve("_delta_log"))) {
            for (Path commit : commits.filter(path -> path.toString().endsWith(".json")).toList()) {
                String log = Files.readString(commit);
                String stripped = log.replace(",\"" + SourceVectorsPlan.FILE_TAG + "\":\"" + SourceVectorsPlan.FILE_TAG_VALUE + "\"", "")
                    .replace("\"" + SourceVectorsPlan.FILE_TAG + "\":\"" + SourceVectorsPlan.FILE_TAG_VALUE + "\",", "");
                Files.writeString(commit, stripped);
            }
        }
        try (Stream<Path> crcs = Files.list(table.resolve("_delta_log"))) {
            for (Path crc : crcs.filter(path -> path.getFileName().toString().startsWith(".")).toList()) Files.delete(crc);
        }
        assertFalse(Files.readString(table.resolve("_delta_log/00000000000000000000.json")).contains(SourceVectorsPlan.FILE_TAG));
    }

    private static long version(Path table) {
        return io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).update().getVersion();
    }

    private static long rowCount(Path table) throws Exception {
        long rows = 0;
        try (var iterator = io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).snapshot().open()) {
            while (iterator.hasNext()) {
                iterator.next();
                rows++;
            }
        }
        return rows;
    }

    private static void deleteRecursively(Path path) throws Exception {
        try (Stream<Path> paths = Files.walk(path)) {
            for (Path each : paths.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(each);
        }
    }
}
