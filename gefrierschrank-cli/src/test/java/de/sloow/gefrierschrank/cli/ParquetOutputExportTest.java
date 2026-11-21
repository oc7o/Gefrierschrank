package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.delta.DeltaTableWriter;
import io.delta.standalone.DeltaLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exports the real Elasticsearch fixture through the dump-to-delta command with the codec chosen on
 * its command line and checks the Parquet it wrote.
 */
class ParquetOutputExportTest {

    static final Path FIXTURES = Path.of("../gefrierschrank-core/src/test/resources/fixtures");
    static final ObjectMapper JSON = new ObjectMapper();

    private String previousCodec;

    @BeforeEach
    void clearCodecProperty() {
        previousCodec = System.clearProperty(DeltaTableWriter.PARQUET_CODEC_KEY);
    }

    @AfterEach
    void restoreCodecProperty() {
        if (previousCodec == null) System.clearProperty(DeltaTableWriter.PARQUET_CODEC_KEY);
        else System.setProperty(DeltaTableWriter.PARQUET_CODEC_KEY, previousCodec);
    }

    @Test
    void exportWritesZstdByDefaultAndTheChosenCodecOtherwise(@TempDir Path tmp) throws Exception {
        var expected = JSON.readTree(FIXTURES.resolve("frozen-expected.json").toFile());
        String index = expected.path("mounted_index").asText();
        var expectedById = new HashMap<String, JsonNode>();
        for (var doc : expected.path("mounted_documents")) expectedById.put(doc.path("_id").asText(), doc.path("_source"));

        for (var codec : new String[]{null, "snappy", "uncompressed"}) {
            Path table = tmp.resolve(codec == null ? "default" : codec);
            var args = new java.util.ArrayList<>(java.util.List.of(FIXTURES.resolve("frozen-repo").toString(),
                "--index", index, "-o", table.toString()));
            if (codec != null) args.addAll(java.util.List.of("--parquet-codec", codec));
            Path reportFile = tmp.resolve("report-" + table.getFileName() + ".json");
            args.addAll(java.util.List.of("--report", reportFile.toString()));
            assertEquals(0, new CommandLine(new DumpToDeltaCommand()).execute(args.toArray(String[]::new)));
            var report = JSON.readTree(reportFile.toFile());
            assertEquals(0, report.path("quarantinedCount").asInt(), report.toString());
            assertEquals(expectedById.size(), report.path("rowsWrittenThisRun").asInt(), report.toString());

            var expectedCodec = codec == null ? CompressionCodecName.ZSTD : CompressionCodecName.valueOf(codec.toUpperCase());
            assertEquals(Set.of(expectedCodec), codecs(table));
            assertEquals(expectedById, sourcesById(table), "source read back from " + expectedCodec + " Parquet");
        }
    }

    /**
     * A host where zstd-jni cannot load its native library (noexec java.io.tmpdir, read-only image)
     * still exports with the default codec, uncompressed as in 1.0.0, while an explicit ZSTD choice
     * stops before exporting anything. A missing -DZstdNativePath file reproduces that load failure
     * deterministically in a child JVM.
     */
    @Test
    void withoutTheZstdNativeLibraryTheDefaultFallsBackAndAChosenZstdStopsAtStartup(@TempDir Path tmp) throws Exception {
        var expected = JSON.readTree(FIXTURES.resolve("frozen-expected.json").toFile());
        String index = expected.path("mounted_index").asText();
        var expectedById = new HashMap<String, JsonNode>();
        for (var doc : expected.path("mounted_documents")) expectedById.put(doc.path("_id").asText(), doc.path("_source"));
        Path workDir = Files.createDirectories(tmp.resolve("cwd"));

        Path table = tmp.resolve("default");
        var fallback = dumpWithoutZstdNative(tmp, workDir, "--index", index, "-o", table.toString());
        assertEquals(0, fallback.exit(), fallback.output());
        assertEquals(1, fallback.output().lines()
            .filter(line -> line.startsWith("warning: Parquet ZSTD compression unavailable (UnsatisfiedLinkError)")).count(),
            fallback.output());
        assertFalse(fallback.output().contains("Exception in thread"), fallback.output());
        assertEquals(Set.of(CompressionCodecName.UNCOMPRESSED), codecs(table));
        assertEquals(expectedById, sourcesById(table));

        Path chosen = tmp.resolve("chosen");
        var refused = dumpWithoutZstdNative(tmp, workDir, "--index", index, "-o", chosen.toString(), "--parquet-codec", "zstd");
        assertEquals(CommandLine.ExitCode.USAGE, refused.exit(), refused.output());
        assertTrue(refused.output().contains("Parquet codec zstd cannot run in this JVM (UnsatisfiedLinkError)"), refused.output());
        assertFalse(refused.output().contains("Exception in thread"), refused.output());
        assertFalse(Files.exists(chosen), "nothing is written");
    }

    record ChildRun(int exit, String output) {}

    private static ChildRun dumpWithoutZstdNative(Path tmp, Path workDir, String... options) throws Exception {
        var command = new java.util.ArrayList<>(java.util.List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-DZstdNativePath=" + tmp.resolve("missing-libzstd-jni.so"),
            "-cp", System.getProperty("java.class.path"),
            Main.class.getName(), "dump-to-delta", FIXTURES.resolve("frozen-repo").toAbsolutePath().toString()));
        command.addAll(java.util.List.of(options));
        Path output = Files.createTempFile(tmp, "child", ".log");
        Process process = new ProcessBuilder(command).directory(workDir.toFile())
            .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        assertTrue(process.waitFor(5, java.util.concurrent.TimeUnit.MINUTES), "export did not finish");
        return new ChildRun(process.exitValue(), Files.readString(output));
    }

    private static Set<CompressionCodecName> codecs(Path table) throws Exception {
        var conf = new Configuration();
        Set<CompressionCodecName> codecs = new TreeSet<>();
        for (var file : DeltaLog.forTable(conf, table.toString()).update().getAllFiles()) {
            var input = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(table + "/" + file.getPath()), conf);
            try (var reader = ParquetFileReader.open(input)) {
                for (var block : reader.getFooter().getBlocks())
                    for (var column : block.getColumns()) codecs.add(column.getCodec());
            }
        }
        return codecs;
    }

    private static Map<String, JsonNode> sourcesById(Path table) throws Exception {
        Map<String, JsonNode> actual = new HashMap<>();
        try (var rows = DeltaLog.forTable(new Configuration(), table.toString()).update().open()) {
            while (rows.hasNext()) {
                var row = rows.next();
                assertNull(actual.put(row.getString("_id"), JSON.readTree(row.getString("_source"))), "duplicate _id");
            }
        }
        return actual;
    }

    /** The shaded CLI jar copies these unchanged from zstd-jni; without them ZSTD cannot load. */
    @Test
    void zstdNativesForTheSupportedPlatformsAreOnTheClasspath() {
        for (String platform : new String[]{"linux/amd64", "linux/aarch64", "darwin/aarch64"}) {
            String os = platform.substring(0, platform.indexOf('/'));
            String library = "libzstd-jni-" + com.github.luben.zstd.util.ZstdVersion.VERSION + (os.equals("darwin") ? ".dylib" : ".so");
            assertNotNull(getClass().getClassLoader().getResource(platform + "/" + library), platform + "/" + library);
        }
    }
}
