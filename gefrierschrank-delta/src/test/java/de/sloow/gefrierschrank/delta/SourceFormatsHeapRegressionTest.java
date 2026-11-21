package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A SMILE document of shared-string back-references, about 1 MB stored and 63 MB as JSON, exported
 * at four shard threads in a child JVM with a small heap: the conversion runs before the memory
 * gate admits the document, at its converted length, and the shard commits without
 * OutOfMemoryError. The child prints its peak heap, to compare with that converted length.
 */
class SourceFormatsHeapRegressionTest {

    static final int COPIES = 1_000_000;
    static final String VALUE = "v".repeat(59) + "!";
    static final int SMALL_DOCUMENTS = 39;

    @Test
    void aLargeConvertedDocumentCommitsInASmallHeap(@TempDir Path target) throws Exception {
        String heap = System.getProperty("gefrierschrank.sourceFormats.heap", "768m");
        Path log = target.resolve("child.log");
        // This module's test classpath binds slf4j to reload4j without a configuration, i.e. at DEBUG,
        // where parquet renders every written value as text; the CLI ships a WARN configuration.
        Path logging = Files.writeString(target.resolve("log4j.properties"), "log4j.rootLogger=WARN\n");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process child = new ProcessBuilder(java, "-Xmx" + heap, "-XX:+ExitOnOutOfMemoryError",
            "-Dlog4j.configuration=" + logging.toUri(), "-cp", System.getProperty("java.class.path"), ChildExport.class.getName(), target.toString())
            .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        assertTrue(child.waitFor(10, TimeUnit.MINUTES), "child export timed out");
        String output = Files.readString(log);
        assertEquals(0, child.exitValue(), () -> "child export failed at -Xmx" + heap + ":\n" + output);
        assertTrue(output.contains("outcome=COMMITTED rows=" + (SMALL_DOCUMENTS + 1)), output);
        int files = Integer.parseInt(output.lines().filter(line -> line.startsWith("files=")).findFirst()
            .orElseThrow().substring("files=".length()));
        assertTrue(files >= 4, "four ranges, each with at least one file: " + output);
        assertTrue(!output.contains("read again as one range"), output);
        assertTrue(output.contains("note: shard 0 of heap_smile: 1 SMILE, 0 CBOR, 0 YAML documents converted to JSON"), output);
        // Peak heap of the child, printed for comparison with the converted length.
        output.lines().filter(line -> line.startsWith("peakHeapBytes=")).forEach(System.out::println);
    }

    /** Child JVM entry point: writes the shard, exports it at four threads, prints outcome and peak heap. */
    public static final class ChildExport {
        public static void main(String[] args) throws Exception {
            Path work = Path.of(args[0]);
            Path shardDir = work.resolve("shard");
            byte[] large = largeSmile();
            long converted = COPIES * (VALUE.length() + 3L) + 7;
            try (var directory = new NIOFSDirectory(shardDir);
                 var writer = new IndexWriter(directory, new IndexWriterConfig().setUseCompoundFile(false))) {
                for (int n = 0; n <= SMALL_DOCUMENTS; n++) {
                    var doc = new Document();
                    doc.add(new StoredField("_id", "doc-" + n));
                    doc.add(new NumericDocValuesField("_primary_term", 1));
                    byte[] source = n == SMALL_DOCUMENTS / 2 ? large
                        : ("{\"n\":" + n + "}").getBytes(StandardCharsets.UTF_8);
                    doc.add(new StoredField("_source", source));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
            }
            large = null;
            System.setProperty(ShardRanges.MIN_RANGE_LIVE_DOCS_PROPERTY, "1");
            var conf = new Configuration();
            conf.set(ShardRanges.SHARD_THREADS_KEY, "4");
            var context = new ArchiveContext("heap_smile", "rid", "suuid", "snap", "heap_smile", "suuid");
            System.gc();
            for (var pool : ManagementFactory.getMemoryPoolMXBeans()) pool.resetPeakUsage();
            var report = ShardRangeExportTest.pipeline(work.resolve("table").toString(), conf,
                    (s, d) -> new Lucene10ShardReader(shardDir))
                .runSnapshot(context, null, ShardRangeExportTest.catalog(), RunReportListener.NO_OP);
            long peak = 0;
            for (var pool : ManagementFactory.getMemoryPoolMXBeans()) {
                if (pool.getType() == MemoryType.HEAP) peak += pool.getPeakUsage().getUsed();
            }
            var shard = report.shards().get(0);
            System.out.println("outcome=" + shard.outcome() + " rows=" + shard.rowsWritten() + " error=" + shard.errorClass());
            var export = ShardRangeExportTest.read(report, conf, work.resolve("table"));
            System.out.println("files=" + export.files().size());
            System.out.println("peakHeapBytes=" + peak + " convertedBytes=" + converted
                + " ratio=" + String.format(java.util.Locale.ROOT, "%.2f", (double) peak / converted)
                + " maxHeapBytes=" + Runtime.getRuntime().maxMemory());
            System.out.flush();
        }

        private static byte[] largeSmile() throws java.io.IOException {
            var factory = SmileFactory.builder().enable(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES).build();
            var out = new ByteArrayOutputStream();
            try (var generator = factory.createGenerator(out)) {
                generator.writeStartObject();
                generator.writeArrayFieldStart("a");
                for (int i = 0; i < COPIES; i++) generator.writeString(VALUE);
                generator.writeEndArray();
                generator.writeEndObject();
            }
            return out.toByteArray();
        }
    }
}
