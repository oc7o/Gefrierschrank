package de.sloow.gefrierschrank.delta;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Writes 60 documents of 8 MiB each in a child JVM with a small heap. Before page and row-group
 * size checks started at the first row, all of them were buffered into a single page and the
 * writer ran out of memory. Takes about a minute, so it only runs with
 * {@code -Dgefrierschrank.heavyTests=true}.
 */
@Tag("heavy")
@EnabledIfSystemProperty(named = "gefrierschrank.heavyTests", matches = "true")
class LargeDocumentHeapRegressionTest {

    static final int DOCUMENTS = 60;
    static final int SOURCE_CHARS = 8 << 20;

    @Test
    void multiMegabyteDocumentsFitASmallHeap(@TempDir Path target) throws Exception {
        String heap = System.getProperty("gefrierschrank.heavyTests.heap", "512m");
        Path table = target.resolve("table");
        Path log = target.resolve("child.log");
        String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
        Process child = new ProcessBuilder(java, "-Xmx" + heap, "-XX:+ExitOnOutOfMemoryError",
            "-cp", System.getProperty("java.class.path"), ChildWriter.class.getName(), table.toString())
            .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        assertTrue(child.waitFor(10, TimeUnit.MINUTES), "child writer timed out");
        assertEquals(0, child.exitValue(), () -> "child writer failed at -Xmx" + heap + ":\n" + tail(log));

        var footers = ParquetFooters.read(new Configuration(), table.toString());
        assertEquals(DOCUMENTS, footers.stream().mapToLong(ParquetFooters.FileFooter::rows).sum());
        long firstRowGroup = footers.get(0).rowGroups().get(0).rows();
        assertTrue(firstRowGroup < 100, "first row group holds " + firstRowGroup + " rows");
    }

    private static String tail(Path log) {
        try {
            var lines = Files.readAllLines(log);
            return String.join("\n", lines.subList(Math.max(0, lines.size() - 30), lines.size()));
        } catch (Exception e) {
            return "(no child output)";
        }
    }

    /** Child JVM entry point: streams freshly generated archive rows into one shard commit. */
    public static final class ChildWriter {
        public static void main(String[] args) throws Exception {
            var context = new ArchiveContext("heavy", "rid", "suuid", "snap", "heavy", "suuid");
            var random = new java.util.Random(60);
            Iterator<String> rows = new Iterator<>() {
                int next;

                @Override public boolean hasNext() {
                    return next < DOCUMENTS;
                }

                @Override public String next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    byte[] raw = new byte[SOURCE_CHARS / 4 * 3];
                    random.nextBytes(raw);
                    String source = "{\"blob\":\"" + java.util.Base64.getEncoder().encodeToString(raw) + "\"}";
                    try {
                        return context.document(source, 0,
                            new de.sloow.gefrierschrank.core.lucene.DocumentMetadata("id-" + next, null, "stored", next++));
                    } catch (java.io.IOException e) {
                        throw new java.io.UncheckedIOException(e);
                    }
                }
            };
            long written = new DeltaTableWriter(args[0])
                .write(new Configuration(), rows, ArchiveContext.mapping(), context.key(0));
            System.out.println("wrote " + written + " rows");
        }
    }
}
