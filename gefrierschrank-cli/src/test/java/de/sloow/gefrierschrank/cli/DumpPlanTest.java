package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@code dump} runs dump-to-delta's per-index plan (mounted references, synthetic reconstruction,
 * one snapshot for metadata and segments), validates before touching the output, and writes it
 * atomically from raw bytes, or directly when the output is a stream such as /dev/stdout.
 */
class DumpPlanTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path CORE_FIXTURES = Es7InProcessExportTest.FIXTURES;
    private static final Path FROZEN_REPO = CORE_FIXTURES.resolve("frozen-repo");

    /** ES 9 logsdb stores no _source; dump used to fail at document 0 with a 0-byte output. */
    @Test
    void logsdbRowsAreReconstructedLikeDumpToDelta(@TempDir Path tmp) throws Exception {
        String index = "test_es93_logsdb_binary";
        Path output = tmp.resolve("logsdb.jsonl");
        assertEquals(0, DumpCommandTest.dump(new FileSystemBlobStore(DumpCommandTest.ES93_REPO), index, output));

        List<String> lines = Files.readAllLines(output, StandardCharsets.UTF_8);
        JsonNode expected = JSON.readTree(DumpCommandTest.ES93_REPO.resolveSibling("es93-logsdb-binary-expected.json").toFile());
        List<JsonNode> expectedSources = new ArrayList<>();
        expected.forEach(expectedSources::add);
        List<JsonNode> actual = new ArrayList<>();
        for (String line : lines) actual.add(JSON.readTree(line));
        assertEquals(sorted(expectedSources), sorted(actual), "Elasticsearch's own synthetic _source");

        Path table = tmp.resolve("table");
        assertEquals(0, Es7InProcessExportTest.dumpToDelta(DumpCommandTest.ES93_REPO, "--index", index, "-o", table.toString()));
        assertEquals(tableSources(table), lines, "the same text as dump-to-delta's _source column, in document order");
    }

    /** A frozen-tier mount has no Lucene files of its own; dump used to fail with 'no written_by version'. */
    @Test
    void mountNameDumpsTheMountedStateAndTheOriginalNameItsLatestSnapshot(@TempDir Path tmp) throws Exception {
        JsonNode expected = JSON.readTree(CORE_FIXTURES.resolve("frozen-expected.json").toFile());
        Path mounted = tmp.resolve("mounted.jsonl");
        var run = captureStdout(() -> DumpCommandTest.dump(new FileSystemBlobStore(FROZEN_REPO),
            expected.path("mounted_index").asText(), mounted));
        assertEquals(0, run.exit(), run.output());
        assertTrue(run.output().contains(" via "), "the resolution chain is shown: " + run.output());
        List<JsonNode> expectedSources = new ArrayList<>();
        for (JsonNode doc : expected.path("mounted_documents")) expectedSources.add(doc.path("_source"));
        List<JsonNode> actual = new ArrayList<>();
        for (String line : Files.readAllLines(mounted, StandardCharsets.UTF_8)) actual.add(JSON.readTree(line));
        assertEquals(sorted(expectedSources), sorted(actual));

        Path table = tmp.resolve("mounted-table");
        assertEquals(0, Es7InProcessExportTest.dumpToDelta(FROZEN_REPO, "--index", expected.path("mounted_index").asText(),
            "-o", table.toString()));
        assertEquals(tableSources(table), Files.readAllLines(mounted, StandardCharsets.UTF_8));

        Path original = tmp.resolve("original.jsonl");
        assertEquals(0, DumpCommandTest.dump(new FileSystemBlobStore(FROZEN_REPO), expected.path("index").asText(), original));
        assertEquals(expected.path("latest_document_count").asInt(), Files.readAllLines(original).size());
    }

    @Test
    void mistypedIndexLeavesTheExistingOutputUntouched(@TempDir Path tmp) throws Exception {
        Path output = Files.writeString(tmp.resolve("out.jsonl"), "previous export\n");
        var run = CommandRuns.captureStderr(() ->
            DumpCommandTest.dump(new FileSystemBlobStore(FROZEN_REPO), "frozen-docs-typo", output));
        assertEquals(1, run.exit());
        assertEquals(List.of("error: InvalidDumpRequestException: the index is not in the repository; the info command lists its indices"),
            errorLines(run.stderr()), run.stderr());
        assertFalse(run.stderr().contains("\tat "), "no stack trace: " + run.stderr());
        assertEquals("previous export\n", Files.readString(output));
        assertEquals(List.of(), partialFiles(tmp));
    }

    /** Shard 0 is written to the partial file before shard 1's download fails. */
    @Test
    void midRunFailureLeavesNeitherOutputNorPartialFile(@TempDir Path tmp) throws Exception {
        Path output = tmp.resolve("out.jsonl");
        Path previous = Files.writeString(tmp.resolve("previous.jsonl"), "previous export\n");
        var store = new DumpCommandTest.RecordingBlobStore(new FileSystemBlobStore(FROZEN_REPO),
            key -> key.matches("indices/[^/]+/1/__.*"), key -> {});
        var run = CommandRuns.captureStderr(() -> DumpCommandTest.dump(store, "frozen-docs-f5ffca09", output));
        assertEquals(1, run.exit());
        assertTrue(store.touched.stream().anyMatch(key -> key.matches("indices/[^/]+/0/__.*")), "shard 0 was read first");
        List<String> errors = errorLines(run.stderr());
        assertEquals(1, errors.size(), run.stderr());
        assertTrue(errors.get(0).startsWith("error: java.io.IOException at "), run.stderr());
        assertFalse(run.stderr().contains("injected"), "exception messages stay out of the error line: " + run.stderr());
        assertFalse(run.stderr().contains("\tat "), "no stack trace: " + run.stderr());
        assertFalse(Files.exists(output));
        assertEquals(List.of(), partialFiles(tmp));

        assertEquals(1, DumpCommandTest.dump(store, "frozen-docs-f5ffca09", previous));
        assertEquals("previous export\n", Files.readString(previous), "a failed run never truncates an existing output");
        assertEquals(List.of(), partialFiles(tmp));
    }

    /** Raw bytes, not a String round trip: malformed UTF-8 used to become U+FFFD. */
    @Test
    void storedRowsAreByteExactIncludingMalformedUtf8(@TempDir Path tmp) throws Exception {
        byte[][] sources = {
            "{\"a\":\"plain\"}".getBytes(StandardCharsets.UTF_8),
            {'{', '"', 'b', '"', ':', '"', (byte) 0xff, (byte) 0xc3, '(', '"', '}'},
            "{\"c\":\"ü€😀\",\"d\":1.00000000000000000001}".getBytes(StandardCharsets.UTF_8)};
        try (var dir = new NIOFSDirectory(tmp); var writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (byte[] source : sources) {
                var doc = new Document();
                doc.add(new StoredField("_source", new BytesRef(source)));
                writer.addDocument(doc);
            }
        }
        var out = new ByteArrayOutputStream();
        try (var reader = new Lucene10ShardReader(tmp)) {
            assertEquals(3, DumpCommand.writeLines(reader, out));
        }
        var expected = new ByteArrayOutputStream();
        for (byte[] source : sources) {
            expected.write(source);
            expected.write('\n');
        }
        assertArrayEquals(expected.toByteArray(), out.toByteArray());
    }

    @Test
    void shardDirectoriesLiveInTheRunDirectory(@TempDir Path tmp) throws Exception {
        Path runDirectory = Files.createDirectory(tmp.resolve("run"));
        List<String> seenDuringDownload = new CopyOnWriteArrayList<>();
        var store = new DumpCommandTest.RecordingBlobStore(new FileSystemBlobStore(FROZEN_REPO), key -> false, key -> {
            if (key.contains("/__")) {
                try (var entries = Files.list(runDirectory)) {
                    entries.forEach(entry -> seenDuringDownload.add(entry.getFileName().toString()));
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }
        });
        Path output = tmp.resolve("out.jsonl");
        Es7InProcessExportTest.withProperties(new String[]{"gefrierschrank.runDirectory", runDirectory.toString()}, () -> {
            assertEquals(0, DumpCommandTest.dump(store, "frozen-docs-f5ffca09", output));
            return null;
        });
        assertFalse(seenDuringDownload.isEmpty());
        assertTrue(seenDuringDownload.stream().allMatch(name -> name.startsWith("gefrierschrank-shard-")), seenDuringDownload.toString());
        try (var left = Files.list(runDirectory)) {
            assertEquals(List.of(), left.toList());
        }
        assertEquals(3, Files.readAllLines(output).size());
    }

    /** An Error is not an exception dump reports, but it must not leave a partial file or a shard directory behind. */
    @Test
    void jvmErrorMidRunLeavesNoPartialFileAndNoShardDirectory(@TempDir Path tmp) throws Exception {
        Path runDirectory = Files.createDirectory(tmp.resolve("run"));
        Path output = Files.writeString(tmp.resolve("out.jsonl"), "previous export\n");
        var store = new DumpCommandTest.RecordingBlobStore(new FileSystemBlobStore(FROZEN_REPO), key -> false, key -> {
            if (key.matches("indices/[^/]+/1/__.*")) throw new ServiceConfigurationError("injected");
        });
        Es7InProcessExportTest.withProperties(new String[]{"gefrierschrank.runDirectory", runDirectory.toString()}, () -> {
            assertThrows(ServiceConfigurationError.class, () -> DumpCommandTest.dump(store, "frozen-docs-f5ffca09", output));
            return null;
        });
        assertEquals("previous export\n", Files.readString(output));
        assertEquals(List.of(), partialFiles(tmp));
        try (var left = Files.list(runDirectory)) {
            assertEquals(List.of(), left.toList());
        }
    }

    /** With one shared partial file, the second run truncated the first's and the first then wrote into the finished output. */
    @Test
    void concurrentDumpsToOneOutputNeverShareAPartialFile(@TempDir Path tmp) throws Exception {
        Path expected = tmp.resolve("expected.jsonl");
        assertEquals(0, DumpCommandTest.dump(new FileSystemBlobStore(FROZEN_REPO), "frozen-docs-f5ffca09", expected));
        Path output = Files.createDirectory(tmp.resolve("out")).resolve("out.jsonl");
        var firstBlocked = new CountDownLatch(1);
        var secondDone = new CountDownLatch(1);
        var first = new DumpCommandTest.RecordingBlobStore(new FileSystemBlobStore(FROZEN_REPO), key -> false, key -> {
            if (key.matches("indices/[^/]+/1/__.*")) {
                firstBlocked.countDown();
                try {
                    assertTrue(secondDone.await(1, TimeUnit.MINUTES));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        var firstRun = CompletableFuture.supplyAsync(() -> DumpCommandTest.dump(first, "frozen-docs-f5ffca09", output));
        assertTrue(firstBlocked.await(1, TimeUnit.MINUTES));
        assertEquals(1, partialFiles(output.getParent()).size(), "the first run's partial file holds shard 0");
        assertEquals(0, DumpCommandTest.dump(new FileSystemBlobStore(FROZEN_REPO), "frozen-docs-f5ffca09", output));
        secondDone.countDown();
        assertEquals(0, firstRun.get(1, TimeUnit.MINUTES));
        assertArrayEquals(Files.readAllBytes(expected), Files.readAllBytes(output));
        assertEquals(List.of(), partialFiles(output.getParent()));
    }

    /** SIGTERM or Ctrl-C mid-run: the shutdown hook deletes the partial file, and the existing output stays. */
    @Test
    void sigtermMidRunLeavesNoPartialFile(@TempDir Path tmp, @TempDir Path childTmp) throws Exception {
        Path output = Files.writeString(tmp.resolve("out.jsonl"), "previous export\n");
        Process child = new ProcessBuilder(javaCommand(childTmp, BlockedDumpChild.class.getName(), FROZEN_REPO.toAbsolutePath().toString(),
            "frozen-docs-f5ffca09", output.toString())).redirectErrorStream(true).start();
        try (var stdout = new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            do {
                line = stdout.readLine();
            } while (line != null && !line.equals(BlockedDumpChild.BLOCKED));
            assertEquals(BlockedDumpChild.BLOCKED, line, "the child stopped before shard 1");
            assertEquals(1, partialFiles(tmp).size(), "the partial file exists while the dump runs");
            child.destroy();
            assertTrue(child.waitFor(1, TimeUnit.MINUTES));
        } finally {
            child.destroyForcibly();
        }
        assertEquals(List.of(), partialFiles(tmp));
        assertEquals("previous export\n", Files.readString(output));
        try (var left = Files.list(childTmp)) {
            assertTrue(left.anyMatch(p -> p.getFileName().toString().startsWith("gefrierschrank-shard-")),
                "the stopped child's shard directory is in its own temp directory, which goes with the test");
        }
    }

    /** Dumps a two-shard index whose shard 1 download never finishes; the parent kills it. */
    public static final class BlockedDumpChild {
        static final String BLOCKED = "blocked in shard 1";

        public static void main(String[] args) {
            var store = new DumpCommandTest.RecordingBlobStore(new FileSystemBlobStore(Path.of(args[0])), key -> false, key -> {
                if (key.matches("indices/[^/]+/1/__.*")) {
                    System.out.println(BLOCKED);
                    System.out.flush();
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            System.exit(DumpCommandTest.dump(store, args[1], Path.of(args[2])));
        }
    }

    /**
     * A named pipe (or a process substitution) cannot be renamed over: dump writes into it, as
     * 1.0.0 did, and moves its status line to stderr. /dev/null works the same way.
     */
    @Test
    void namedPipeAndDevNullAreWrittenDirectly(@TempDir Path tmp) throws Exception {
        Path expected = tmp.resolve("expected.jsonl");
        assertEquals(0, DumpCommandTest.dump(new FileSystemBlobStore(FROZEN_REPO), "frozen-docs-f5ffca09", expected));
        Path fifo = tmp.resolve("pipe");
        assumeTrue(new ProcessBuilder("mkfifo", fifo.toString()).start().waitFor() == 0, "mkfifo is available");
        var received = new CompletableFuture<byte[]>();
        Thread reader = new Thread(() -> {
            try (var in = Files.newInputStream(fifo)) {
                received.complete(in.readAllBytes());
            } catch (java.io.IOException e) {
                received.completeExceptionally(e);
            }
        });
        reader.setDaemon(true);
        reader.start();
        var stdout = new java.util.concurrent.atomic.AtomicReference<String>();
        var run = CommandRuns.captureStderr(() -> {
            var dumped = captureStdout(() -> DumpCommandTest.dump(new FileSystemBlobStore(FROZEN_REPO), "frozen-docs-f5ffca09", fifo));
            stdout.set(dumped.output());
            return dumped.exit();
        });
        assertEquals(0, run.exit(), run.stderr());
        assertArrayEquals(Files.readAllBytes(expected), received.get(1, TimeUnit.MINUTES));
        assertEquals("", stdout.get());
        assertTrue(run.stderr().contains("Dumped 3 documents to " + fifo), run.stderr());
        assertTrue(Files.exists(fifo) && !Files.isRegularFile(fifo), "the pipe is still a pipe");
        assertEquals(List.of(), partialFiles(tmp));

        assertEquals(0, DumpCommandTest.dump(new FileSystemBlobStore(FROZEN_REPO), "frozen-docs-f5ffca09", Path.of("/dev/null")));
        assertFalse(Files.isRegularFile(Path.of("/dev/null")));
    }

    /**
     * {@code -o /dev/stdout} into a pipe, and redirected to a regular file, which macOS then
     * reports as a regular file whose directory ({@code /dev/fd}) cannot take a partial file.
     */
    @Test
    void devStdoutIsWrittenDirectlyIntoAPipeAndARedirectedFile(@TempDir Path tmp, @TempDir Path childTmp) throws Exception {
        Path expected = tmp.resolve("expected.jsonl");
        assertEquals(0, DumpCommandTest.dump(new FileSystemBlobStore(FROZEN_REPO), "frozen-docs-f5ffca09", expected));
        List<String> command = javaCommand(childTmp, Main.class.getName(), "dump", FROZEN_REPO.toAbsolutePath().toString(),
            "--index", "frozen-docs-f5ffca09", "-o", "/dev/stdout");
        Path stderr = tmp.resolve("stderr.txt");

        Path redirected = tmp.resolve("redirected.jsonl");
        Process toFile = new ProcessBuilder(command).redirectOutput(redirected.toFile()).redirectError(stderr.toFile()).start();
        assertTrue(toFile.waitFor(3, TimeUnit.MINUTES));
        assertEquals(0, toFile.exitValue(), Files.readString(stderr));
        assertArrayEquals(Files.readAllBytes(expected), Files.readAllBytes(redirected));
        assertTrue(Files.readString(stderr).contains("Dumped 3 documents to /dev/stdout"), Files.readString(stderr));

        Process toPipe = new ProcessBuilder(command).redirectError(stderr.toFile()).start();
        byte[] streamed = toPipe.getInputStream().readAllBytes();
        assertTrue(toPipe.waitFor(3, TimeUnit.MINUTES));
        assertEquals(0, toPipe.exitValue(), Files.readString(stderr));
        assertArrayEquals(Files.readAllBytes(expected), streamed);
        assertEquals(List.of(), partialFiles(tmp));
    }

    @Test
    void descriptorPathsAreRecognizedWhateverTheyPointAt() {
        for (String path : List.of("/dev/stdout", "/dev/stderr", "/dev/fd/1", "/dev/fd/63", "/dev/./stdout",
                "/proc/self/fd/1", "/proc/4711/fd/9")) {
            assertTrue(DumpCommand.Destination.namesFileDescriptor(Path.of(path)), path);
        }
        for (String path : List.of("/dev/shm/out.jsonl", "/dev/stdout.jsonl", "/dev/fdx/1", "/proc/self/status",
                "/proc/self/fd", "/tmp/fd/1", "out.jsonl")) {
            assertFalse(DumpCommand.Destination.namesFileDescriptor(Path.of(path)), path);
        }
    }

    /** Before, the rename replaced the link with a regular file and left the target's old content. */
    @Test
    void symlinkedOutputReplacesItsTargetAndKeepsTheLinkAndThePermissions(@TempDir Path tmp) throws Exception {
        Path target = Files.writeString(Files.createDirectory(tmp.resolve("real")).resolve("target.jsonl"), "previous export\n");
        Files.setPosixFilePermissions(target, PosixFilePermissions.fromString("rw-------"));
        Path link = Files.createSymbolicLink(tmp.resolve("link.jsonl"), target);
        assertEquals(0, DumpCommandTest.dump(new FileSystemBlobStore(FROZEN_REPO), "frozen-docs-f5ffca09", link));
        assertTrue(Files.isSymbolicLink(link));
        assertEquals(3, Files.readAllLines(target).size());
        assertEquals(PosixFilePermissions.fromString("rw-------"), Files.getPosixFilePermissions(target),
            "the replaced output keeps its permission bits");
        assertEquals(List.of(), partialFiles(tmp));
        assertEquals(List.of(), partialFiles(target.getParent()));
    }

    @Test
    void outputsThatCannotBeReplacedAreRefusedBeforeTheRepositoryIsRead(@TempDir Path tmp) throws Exception {
        Path readOnly = Files.writeString(tmp.resolve("read-only.jsonl"), "previous export\n");
        Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("r--r--r--"));
        Path lockedDirectory = Files.createDirectory(tmp.resolve("locked"));
        Files.setPosixFilePermissions(lockedDirectory, PosixFilePermissions.fromString("r-xr-xr-x"));
        Path dangling = Files.createSymbolicLink(tmp.resolve("dangling.jsonl"), tmp.resolve("missing-target.jsonl"));
        try {
            assumeFalse(Files.isWritable(readOnly), "permission bits are enforced (not root)");
            var cases = new LinkedHashMap<Path, String>();
            cases.put(readOnly, "the output file is not writable");
            cases.put(tmp.resolve("missing").resolve("out.jsonl"), "the output's directory does not exist");
            cases.put(lockedDirectory.resolve("out.jsonl"),
                "the output's directory is not writable; dump writes a partial file beside the output");
            cases.put(dangling, "the output is a symbolic link to a missing file");
            for (var entry : cases.entrySet()) {
                var store = new DumpCommandTest.RecordingBlobStore(new FileSystemBlobStore(FROZEN_REPO));
                var run = CommandRuns.captureStderr(() -> DumpCommandTest.dump(store, "frozen-docs-f5ffca09", entry.getKey()));
                assertEquals(1, run.exit(), entry.getKey().toString());
                assertEquals(List.of("error: InvalidDumpRequestException: " + entry.getValue()), errorLines(run.stderr()), run.stderr());
                assertEquals(List.of(), store.touched, "refused before the repository is read");
            }
            assertEquals("previous export\n", Files.readString(readOnly));
            assertFalse(Files.exists(tmp.resolve("missing-target.jsonl")));
            assertTrue(Files.isSymbolicLink(dangling));
        } finally {
            Files.setPosixFilePermissions(lockedDirectory, PosixFilePermissions.fromString("rwx------"));
            Files.setPosixFilePermissions(readOnly, PosixFilePermissions.fromString("rw-------"));
        }
    }

    static List<String> partialFiles(Path directory) throws java.io.IOException {
        try (var files = Files.list(directory)) {
            return files.map(file -> file.getFileName().toString()).filter(name -> name.endsWith(".partial")).toList();
        }
    }

    /**
     * A child JVM with its own java.io.tmpdir: a killed dump leaves its shard directory there, not
     * in the shared temp directory, where every later start reports it as an earlier version's.
     */
    private static List<String> javaCommand(Path javaTmp, String mainClass, String... args) {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Djava.io.tmpdir=" + javaTmp, "-cp", System.getProperty("java.class.path"), mainClass));
        command.addAll(List.of(args));
        return command;
    }

    private static List<String> errorLines(String stderr) {
        return stderr.lines().filter(line -> line.startsWith("error: ")).toList();
    }

    static List<String> sorted(List<JsonNode> nodes) {
        return nodes.stream().map(JsonNode::toString).sorted().toList();
    }

    /** The table's _source values in _shard, _doc order: what dump writes for the same snapshot. */
    static List<String> tableSources(Path table) throws Exception {
        record Row(long shard, long doc, String source) {}
        var rows = new ArrayList<Row>();
        try (var iterator = io.delta.standalone.DeltaLog.forTable(new Configuration(), table.toString()).snapshot().open()) {
            while (iterator.hasNext()) {
                var row = iterator.next();
                rows.add(new Row(row.getLong("_shard"), row.getLong("_doc"), row.getString("_source")));
            }
        }
        rows.sort(Comparator.comparingLong(Row::shard).thenComparingLong(Row::doc));
        return rows.stream().map(Row::source).toList();
    }

    record Output(int exit, String output) {}

    static Output captureStdout(java.util.concurrent.Callable<Integer> body) throws Exception {
        var previous = System.out;
        var buffer = new ByteArrayOutputStream();
        System.setOut(new java.io.PrintStream(buffer, true, StandardCharsets.UTF_8));
        try {
            int exit = body.call();
            return new Output(exit, buffer.toString(StandardCharsets.UTF_8));
        } finally {
            System.setOut(previous);
        }
    }
}
