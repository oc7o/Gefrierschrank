package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.delta.SourceVectorsPlan;
import de.sloow.gefrierschrank.syntheticsource.SourceVectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SurveyCommandTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    static final Path CORE_FIXTURES = Path.of("../gefrierschrank-core/src/test/resources/fixtures");
    static final Path SYNTHETIC_FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");

    /** Survey reads index.latest, index-N, meta-*.dat and snap-*.dat: never a data blob. */
    @Test
    void readsMetadataOnlyAndOneMetaBlobPerIndexWithoutSizes() throws Exception {
        var store = new CountingStore(new FileSystemBlobStore(SYNTHETIC_FIXTURES.resolve("es93-repo")));
        var withSizes = run(store, 16, "--csv");
        assertEquals(0, withSizes.exitCode(), withSizes.err());
        assertTrue(store.reads.keySet().stream().noneMatch(key -> key.contains("/__")), store.reads.toString());
        assertEquals(4, store.readsMatching("indices/[^/]+/\\d+/snap-.*\\.dat"), store.reads.toString());

        store.reads.clear();
        var withoutSizes = run(store, 16, "--csv", "--no-sizes");
        assertEquals(0, withoutSizes.exitCode(), withoutSizes.err());
        assertEquals(0, store.readsMatching("indices/[^/]+/\\d+/.*"), store.reads.toString());
        assertEquals(4, store.readsMatching("indices/[^/]+/meta-.*\\.dat"), store.reads.toString());
        assertEquals(verdicts(withSizes.out()), verdicts(withoutSizes.out()));
        for (String line : withoutSizes.out().lines().skip(1).toList()) assertEquals("", line.split(",", -1)[4], line);
    }

    /**
     * frozen-repo's mount records this repository's own repository_uuid. With the backing index
     * deleted from the catalog (ILM's delete_searchable_snapshot while an older snapshot still
     * holds the shell) the backing is gone, not elsewhere: survey says so, without a --repository
     * hint, and dump-to-delta indeed fails the index.
     */
    @Test
    void aMountWhoseBackingWasDeletedFromThisRepositoryIsNotExportable(@TempDir Path tmp) throws Exception {
        Path repo = frozenRepoWithoutBackingIndex(tmp.resolve("repo"), null);
        assertEquals(MOUNT_REPOSITORY_UUID, new RepositoryInspector(new FileSystemBlobStore(repo)).repositoryUuid());

        String mount = mountLine(run(new FileSystemBlobStore(repo), 4, "--csv"));

        assertTrue(mount.startsWith("partial-frozen-docs-f5ffca09,not_exportable,backing_missing,2,,"
            + "full-export-test-f5ffca09/-qDma32ZRT-aOibqhqfTMw/frozen-docs-f5ffca09,,"), mount);
        assertFalse(mount.contains("--repository"), mount);
        var export = execute(new DumpToDeltaCommand(), repo.toString(), "--index", "partial-frozen-docs-f5ffca09",
            "-o", tmp.resolve("table").toString());
        assertEquals(1, export.exitCode(), export.err());
        assertTrue(export.err().contains("UnresolvedSnapshotException"), export.err());
    }

    /** The mount records another repository_uuid: the backing lives there, so survey names the option. */
    @Test
    void aMountBackedByAnotherRepositoryNamesTheRepositoryOption(@TempDir Path tmp) throws Exception {
        Path repo = frozenRepoWithoutBackingIndex(tmp, "another-repository-uuid");

        String mount = mountLine(run(new FileSystemBlobStore(repo), 4, "--csv"));

        assertTrue(mount.startsWith("partial-frozen-docs-f5ffca09,mounted,other_repository,2,,"), mount);
        assertTrue(mount.contains("backing snapshot is in repository 'full-export-test-f5ffca09': survey it there, "
            + "export with dump-to-delta --repository full-export-test-f5ffca09=URI"), mount);
    }

    /** Without a repository_uuid (repositories older than 7.12) survey cannot tell elsewhere from deleted, and says so. */
    @Test
    void aMountWithoutRepositoryUuidsNamesBothPossibilities(@TempDir Path tmp) throws Exception {
        Path repo = frozenRepoWithoutBackingIndex(tmp, "_na_");

        String mount = mountLine(run(new FileSystemBlobStore(repo), 4, "--csv"));

        assertTrue(mount.startsWith("partial-frozen-docs-f5ffca09,mounted,other_repository,2,,"), mount);
        assertTrue(mount.contains("no repository_uuid tells whether 'full-export-test-f5ffca09' is another one or it was deleted"),
            mount);
        assertTrue(mount.contains("--repository full-export-test-f5ffca09=URI"), mount);
    }

    /** A shard whose snap-*.dat is unreadable leaves the index UNKNOWN and unsized, not sized by the shards read before it. */
    @Test
    void anIndexWithAnUnreadableShardSnapshotIsUnknownAndUnsized(@TempDir Path repo) throws Exception {
        copy(CORE_FIXTURES.resolve("frozen-repo"), repo);
        String indexId = new RepositoryInspector(new FileSystemBlobStore(repo)).index("frozen-docs-f5ffca09").repoUuid();
        try (var snaps = Files.newDirectoryStream(repo.resolve("indices").resolve(indexId).resolve("1"), "snap-*.dat")) {
            for (Path snap : snaps) Files.delete(snap);
        }

        var result = run(new FileSystemBlobStore(repo), 4, "--csv");

        assertEquals(0, result.exitCode(), result.err());
        String original = result.out().lines().filter(l -> l.startsWith("frozen-docs-f5ffca09,")).findFirst().orElseThrow();
        assertEquals("frozen-docs-f5ffca09,unknown,metadata_unreadable,2,,,,NoSuchFileException", original);
    }

    /** Only the first probe runs alone (it builds the isolated loader); a failing first probe releases the others. */
    @Test
    void theFirstVectorProbeRunsAlone() throws Exception {
        var inFlight = new AtomicInteger();
        var calls = new AtomicInteger();
        var overlappedFirst = new AtomicBoolean();
        SourceVectorsPlan.Prober probe = SurveyCommand.firstCallAlone((index, metadata) -> {
            inFlight.incrementAndGet();
            try {
                if (calls.incrementAndGet() == 1) {
                    Thread.sleep(200);
                    if (inFlight.get() != 1) overlappedFirst.set(true);
                    throw new IOException("first probe fails");
                }
                return new SourceVectors.Result(SourceVectors.Verdict.NONE, Set.of());
            } catch (InterruptedException e) {
                throw new IOException(e);
            } finally {
                inFlight.decrementAndGet();
            }
        });
        var start = new CyclicBarrier(8);
        var failures = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            threads.add(Thread.ofPlatform().start(() -> {
                try {
                    start.await();
                    probe.probe("idx", null);
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }));
        }
        for (Thread thread : threads) thread.join(10_000);

        assertEquals(8, calls.get());
        assertEquals(1, failures.get());
        assertFalse(overlappedFirst.get(), "another probe ran while the first was building the loader");
    }

    /** The repository_uuid frozen-repo's mount records: frozen-repo's own. */
    private static final String MOUNT_REPOSITORY_UUID = "_ndEw2aeR4izVvDjY0QNqw";

    /** A copy of frozen-repo whose catalog lost the mount's backing index; {@code repositoryUuid} replaces index-N's uuid when set. */
    private static Path frozenRepoWithoutBackingIndex(Path repo, String repositoryUuid) throws IOException {
        copy(CORE_FIXTURES.resolve("frozen-repo"), repo);
        Path indexN = repo.resolve("index-3");
        var catalog = (ObjectNode) JSON.readTree(indexN.toFile());
        ((ObjectNode) catalog.path("indices")).remove("frozen-docs-f5ffca09");
        if (repositoryUuid != null) catalog.put("uuid", repositoryUuid);
        JSON.writeValue(indexN.toFile(), catalog);
        return repo;
    }

    private static String mountLine(Result result) {
        assertEquals(0, result.exitCode(), result.err());
        return result.out().lines().filter(l -> l.startsWith("partial-frozen-docs-f5ffca09,")).findFirst().orElseThrow();
    }

    @Test
    void csvQuotesFieldsWithCommas() throws Exception {
        var result = run(new FileSystemBlobStore(SYNTHETIC_FIXTURES.resolve("es93-repo")), 1, "--csv");
        assertEquals("index,verdict,reason,shards,bytes,backing,group,note", result.out().lines().findFirst().orElseThrow());
        for (String line : result.out().lines().skip(1).toList()) {
            assertEquals(8, line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1).length, line);
        }
    }

    @Test
    void anUnreadableRepositoryIsAnErrorWithTheExceptionClass(@TempDir Path empty) {
        var result = run(new FileSystemBlobStore(empty), 16);
        assertEquals(1, result.exitCode());
        assertTrue(result.err().contains("error: NoSuchFileException: "), result.err());
    }

    record Result(int exitCode, String out, String err) {}

    /** Runs survey over {@code store} with {@code threads} fetch threads; the repository argument is ignored. */
    static Result run(BlobStore store, int threads, String... options) {
        var command = new SurveyCommand() {
            @Override
            BlobStore openStore() {
                return store;
            }
        };
        command.fetchThreads = threads;
        return capture(command, options);
    }

    /** Runs {@code command} with a placeholder repository argument followed by {@code options}. */
    static Result capture(Object command, String... options) {
        String[] args = new String[options.length + 1];
        args[0] = "unused-repository";
        System.arraycopy(options, 0, args, 1, options.length);
        return execute(command, args);
    }

    static Result execute(Object command, String... args) {
        var out = new ByteArrayOutputStream();
        var err = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
        System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
        try {
            int exit = new CommandLine(command).execute(args);
            return new Result(exit, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }

    /** index -> verdict(reason) from survey CSV. */
    static Map<String, String> verdicts(String csv) {
        Map<String, String> verdicts = new LinkedHashMap<>();
        for (String line : csv.lines().skip(1).toList()) {
            String[] fields = line.split(",", -1);
            verdicts.put(fields[0], fields[2].isEmpty() ? fields[1] : fields[1] + "(" + fields[2] + ")");
        }
        return verdicts;
    }

    static void copy(Path from, Path to) throws IOException {
        try (var files = Files.walk(from)) {
            for (Path file : files.toList()) {
                Path target = to.resolve(from.relativize(file).toString());
                if (Files.isDirectory(file)) Files.createDirectories(target);
                else Files.copy(file, target);
            }
        }
    }

    static final class CountingStore implements BlobStore {
        final Map<String, AtomicInteger> reads = new ConcurrentHashMap<>();
        private final BlobStore delegate;

        CountingStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        long readsMatching(String regex) {
            return reads.entrySet().stream().filter(e -> e.getKey().matches(regex)).mapToLong(e -> e.getValue().get()).sum();
        }

        @Override
        public Optional<BlobMetadata> stat(String key) {
            return delegate.stat(key);
        }

        @Override
        public InputStream read(String key) throws IOException {
            reads.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            return delegate.read(key);
        }

        @Override
        public List<String> list(String prefix) {
            return delegate.list(prefix);
        }

        @Override
        public List<BlobMetadata> listWithSizes(String prefix) {
            return delegate.listWithSizes(prefix);
        }

        @Override
        public void put(String key, byte[] content) {
            throw new UnsupportedOperationException("the source repository is read-only");
        }
    }
}
