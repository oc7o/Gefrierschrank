package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * One inspector shared by concurrent index workers: cold callers must not each download and
 * parse index-N (8 and 16 GETs before), and a generation that advances after the first load must
 * not change any answer.
 */
class CatalogConcurrencyTest {

    private static final Path DEMO_REPO = Path.of("src/test/resources/fixtures/demo-repo");
    private static final String INDEX = "test_data_8_17_0";
    private static final String SNAPSHOT = "h2mJ0DXuT4OA76OqN3LDZA";

    @ParameterizedTest
    @ValueSource(ints = {8, 16})
    void coldConcurrentCallersFetchIndexLatestAndIndexNOnce(int threads) throws Exception {
        List<String> serial = answers(new RepositoryInspector(new FileSystemBlobStore(DEMO_REPO)), threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int trial = 0; trial < 20; trial++) {
                var store = new CountingBlobStore(new FileSystemBlobStore(DEMO_REPO));
                var inspector = new RepositoryInspector(store);
                var start = new CyclicBarrier(threads);
                List<Future<String>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    int caller = t;
                    futures.add(pool.submit(() -> {
                        start.await();
                        return answer(inspector, caller);
                    }));
                }
                List<String> concurrent = new ArrayList<>();
                for (var future : futures) concurrent.add(future.get());

                assertEquals(serial, concurrent);
                assertEquals(1, store.readsOf("index.latest"), "trial " + trial);
                assertEquals(1, store.readsOf("index-2"), "trial " + trial);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void anIndexLatestAdvanceAfterTheFirstLoadChangesNoAnswer(@TempDir Path repo) throws Exception {
        copy(DEMO_REPO, repo);
        var store = new CountingBlobStore(new FileSystemBlobStore(repo));
        var inspector = new RepositoryInspector(store);
        List<String> before = answers(inspector, 9);

        // A newer generation in which an index is gone, as a concurrent delete would leave it.
        String advanced = Files.readString(repo.resolve("index-2"))
            .replace("\"test_data_7_17_9\"", "\"renamed_index\"");
        Files.writeString(repo.resolve("index-3"), advanced);
        Files.write(repo.resolve("index.latest"), new byte[] {0, 0, 0, 0, 0, 0, 0, 3});

        assertEquals(before, answers(inspector, 9));
        assertEquals(2, inspector.overview().latestGeneration());
        assertEquals(1, store.readsOf("index.latest"));
        assertEquals(0, store.readsOf("index-3"));
        assertEquals("[]", new RepositoryInspector(new FileSystemBlobStore(repo)).overview().indices().stream()
            .filter(i -> i.name().equals("test_data_7_17_9")).toList().toString(), "the fixture copy did advance");
    }

    @Test
    void sharedCollectionsAreUnmodifiable() throws IOException {
        var inspector = new RepositoryInspector(new FileSystemBlobStore(DEMO_REPO));
        RepositoryOverview overview = inspector.overview();
        IndexOverview index = inspector.index(INDEX);

        assertThrows(UnsupportedOperationException.class, () -> overview.snapshots().clear());
        assertThrows(UnsupportedOperationException.class, () -> overview.indices().remove(0));
        assertThrows(UnsupportedOperationException.class, () -> index.snapshotUuids().add("x"));
        assertThrows(UnsupportedOperationException.class, () -> index.shardGenerations().set(0, "x"));
        assertEquals(false, index.snapshotUuids().contains(null), "contains(null) answers like the old ArrayList");
    }

    /** A spread of lookups, one per caller, so concurrent first calls race through different paths. */
    private static String answer(RepositoryInspector inspector, int caller) throws IOException {
        return switch (caller % 9) {
            case 0 -> inspector.overview().indices().toString();
            case 1 -> inspector.index(INDEX).toString();
            case 2 -> inspector.latestSnapshotUuid(INDEX);
            case 3 -> inspector.requireSnapshot(INDEX, SNAPSHOT).toString();
            case 4 -> inspector.repositoryUuid();
            case 5 -> inspector.snapshotOfIndex(INDEX, "snap_8_17_0").toString();
            case 6 -> String.valueOf(inspector.containsSnapshot(INDEX, "Z_nnpp14S7SwTz5rTG53pA", SNAPSHOT));
            case 7 -> inspector.indexMetadata(INDEX).path("settings").toString();
            default -> inspector.shardSnapshot(INDEX, 0).toString();
        };
    }

    private static List<String> answers(RepositoryInspector inspector, int callers) throws IOException {
        List<String> answers = new ArrayList<>();
        for (int caller = 0; caller < callers; caller++) answers.add(answer(inspector, caller));
        return answers;
    }

    private static void copy(Path from, Path to) throws IOException {
        try (var files = Files.walk(from)) {
            for (Path file : files.toList()) {
                Path target = to.resolve(from.relativize(file).toString());
                if (Files.isDirectory(file)) Files.createDirectories(target);
                else Files.copy(file, target);
            }
        }
    }

    /** Thread-safe read counts; index.latest is slowed down so cold callers pile up behind it. */
    private static final class CountingBlobStore implements BlobStore {
        private final BlobStore delegate;
        private final Map<String, AtomicInteger> reads = new ConcurrentHashMap<>();

        CountingBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        int readsOf(String key) {
            return reads.getOrDefault(key, new AtomicInteger()).get();
        }

        @Override
        public Optional<BlobMetadata> stat(String key) {
            return delegate.stat(key);
        }

        @Override
        public InputStream read(String key) throws IOException {
            reads.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            if (key.equals("index.latest")) {
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
            }
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
