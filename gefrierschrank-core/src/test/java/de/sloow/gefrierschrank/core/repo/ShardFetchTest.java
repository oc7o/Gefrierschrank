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
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Rounds of a shard download: only the planned blobs are read, the rest exactly once on completion. */
class ShardFetchTest {

    static final String PREFIX = "indices/x/0/";

    /** A FileSystemBlobStore that counts reads per key, ranged reads included. */
    static final class CountingStore implements BlobStore {
        final FileSystemBlobStore delegate;
        final Map<String, AtomicInteger> reads = new ConcurrentHashMap<>();

        CountingStore(Path root) {
            this.delegate = new FileSystemBlobStore(root);
        }

        @Override public Optional<BlobMetadata> stat(String key) { return delegate.stat(key); }

        @Override public InputStream read(String key) throws IOException {
            reads.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            return delegate.read(key);
        }

        @Override public RangeRead openRange(String key, long offset, long length, String expectedVersion) throws IOException {
            reads.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            return delegate.openRange(key, offset, length, expectedVersion);
        }

        @Override public List<String> list(String prefix) { return delegate.list(prefix); }
        @Override public List<BlobMetadata> listWithSizes(String prefix) { return delegate.listWithSizes(prefix); }
        @Override public void put(String key, byte[] content) { delegate.put(key, content); }

        /** Distinct blob names read, without the shard prefix and the part suffix. */
        Set<String> blobsRead() {
            return reads.keySet().stream().map(k -> k.substring(PREFIX.length()).replaceAll("\\.part\\d+$", ""))
                .collect(Collectors.toSet());
        }

        int readsOf(String blob) {
            return reads.entrySet().stream().filter(e -> e.getKey().substring(PREFIX.length()).replaceAll("\\.part\\d+$", "").equals(blob))
                .mapToInt(e -> e.getValue().get()).sum();
        }
    }

    record Fixture(ShardSnapshot shard, Map<String, byte[]> contents, Path repo) {}

    static final List<String> NAMES = List.of("segments_5", "_0.si", "_0.fnm", "_0.fdt", "_0.fdx", "_0.fdm", "_0.tim",
        "_0.tip", "_0.doc", "_0.kdd", "_0.nvd", "_0_Lucene90_0.dvd", "_0_1.liv", "_1.si", "_1.cfs", "_1.cfe");

    static Fixture fixture(Path tmp) throws IOException {
        Path repo = Files.createDirectories(tmp.resolve("repo").resolve(PREFIX));
        var files = new ArrayList<RepoFileInfo>();
        var contents = new java.util.LinkedHashMap<String, byte[]>();
        int i = 0;
        for (String name : NAMES) {
            byte[] content = LuceneFooterVerifierTest.luceneFile(64 + 97 * i, i);
            String checksum = LuceneFooterVerifierTest.checksumOf(content);
            boolean embedded = name.startsWith("segments_") || name.endsWith(".si");
            if (embedded) {
                files.add(new RepoFileInfo("v__" + i, name, content.length, checksum, "10.3.2",
                    Base64.getEncoder().encodeToString(content)));
            } else {
                long partSize = i % 3 == 0 ? 100 : 0;
                var file = new RepoFileInfo("__" + i, name, content.length, checksum, "10.3.2", null, partSize);
                for (int part = 0; part < file.partCount(); part++) {
                    int start = (int) (part * (partSize == 0 ? content.length : partSize));
                    int end = (int) Math.min(content.length, start + (partSize == 0 ? content.length : partSize));
                    Files.write(repo.resolve(file.partName(part)), java.util.Arrays.copyOfRange(content, start, end));
                }
                files.add(file);
            }
            contents.put(name, content);
            i++;
        }
        long total = files.stream().mapToLong(RepoFileInfo::length).sum();
        return new Fixture(new ShardSnapshot("repo", PREFIX, "snap", 1, total, files), contents, tmp.resolve("repo"));
    }

    static RepoFileInfo info(Fixture fixture, String name) {
        return fixture.shard().files().stream().filter(f -> f.physicalName().equals(name)).findFirst().orElseThrow();
    }

    static String blob(Fixture fixture, String name) {
        return info(fixture, name).blobName();
    }

    /** The first blob holding the file's bytes: the blob itself, or its first part. */
    static Path firstBlob(Fixture fixture, String name) {
        return fixture.repo().resolve(PREFIX).resolve(info(fixture, name).partName(0));
    }

    static long downloadedBytes(Fixture fixture, Set<String> names) {
        return fixture.shard().files().stream().filter(f -> names.contains(f.physicalName()) && !f.blobName().startsWith("v__"))
            .mapToLong(RepoFileInfo::length).sum();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void roundsReadOnlyTheirFilesAndCompletionReadsTheRestOnce(int concurrency, @TempDir Path tmp) throws Exception {
        Fixture fixture = fixture(tmp);
        var store = new CountingStore(fixture.repo());
        Path dir = tmp.resolve("shard");
        ShardFetch fetch = new ShardMaterializer(store, concurrency).begin(fixture.shard(), dir);
        assertEquals(Set.of(), store.blobsRead(), "begin reads nothing");

        fetch.fetch(file -> SelectiveFetch.isCommitFile(file.physicalName()));
        Set<String> commitFiles = Set.of("segments_5", "_0.si", "_0.fnm", "_1.si", "_1.cfs", "_1.cfe");
        assertEquals(Set.of(blob(fixture, "_0.fnm"), blob(fixture, "_1.cfs"), blob(fixture, "_1.cfe")), store.blobsRead(),
            "embedded files trigger no read");
        assertEquals(commitFiles, listing(dir));
        assertEquals(downloadedBytes(fixture, commitFiles), fetch.fetchedBytes());
        assertFalse(fetch.complete());

        Set<String> planned = Set.of("_0.fdt", "_0.fdx", "_0.fdm", "_0_Lucene90_0.dvd", "_0_1.liv");
        fetch.fetch(file -> planned.contains(file.physicalName()) || commitFiles.contains(file.physicalName()));
        Set<String> skipped = Set.of("_0.tim", "_0.tip", "_0.doc", "_0.kdd", "_0.nvd");
        for (String name : skipped) assertEquals(0, store.readsOf(blob(fixture, name)), name);
        for (String name : planned) assertTrue(store.readsOf(blob(fixture, name)) >= 1, name);
        assertFalse(fetch.completed());

        var pool = Executors.newFixedThreadPool(4);
        try {
            var start = new CountDownLatch(1);
            var results = new ArrayList<Future<?>>();
            for (int caller = 0; caller < 4; caller++) {
                results.add(pool.submit(() -> {
                    start.await();
                    fetch.ensureComplete();
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> result : results) result.get();
        } finally {
            pool.shutdownNow();
        }
        assertTrue(fetch.complete());
        assertTrue(fetch.completed());
        for (var file : fixture.shard().files()) {
            if (file.blobName().startsWith("v__")) continue;
            assertEquals(file.partCount(), store.readsOf(file.blobName()), "each part read exactly once: " + file.physicalName());
        }
        for (var entry : fixture.contents().entrySet()) {
            assertArrayEquals(entry.getValue(), Files.readAllBytes(dir.resolve(entry.getKey())), entry.getKey());
        }
        assertEquals(downloadedBytes(fixture, Set.copyOf(NAMES)), fetch.fetchedBytes());
    }

    @Test
    void materializeReportsTheDownloadedBytesWithoutEmbeddedFiles(@TempDir Path tmp) throws Exception {
        Fixture fixture = fixture(tmp);
        long bytes = new ShardMaterializer(new FileSystemBlobStore(fixture.repo()), 4).materialize(fixture.shard(), tmp.resolve("all"));
        assertEquals(downloadedBytes(fixture, Set.copyOf(NAMES)), bytes);
        assertEquals(Set.copyOf(NAMES), listing(tmp.resolve("all")));
    }

    @Test
    void everyNameIsValidatedEvenWhenNoRoundWouldFetchIt(@TempDir Path tmp) throws Exception {
        Fixture fixture = fixture(tmp);
        var files = new ArrayList<>(fixture.shard().files());
        files.add(new RepoFileInfo("__evil", "../_9.tim", 10, null, "10.3.2", null));
        var shard = new ShardSnapshot("repo", PREFIX, "snap", 1, 0, files);
        var store = new CountingStore(fixture.repo());
        var failure = assertThrows(IOException.class, () -> new ShardMaterializer(store).begin(shard, tmp.resolve("shard")));
        assertEquals("invalid physical shard filename", failure.getMessage());
        assertEquals(Set.of(), store.blobsRead());
    }

    @Test
    void aCorruptSkippedBlobIsNeverReadAndACorruptPlannedOneFails(@TempDir Path tmp) throws Exception {
        Fixture fixture = fixture(tmp);
        Path tim = firstBlob(fixture, "_0.tim");
        byte[] corrupt = Files.readAllBytes(tim);
        corrupt[3] ^= 1;
        Files.write(tim, corrupt);
        var store = new CountingStore(fixture.repo());
        ShardFetch fetch = new ShardMaterializer(store, 4).begin(fixture.shard(), tmp.resolve("shard"));
        fetch.fetch(file -> !file.physicalName().equals("_0.tim"));
        assertEquals(0, store.readsOf(blob(fixture, "_0.tim")));
        assertThrows(IOException.class, fetch::ensureComplete);
        assertFalse(fetch.complete());

        ShardFetch planned = new ShardMaterializer(store, 1).begin(fixture.shard(), tmp.resolve("planned"));
        assertThrows(IOException.class, () -> planned.fetch(file -> file.physicalName().startsWith("_0.t")));
        assertTrue(planned.fetchedBytes() > 0, "a failed round reports the bytes that arrived");
    }

    @Test
    void aFailedRoundLeavesItsFilesUnfetched(@TempDir Path tmp) throws Exception {
        Fixture fixture = fixture(tmp);
        Path fdt = firstBlob(fixture, "_0.fdt");
        byte[] good = Files.readAllBytes(fdt);
        byte[] corrupt = good.clone();
        corrupt[1] ^= 1;
        Files.write(fdt, corrupt);
        ShardFetch fetch = new ShardMaterializer(new FileSystemBlobStore(fixture.repo()), 1).begin(fixture.shard(), tmp.resolve("shard"));
        assertThrows(IOException.class, () -> fetch.fetch(file -> file.physicalName().startsWith("_0.fd")));
        Files.write(fdt, good);
        fetch.ensureComplete();
        assertArrayEquals(fixture.contents().get("_0.fdt"), Files.readAllBytes(tmp.resolve("shard").resolve("_0.fdt")));
    }

    static Set<String> listing(Path dir) throws IOException {
        try (var files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString()).collect(Collectors.toSet());
        }
    }
}
