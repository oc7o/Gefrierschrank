package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/** Materializes shard Lucene files from the demo repository onto the local filesystem. */
class ShardMaterializerTest {

    static RepositoryInspector inspector;

    @BeforeAll
    static void setup() {
        inspector = new RepositoryInspector(
            new FileSystemBlobStore(Path.of("src/test/resources/fixtures/demo-repo")));
    }

    @Test
    void materializesEs8ShardWithCorrectFileSizes(@TempDir Path target) throws IOException {
        ShardSnapshot shard = inspector.shardSnapshot("test_data_8_17_0", 0);
        new ShardMaterializer(
            new FileSystemBlobStore(Path.of("src/test/resources/fixtures/demo-repo")))
            .materialize(shard, target);

        var sizes = Files.list(target).collect(Collectors.toMap(
            p -> p.getFileName().toString(),
            p -> {
                try { return Files.size(p); } catch (IOException e) { throw new RuntimeException(e); }
            }));

        // stored blobs
        assertEquals(192090L, sizes.get("_0.cfs"));
        assertEquals(697L, sizes.get("_0.cfe"));
        // embedded files from meta_hash
        assertEquals(353L, sizes.get("_0.si"));
        assertEquals(337L, sizes.get("segments_3"));
        assertEquals(4, sizes.size());
    }

    @Test
    void materializesEs9Shard(@TempDir Path target) throws IOException {
        ShardSnapshot shard = inspector.shardSnapshot("test_data_9_3_0", 0);
        new ShardMaterializer(
            new FileSystemBlobStore(Path.of("src/test/resources/fixtures/demo-repo")))
            .materialize(shard, target);

        assertTrue(Files.exists(target.resolve("_0.cfs")));
        assertTrue(Files.exists(target.resolve("segments_3")));
    }

    @Test
    void materializedSiStartsWithLuceneMagic(@TempDir Path target) throws IOException {
        ShardSnapshot shard = inspector.shardSnapshot("test_data_8_17_0", 0);
        new ShardMaterializer(
            new FileSystemBlobStore(Path.of("src/test/resources/fixtures/demo-repo")))
            .materialize(shard, target);

        byte[] si = Files.readAllBytes(target.resolve("_0.si"));
        int magic = ((si[0] & 0xFF) << 24) | ((si[1] & 0xFF) << 16) | ((si[2] & 0xFF) << 8) | (si[3] & 0xFF);
        assertEquals(0x3FD76C17, magic, "_0.si must be a valid Lucene file (not the raw meta_hash wrapper)");
    }
    @Test
    void reassemblesChunkedFilesAndIgnoresNonVirtualMetadataHashes(@TempDir Path tmp) throws Exception {
        Path repo = Files.createDirectory(tmp.resolve("repo"));
        Files.write(repo.resolve("__large.part0"), new byte[]{1, 2, 3});
        Files.write(repo.resolve("__large.part1"), new byte[]{4, 5, 6});
        Files.write(repo.resolve("__large.part2"), new byte[]{7});
        var file = new RepoFileInfo("__large", "_0.cfs", 7, null, "10.3.2", "AA==", 3);
        var shard = new ShardSnapshot("repo", "", "snap", 1, 7, java.util.List.of(file));
        Path output = tmp.resolve("out");
        new ShardMaterializer(new FileSystemBlobStore(repo)).materialize(shard, output);
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7}, Files.readAllBytes(output.resolve("_0.cfs")));
    }

    /**
     * The parent is a run root: once the exiting process deleted it, a shard thread that starts
     * late must fail rather than recreate it and download a whole shard nothing deletes any more.
     */
    @Test
    void createsItsTargetDirectoryButNoMissingParent(@TempDir Path tmp) throws Exception {
        ShardSnapshot shard = inspector.shardSnapshot("test_data_9_3_0", 0);
        Path deletedRoot = tmp.resolve("gefrierschrank-run-deleted");
        for (int concurrency : new int[]{1, 4}) {
            var materializer = new ShardMaterializer(new FileSystemBlobStore(DEMO_REPO), concurrency);
            assertThrows(java.nio.file.NoSuchFileException.class,
                () -> materializer.materialize(shard, deletedRoot.resolve("gefrierschrank-shard-1")));
            assertFalse(Files.exists(deletedRoot));
            materializer.materialize(shard, tmp.resolve("new-" + concurrency));
            assertTrue(Files.exists(tmp.resolve("new-" + concurrency).resolve("segments_3")));
        }
    }

    @Test
    void rejectsTruncatedFilesBeforeOpeningLucene(@TempDir Path tmp) throws Exception {
        Path repo = Files.createDirectory(tmp.resolve("repo"));
        Files.write(repo.resolve("__file"), new byte[]{1, 2});
        var file = new RepoFileInfo("__file", "_0.cfs", 3, null, "10.3.2", null);
        var shard = new ShardSnapshot("repo", "", "snap", 1, 3, java.util.List.of(file));
        assertThrows(IOException.class,
            () -> new ShardMaterializer(new FileSystemBlobStore(repo)).materialize(shard, tmp.resolve("out")));
    }

    @Test
    void rejectsSameSizeCorruption(@TempDir Path tmp) throws Exception {
        Path repo = Files.createDirectory(tmp.resolve("repo"));
        var bytes = new java.io.ByteArrayOutputStream();
        var output = new java.io.DataOutputStream(bytes);
        output.writeLong(42); output.writeInt(0xC02893E8); output.writeInt(0);
        var crc = new java.util.zip.CRC32(); crc.update(bytes.toByteArray());
        output.writeLong(crc.getValue());
        byte[] original = bytes.toByteArray();
        var file = new RepoFileInfo("__file", "_0.cfs", original.length, Long.toString(crc.getValue(), 36), "10.3.2", null);
        var shard = new ShardSnapshot("repo", "", "snap", 1, original.length, java.util.List.of(file));
        Files.write(repo.resolve("__file"), original);
        var materializer = new ShardMaterializer(new FileSystemBlobStore(repo));
        materializer.materialize(shard, tmp.resolve("valid"));
        original[2] ^= 1;
        Files.write(repo.resolve("__file"), original);
        assertThrows(IOException.class, () -> materializer.materialize(shard, tmp.resolve("corrupt")));
    }

    /** SHA-256 of every materialized file, captured with the 1.0.0 two-pass materializer. */
    static final Map<String, String> GOLDEN = Map.ofEntries(
        Map.entry("test_data_8_17_0/_0.cfe", "6b8f872a64e25ce52a24e00f31f9e129275c04895c46e6ac0a19592f6cda51d4"),
        Map.entry("test_data_8_17_0/_0.cfs", "392ade2289900a1347e62ae4bcd64dd1fbc7bc42a4aad2701a0f81aae254349d"),
        Map.entry("test_data_8_17_0/_0.si", "9cd23bfcc29ede6f9e00aa3a329a112d7813648d74963f028c288b5194949eee"),
        Map.entry("test_data_8_17_0/segments_3", "e57edd1996f0183f3f9386222c79b8b3d61a6ccb012601b0750a9a4f328b0342"),
        Map.entry("test_data_9_3_0/_0.cfe", "0731cf3aa3f8c50ffbefa4f9f1e986a6e522c048a766c1622040bc39c92accc0"),
        Map.entry("test_data_9_3_0/_0.cfs", "3002222eab37e2b4ca36849dc185743e6df9c1d2ff1ad20929f4e6938f75c542"),
        Map.entry("test_data_9_3_0/_0.si", "d5d61ebb2f3c646e29b2eac9cbca0f495e4e0fe1f6f9000714cd4c766c23b638"),
        Map.entry("test_data_9_3_0/segments_3", "3a9eea79ca069fb915b1bc9d7fed0dff1e289ef25f170da6b3664d1d4e3184cb"),
        Map.entry("test_data_7_17_9/_0.cfe", "6663781f2de26994338c52317325874b8b46fd9f1b20ddfedbb266620012aa32"),
        Map.entry("test_data_7_17_9/_0.cfs", "be2410fe0ffaf534717ff08fd6f186c042772316026034bbfc94568c7bd86fe0"),
        Map.entry("test_data_7_17_9/_0.si", "cd3a2504bb73c164bce069e473c0c9a671724d08b9e544fb364bdbf8add93075"),
        Map.entry("test_data_7_17_9/segments_3", "65515481778a7981413b4d2951b220784a3859c7eacf1072ca4fce630b112bcb"));

    @ParameterizedTest
    @ValueSource(strings = {"test_data_8_17_0", "test_data_9_3_0", "test_data_7_17_9"})
    void demoShardsMaterializeByteIdenticalToTheTwoPassMaterializer(String index, @TempDir Path target) throws Exception {
        new ShardMaterializer(new FileSystemBlobStore(DEMO_REPO)).materialize(inspector.shardSnapshot(index, 0), target);
        assertEquals(golden(index), digests(index, target));
    }

    @Test
    void acceptsAFooterStraddlingTwoParts(@TempDir Path tmp) throws Exception {
        byte[] content = LuceneFooterVerifierTest.luceneFile(40, 11);
        var shard = partedShard(tmp, content, 36, LuceneFooterVerifierTest.checksumOf(content));
        new ShardMaterializer(new FileSystemBlobStore(tmp.resolve("repo"))).materialize(shard, tmp.resolve("out"));
        assertArrayEquals(content, Files.readAllBytes(tmp.resolve("out/_0.fdt")));
    }

    @Test
    void rejectsACorruptedFooterStraddlingTwoParts(@TempDir Path tmp) throws Exception {
        byte[] content = LuceneFooterVerifierTest.luceneFile(40, 12);
        String checksum = LuceneFooterVerifierTest.checksumOf(content);
        content[37] ^= 0x40;
        var shard = partedShard(tmp, content, 36, checksum);
        var failure = assertThrows(IOException.class, () ->
            new ShardMaterializer(new FileSystemBlobStore(tmp.resolve("repo"))).materialize(shard, tmp.resolve("out")));
        assertEquals(LuceneFooterVerifier.CHECKSUM_MISMATCH, failure.getMessage());
    }

    @Test
    void rejectsABlobLongerThanTheMetadataLength(@TempDir Path tmp) throws Exception {
        Path repo = Files.createDirectory(tmp.resolve("repo"));
        Files.write(repo.resolve("__file"), new byte[]{1, 2, 3, 4});
        var file = new RepoFileInfo("__file", "_0.cfs", 3, null, "10.3.2", null);
        var shard = new ShardSnapshot("repo", "", "snap", 1, 3, List.of(file));
        assertThrows(IOException.class,
            () -> new ShardMaterializer(new FileSystemBlobStore(repo)).materialize(shard, tmp.resolve("out")));
    }

    @Test
    void materializesAZeroLengthFile(@TempDir Path tmp) throws Exception {
        Path repo = Files.createDirectory(tmp.resolve("repo"));
        Files.write(repo.resolve("__empty"), new byte[0]);
        var file = new RepoFileInfo("__empty", "write.lock", 0, null, "10.3.2", null);
        var shard = new ShardSnapshot("repo", "", "snap", 1, 0, List.of(file));
        new ShardMaterializer(new FileSystemBlobStore(repo)).materialize(shard, tmp.resolve("out"));
        assertEquals(0, Files.size(tmp.resolve("out/write.lock")));
    }

    @Test
    void verifiesEmbeddedFilesBeforeWritingThem(@TempDir Path tmp) throws Exception {
        byte[] content = LuceneFooterVerifierTest.luceneFile(48, 13);
        String checksum = LuceneFooterVerifierTest.checksumOf(content);
        String encoded = Base64.getEncoder().encodeToString(content);
        var valid = new ShardSnapshot("repo", "", "snap", 1, 48,
            List.of(new RepoFileInfo("v__ok", "_0.si", 48, checksum, "10.3.2", encoded)));
        new ShardMaterializer(new FileSystemBlobStore(tmp)).materialize(valid, tmp.resolve("valid"));
        assertArrayEquals(content, Files.readAllBytes(tmp.resolve("valid/_0.si")));

        content[5] ^= 1;
        var corrupt = new ShardSnapshot("repo", "", "snap", 1, 48,
            List.of(new RepoFileInfo("v__bad", "_0.si", 48, checksum, "10.3.2", Base64.getEncoder().encodeToString(content))));
        assertThrows(IOException.class,
            () -> new ShardMaterializer(new FileSystemBlobStore(tmp)).materialize(corrupt, tmp.resolve("corrupt")));
        assertFalse(Files.exists(tmp.resolve("corrupt/_0.si")), "corrupt embedded content must never be written");
    }

    @ParameterizedTest
    @ValueSource(strings = {"test_data_8_17_0", "test_data_9_3_0", "test_data_7_17_9"})
    void sequentialAndParallelDownloadsProduceIdenticalDirectories(String index, @TempDir Path tmp) throws Exception {
        ShardSnapshot shard = inspector.shardSnapshot(index, 0);
        new ShardMaterializer(new FileSystemBlobStore(DEMO_REPO), 1).materialize(shard, tmp.resolve("one"));
        new ShardMaterializer(new FileSystemBlobStore(DEMO_REPO), 8).materialize(shard, tmp.resolve("eight"));
        assertEquals(golden(index), digests(index, tmp.resolve("one")));
        assertEquals(golden(index), digests(index, tmp.resolve("eight")));
    }

    @Test
    void parallelDownloadsCompletingOutOfOrderStillVerifyEveryFile(@TempDir Path tmp) throws Exception {
        var store = new FakeStore(8);
        var random = new java.util.SplittableRandom(21);
        var files = new java.util.ArrayList<RepoFileInfo>();
        var expected = new java.util.HashMap<String, byte[]>();
        for (int i = 0; i < 24; i++) {
            byte[] content = LuceneFooterVerifierTest.luceneFile(random.nextInt(16, 400_000), i);
            long partSize = i % 3 == 0 ? random.nextInt(9, 200_000) : 0;
            var file = new RepoFileInfo("__f" + i, "_" + i + ".fdt", content.length,
                i % 5 == 0 ? null : LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null, partSize);
            store.putParts("indices/x/0/", file, content);
            files.add(file);
            expected.put(file.physicalName(), content);
        }
        var shard = new ShardSnapshot("repo", "indices/x/0/", "snap", 1, 0, files);
        new ShardMaterializer(store, 8).materialize(shard, tmp.resolve("out"));
        for (var entry : expected.entrySet()) {
            assertArrayEquals(entry.getValue(), Files.readAllBytes(tmp.resolve("out").resolve(entry.getKey())), entry.getKey());
        }
        assertEquals(0, store.open.get());
        assertTrue(store.maxOpen.get() > 1 && store.maxOpen.get() <= 8, "concurrent streams: " + store.maxOpen.get());
    }

    @Test
    void oneFailingFileFailsTheShardOnlyAfterEveryTaskHasStopped(@TempDir Path tmp) throws Exception {
        var store = new FakeStore(20);
        var files = new java.util.ArrayList<RepoFileInfo>();
        for (int i = 0; i < 12; i++) {
            byte[] content = LuceneFooterVerifierTest.luceneFile(2_000_000, i);
            var file = new RepoFileInfo("__f" + i, "_" + i + ".fdt", content.length,
                LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null);
            store.putParts("", file, content);
            files.add(file);
        }
        store.failingKey = "__f5";
        store.stubbornMillis = 10;
        var shard = new ShardSnapshot("repo", "", "snap", 1, 0, files);
        Path out = tmp.resolve("out");
        long started = System.nanoTime();
        var failure = assertThrows(IOException.class, () -> new ShardMaterializer(store, 4).materialize(shard, out));
        long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
        assertEquals(0, store.activeCalls.get(), "a download task is still inside the store when materialize throws");
        assertNoDownloadTaskRunning();
        assertEquals("injected failure", failure.getMessage());
        assertEquals(0, store.open.get(), "every download stream is closed when materialize throws");
        int readsAtFailure = store.reads.get();
        assertTrue(readsAtFailure < files.size(), "queued files are never started after the failure");
        assertTrue(elapsedMillis < 5_000, "running downloads are cancelled, not awaited to completion: " + elapsedMillis);
        deleteRecursively(out);
        Thread.sleep(100);
        assertFalse(Files.exists(out), "no task writes into the directory after materialize threw");
        assertEquals(readsAtFailure, store.reads.get());
    }

    /**
     * A download that fails with an Error after another download's failure has cancelled the
     * shard, as an OutOfMemoryError while its stream is torn down would: materialize throws the
     * Error, so the run ends, instead of the first failure, which only quarantines the shard.
     */
    @Test
    void anErrorFromADownloadIsThrownEvenAfterAnotherDownloadFailedFirst(@TempDir Path tmp) throws Exception {
        var store = new FakeStore(0);
        var files = new java.util.ArrayList<RepoFileInfo>();
        for (int i = 0; i < 2; i++) {
            byte[] content = LuceneFooterVerifierTest.luceneFile(300_000, 500 + i);
            var file = new RepoFileInfo("__e" + i, "_" + i + ".fdt", content.length,
                LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null);
            store.putParts("", file, content);
            files.add(file);
        }
        var crash = new OutOfMemoryError("injected while the shard is cancelled");
        var secondReading = new java.util.concurrent.CountDownLatch(1);
        var firstFailed = new java.util.concurrent.CountDownLatch(1);
        BlobStore blobs = new BlobStore() {
            @Override
            public java.io.InputStream read(String key) throws IOException {
                if (key.equals("__e0")) {
                    awaitQuietly(secondReading);
                    firstFailed.countDown();
                    throw new IOException("injected failure");
                }
                secondReading.countDown();
                awaitQuietly(firstFailed);
                // Long enough for the caller to have taken the first failure and cancelled the shard.
                FakeStore.sleepThroughInterrupts(300);
                throw crash;
            }

            private static void awaitQuietly(java.util.concurrent.CountDownLatch latch) {
                try {
                    assertTrue(latch.await(30, java.util.concurrent.TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override public java.util.Optional<BlobMetadata> stat(String key) { return store.stat(key); }
            @Override public List<String> list(String prefix) { return store.list(prefix); }
            @Override public List<BlobMetadata> listWithSizes(String prefix) { return store.listWithSizes(prefix); }
            @Override public void put(String key, byte[] content) { store.put(key, content); }
        };
        var shard = new ShardSnapshot("repo", "", "snap", 1, 0, files);
        var thrown = assertThrows(OutOfMemoryError.class, () -> new ShardMaterializer(blobs, 2).materialize(shard, tmp.resolve("out")));
        assertSame(crash, thrown);
        assertNoDownloadTaskRunning();
    }

    @Test
    void concurrentMaterializeCallsOnDifferentDirectoriesAreIndependent(@TempDir Path tmp) throws Exception {
        var healthy = new FakeStore(2);
        var broken = new FakeStore(2);
        var files = new java.util.ArrayList<RepoFileInfo>();
        var contents = new java.util.ArrayList<byte[]>();
        for (int i = 0; i < 6; i++) {
            byte[] content = LuceneFooterVerifierTest.luceneFile(300_000, 100 + i);
            var file = new RepoFileInfo("__f" + i, "_" + i + ".fdt", content.length,
                LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null);
            healthy.putParts("", file, content);
            broken.putParts("", file, content);
            files.add(file);
            contents.add(content);
        }
        broken.failingKey = "__f0";
        var shard = new ShardSnapshot("repo", "", "snap", 1, 0, files);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(4);
        try {
            var results = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < 4; i++) {
                Path dir = tmp.resolve("shard-" + i);
                BlobStore store = i % 2 == 0 ? healthy : broken;
                results.add(pool.submit(() -> {
                    new ShardMaterializer(store, 3).materialize(shard, dir);
                    return null;
                }));
            }
            for (int i = 0; i < 4; i++) {
                if (i % 2 == 0) {
                    results.get(i).get();
                    for (int f = 0; f < files.size(); f++) {
                        assertArrayEquals(contents.get(f), Files.readAllBytes(tmp.resolve("shard-" + i).resolve("_" + f + ".fdt")));
                    }
                } else {
                    var failure = assertThrows(java.util.concurrent.ExecutionException.class, results.get(i)::get);
                    assertEquals("injected failure", failure.getCause().getMessage());
                }
            }
        } finally {
            pool.shutdown();
        }
    }

    @Test
    void concurrencyComesFromTheSystemPropertyByDefault(@TempDir Path tmp) throws Exception {
        var store = new FakeStore(10);
        var files = new java.util.ArrayList<RepoFileInfo>();
        for (int i = 0; i < 8; i++) {
            byte[] content = LuceneFooterVerifierTest.luceneFile(400_000, 200 + i);
            var file = new RepoFileInfo("__f" + i, "_" + i + ".fdt", content.length, null, "10.3.2", null);
            store.putParts("", file, content);
            files.add(file);
        }
        var shard = new ShardSnapshot("repo", "", "snap", 1, 0, files);
        String previous = System.getProperty(ShardMaterializer.CONCURRENCY_PROPERTY);
        System.setProperty(ShardMaterializer.CONCURRENCY_PROPERTY, "2");
        try {
            new ShardMaterializer(store).materialize(shard, tmp.resolve("two"));
            for (var value : Map.of("0", 1, "-3", 1, "32", 32, "1000", 32, "not a number", 8).entrySet()) {
                System.setProperty(ShardMaterializer.CONCURRENCY_PROPERTY, value.getKey());
                assertEquals(value.getValue(), ShardMaterializer.configuredConcurrency(), value.getKey());
            }
        } finally {
            if (previous == null) System.clearProperty(ShardMaterializer.CONCURRENCY_PROPERTY);
            else System.setProperty(ShardMaterializer.CONCURRENCY_PROPERTY, previous);
        }
        assertEquals(2, store.maxOpen.get());
        store.maxOpen.set(0);
        new ShardMaterializer(store, 1).materialize(shard, tmp.resolve("one"));
        assertEquals(1, store.maxOpen.get());
        assertThrows(IllegalArgumentException.class, () -> new ShardMaterializer(store, 0));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 105, 127, 128, 129, 1000})
    void rangedDownloadsReassembleExactBytesAroundRangeAndPartBoundaries(int length, @TempDir Path tmp) throws Exception {
        for (int partSize : new int[]{length, 100}) {
            for (int rangeBytes : new int[]{8, 64}) {
                Path dir = tmp.resolve((partSize == length ? "single-" : "parted-") + rangeBytes);
                boolean footer = length >= 16;
                byte[] content = footer ? LuceneFooterVerifierTest.luceneFile(length, length) : new byte[length];
                if (!footer) new java.util.SplittableRandom(length).nextBytes(content);
                String checksum = footer ? LuceneFooterVerifierTest.checksumOf(content) : null;
                String label = "part size " + partSize + ", range " + rangeBytes;
                var shard = partedShard(dir, content, partSize, checksum);
                new ShardMaterializer(new FileSystemBlobStore(dir.resolve("repo")), 4, 0, rangeBytes)
                    .materialize(shard, dir.resolve("out"));
                assertArrayEquals(content, Files.readAllBytes(dir.resolve("out/_0.fdt")), label);

                if (footer) content[length - 3] ^= 0x10;
                var corrupt = partedShard(dir.resolve("corrupt"), content, partSize, footer ? checksum : "abc");
                var failure = assertThrows(IOException.class, () -> new ShardMaterializer(
                    new FileSystemBlobStore(dir.resolve("corrupt/repo")), 4, 0, rangeBytes).materialize(corrupt, dir.resolve("corrupt/out")));
                assertEquals(footer ? LuceneFooterVerifier.CHECKSUM_MISMATCH : LuceneFooterVerifier.NO_FOOTER, failure.getMessage(), label);
            }
        }
    }

    @Test
    void rangedDownloadsCompletingOutOfOrderStillVerifyEveryFile(@TempDir Path tmp) throws Exception {
        var store = new FakeStore(3);
        store.pinsVersions = true;
        var random = new java.util.SplittableRandom(34);
        var files = new java.util.ArrayList<RepoFileInfo>();
        var expected = new java.util.HashMap<String, byte[]>();
        for (int i = 0; i < 10; i++) {
            byte[] content = LuceneFooterVerifierTest.luceneFile(random.nextInt(16, 600_000), 300 + i);
            long partSize = i % 2 == 0 ? random.nextInt(50_000, 250_000) : 0;
            var file = new RepoFileInfo("__r" + i, "_" + i + ".fdt", content.length,
                LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null, partSize);
            store.putParts("indices/x/0/", file, content);
            files.add(file);
            expected.put(file.physicalName(), content);
        }
        var shard = new ShardSnapshot("repo", "indices/x/0/", "snap", 1, 0, files);
        new ShardMaterializer(store, 8, 100_000, 32_768).materialize(shard, tmp.resolve("out"));
        for (var entry : expected.entrySet()) {
            assertArrayEquals(entry.getValue(), Files.readAllBytes(tmp.resolve("out").resolve(entry.getKey())), entry.getKey());
        }
        assertTrue(store.rangeReads.get() > 20, "parts above the threshold are fetched as ranges: " + store.rangeReads.get());
        assertEquals(0, store.open.get());
        assertTrue(store.maxOpen.get() > 1 && store.maxOpen.get() <= 8, "concurrent streams: " + store.maxOpen.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aBlobOverwrittenBetweenTwoRangesIsNeverStitched(boolean storeIgnoresExpectedVersion, @TempDir Path tmp) throws Exception {
        var store = new FakeStore(1);
        store.pinsVersions = true;
        store.ignoresExpectedVersion = storeIgnoresExpectedVersion;
        byte[] content = LuceneFooterVerifierTest.luceneFile(500_000, 41);
        var file = new RepoFileInfo("__big", "_0.fdt", content.length, LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null);
        store.putParts("", file, content);
        store.overwriteAfterFirstRangeOf = "__big";
        var shard = new ShardSnapshot("repo", "", "snap", 1, content.length, List.of(file));
        var failure = assertThrows(IOException.class,
            () -> new ShardMaterializer(store, 4, 100_000, 65_536).materialize(shard, tmp.resolve("out")));
        assertEquals(BlobStore.BLOB_CHANGED, failure.getMessage());
        assertNoDownloadTaskRunning();
        assertEquals(0, store.open.get());
    }

    @Test
    void aStoreThatCannotPinVersionsGetsLargePartsReadWhole(@TempDir Path tmp) throws Exception {
        var store = new FakeStore(1);
        byte[] content = LuceneFooterVerifierTest.luceneFile(800_000, 42);
        var file = new RepoFileInfo("__big", "_0.fdt", content.length, LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null, 300_000);
        store.putParts("", file, content);
        var shard = new ShardSnapshot("repo", "", "snap", 1, content.length, List.of(file));
        new ShardMaterializer(store, 4, 100_000, 65_536).materialize(shard, tmp.resolve("out"));
        assertArrayEquals(content, Files.readAllBytes(tmp.resolve("out/_0.fdt")));
        for (int part = 0; part < file.partCount(); part++) {
            assertEquals(2, store.readsByKey.get(file.partName(part)).get(),
                "one unpinned first range, then the whole part: " + file.partName(part));
        }
        assertEquals(0, store.open.get());
    }

    @Test
    void oneFailingRangeFailsTheShardOnlyAfterEveryTaskHasStopped(@TempDir Path tmp) throws Exception {
        var store = new FakeStore(5);
        store.pinsVersions = true;
        var files = new java.util.ArrayList<RepoFileInfo>();
        for (int i = 0; i < 3; i++) {
            byte[] content = LuceneFooterVerifierTest.luceneFile(3_000_000, 400 + i);
            var file = new RepoFileInfo("__r" + i, "_" + i + ".fdt", content.length,
                LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null);
            store.putParts("", file, content);
            files.add(file);
        }
        store.failingKey = "__r1";
        store.failingRangeOffset = 5 * 65_536;
        store.stubbornMillis = 10;
        var shard = new ShardSnapshot("repo", "", "snap", 1, 0, files);
        Path out = tmp.resolve("out");
        var failure = assertThrows(IOException.class,
            () -> new ShardMaterializer(store, 4, 100_000, 65_536).materialize(shard, out));
        assertEquals(0, store.activeCalls.get(), "a range task is still inside the store when materialize throws");
        assertNoDownloadTaskRunning();
        assertEquals("injected failure", failure.getMessage());
        assertEquals(0, store.open.get(), "every range stream is closed when materialize throws");
        int rangesAtFailure = store.rangeReads.get();
        assertTrue(rangesAtFailure < 3 * 46, "queued ranges are never started after the failure: " + rangesAtFailure);
        deleteRecursively(out);
        Thread.sleep(100);
        assertFalse(Files.exists(out), "no task writes into the directory after materialize threw");
        assertEquals(rangesAtFailure, store.rangeReads.get());
    }

    @Test
    void partsLongerThanTheMetadataAreRejectedWhenDownloadedSeparately(@TempDir Path tmp) throws Exception {
        byte[] content = LuceneFooterVerifierTest.luceneFile(300, 43);
        var file = new RepoFileInfo("__parted", "_0.fdt", content.length, LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null, 100);
        for (long threshold : new long[]{0, 1_000}) {
            Path repo = Files.createDirectories(tmp.resolve("repo-" + threshold));
            for (int part = 0; part < file.partCount(); part++) {
                int from = part * 100;
                Files.write(repo.resolve(file.partName(part)), java.util.Arrays.copyOfRange(content, from, from + 100));
            }
            Files.write(repo.resolve(file.partName(1)), new byte[101]);
            var shard = new ShardSnapshot("repo", "", "snap", 1, content.length, List.of(file));
            var failure = assertThrows(IOException.class, () -> new ShardMaterializer(
                new FileSystemBlobStore(repo), 4, threshold, 64).materialize(shard, tmp.resolve("out-" + threshold)));
            assertEquals(LuceneFooterVerifier.LENGTH_MISMATCH, failure.getMessage(), "threshold " + threshold);
        }
    }

    /**
     * Through the S3 client, bodies that break mid-way (reset, premature close, read timeout) are
     * continued inside the store, so every file still verifies and nothing is fetched twice:
     * sequentially (c1), at the default concurrency (whole blobs and parts), and as ranges.
     */
    @ParameterizedTest
    @ValueSource(strings = {"c1", "c8", "ranged"})
    void bodiesBrokenMidwayOnAFlakyS3StoreStillVerifyEveryFile(String config, @TempDir Path tmp) throws Exception {
        try (var stub = new FaultyS3Stub()) {
            stub.resetDelayMillis = 50;
            var random = new java.util.SplittableRandom(77);
            var files = new java.util.ArrayList<RepoFileInfo>();
            var expected = new java.util.HashMap<String, byte[]>();
            long total = 0;
            for (int i = 0; i < 10; i++) {
                byte[] content = LuceneFooterVerifierTest.luceneFile(random.nextInt(100_000, 1_500_000), 500 + i);
                long partSize = i % 3 == 0 ? random.nextInt(300_000, 700_000) : 0;
                var file = new RepoFileInfo("__s" + i, "_" + i + ".fdt", content.length,
                    LuceneFooterVerifierTest.checksumOf(content), "10.3.2", null, partSize);
                long size = file.partCount() == 1 ? content.length : partSize;
                for (int part = 0; part < file.partCount(); part++) {
                    int from = (int) (part * size);
                    stub.put("indices/x/0/" + file.partName(part),
                        java.util.Arrays.copyOfRange(content, from, (int) Math.min(content.length, from + size)), "e" + i + "p" + part);
                }
                files.add(file);
                expected.put(file.physicalName(), content);
                total += content.length;
            }
            var getsByKey = new java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicInteger>();
            var faults = new java.util.concurrent.atomic.AtomicInteger();
            stub.plan = (request, bodyLength) -> {
                int nth = getsByKey.computeIfAbsent(request.key(), k -> new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet();
                if (bodyLength < 64 * 1024 || nth > (request.key().hashCode() % 2 == 0 ? 2 : 1)) return null;
                int fault = faults.getAndIncrement();
                var kind = fault == 0 ? FaultyS3Stub.Fault.STALL : fault % 4 == 1 ? FaultyS3Stub.Fault.RESET : FaultyS3Stub.Fault.CLOSE;
                return new FaultyS3Stub.Injection(kind, bodyLength / 2);
            };
            var shard = new ShardSnapshot("repo", "indices/x/0/", "snap", 1, total, files);
            try (var store = stub.store(S3BodyResumeTest.FAST_RESUME, S3BodyResumeTest.SOCKET_TIMEOUT)) {
                var materializer = switch (config) {
                    case "c1" -> new ShardMaterializer(store, 1);
                    case "c8" -> new ShardMaterializer(store, 8);
                    default -> new ShardMaterializer(store, 8, 400_000, 128 * 1024);
                };
                materializer.materialize(shard, tmp.resolve("out"));
                for (var entry : expected.entrySet()) {
                    assertArrayEquals(entry.getValue(), Files.readAllBytes(tmp.resolve("out").resolve(entry.getKey())), entry.getKey());
                }
                assertTrue(stub.faultsInjected.get() >= 10, "faults: " + stub.faultsInjected.get());
                assertEquals(stub.faultsInjected.get(), store.bodyResumes());
            }
            assertTrue(stub.bodyBytesSent.get() - total < (1 << 20),
                "only the undelivered rest is fetched again: " + stub.bodyBytesSent.get() + " for " + total);
            long deadline = System.nanoTime() + 2_000_000_000L;
            while (stub.activeConnections.get() > 0 && System.nanoTime() < deadline) Thread.sleep(10);
            assertEquals(0, stub.activeConnections.get());
        }
    }

    /** Checked at once, not polled: materialize must not return or throw before its tasks have ended. */
    private static void assertNoDownloadTaskRunning() {
        boolean running = Thread.getAllStackTraces().keySet().stream()
            .anyMatch(t -> t.getName().startsWith("gefrierschrank-download-") && t.isAlive()
                && java.util.Arrays.stream(t.getStackTrace()).anyMatch(f -> f.getClassName().equals(ShardMaterializer.class.getName())));
        assertFalse(running, "a download task is still running after materialize threw");
    }

    private static void deleteRecursively(Path dir) throws IOException {
        try (var walk = Files.walk(dir)) {
            for (Path p : walk.sorted(java.util.Comparator.reverseOrder()).toList()) Files.delete(p);
        }
    }

    /**
     * In-memory store whose reads start after a random delay and trickle 64 KiB chunks with
     * small pauses, so downloads overlap and finish in random order. A closed stream fails its
     * next read, like an aborted HTTP body. {@link #activeCalls} counts the threads inside the
     * store at any moment.
     */
    static final class FakeStore implements BlobStore {
        final java.util.Map<String, byte[]> blobs = new java.util.concurrent.ConcurrentHashMap<>();
        final java.util.concurrent.atomic.AtomicInteger open = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger maxOpen = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger rangeReads = new java.util.concurrent.atomic.AtomicInteger();
        final Map<String, java.util.concurrent.atomic.AtomicInteger> readsByKey = new java.util.concurrent.ConcurrentHashMap<>();
        final Map<String, Integer> versions = new java.util.concurrent.ConcurrentHashMap<>();
        final int maxDelayMillis;
        volatile String failingKey;
        /** With a failingKey: fail only the ranged read of that key at this offset. */
        volatile long failingRangeOffset = -1;
        volatile boolean pinsVersions;
        volatile boolean ignoresExpectedVersion;
        volatile String overwriteAfterFirstRangeOf;
        /**
         * When above 0, every pause lasts at least this long and neither an interrupt nor closing
         * the stream cuts it short, like a read blocked on a socket, so cancellation alone cannot
         * end a running task quickly.
         */
        volatile int stubbornMillis;
        final java.util.concurrent.atomic.AtomicInteger activeCalls = new java.util.concurrent.atomic.AtomicInteger();

        FakeStore(int maxDelayMillis) {
            this.maxDelayMillis = maxDelayMillis;
        }

        void putParts(String prefix, RepoFileInfo file, byte[] content) {
            long partSize = file.partCount() == 1 ? content.length : file.partSize();
            for (int part = 0; part < file.partCount(); part++) {
                int from = (int) (part * partSize);
                blobs.put(prefix + file.partName(part),
                    java.util.Arrays.copyOfRange(content, from, (int) Math.min(content.length, from + partSize)));
            }
        }

        private void pause() throws IOException {
            int delay = java.util.concurrent.ThreadLocalRandom.current().nextInt(maxDelayMillis + 1);
            if (stubbornMillis > 0) {
                sleepThroughInterrupts(stubbornMillis + delay);
                return;
            }
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new java.io.InterruptedIOException();
            }
        }

        private static void sleepThroughInterrupts(long millis) {
            long end = System.nanoTime() + millis * 1_000_000;
            boolean interrupted = false;
            for (long left = millis; left > 0; left = (end - System.nanoTime()) / 1_000_000) {
                try {
                    Thread.sleep(left);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) Thread.currentThread().interrupt();
        }

        @Override
        public java.io.InputStream read(String key) throws IOException {
            activeCalls.incrementAndGet();
            try {
                return readCounted(key);
            } finally {
                activeCalls.decrementAndGet();
            }
        }

        private java.io.InputStream readCounted(String key) throws IOException {
            reads.incrementAndGet();
            readsByKey.computeIfAbsent(key, k -> new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet();
            pause();
            if (key.equals(failingKey) && failingRangeOffset < 0) throw new IOException("injected failure");
            byte[] content = blobs.get(key);
            if (content == null) throw new IOException("Blob not found: " + key);
            return trickle(content);
        }

        /**
         * With {@link #pinsVersions}, a real ranged read: the version counts overwrites of the
         * blob, and an expected version that no longer matches fails unless the store is set to
         * ignore it. Otherwise the interface default (read and skip, no version).
         */
        @Override
        public RangeRead openRange(String key, long offset, long length, String expectedVersion) throws IOException {
            if (!pinsVersions) return BlobStore.super.openRange(key, offset, length, expectedVersion);
            activeCalls.incrementAndGet();
            try {
                return openPinnedRange(key, offset, length, expectedVersion);
            } finally {
                activeCalls.decrementAndGet();
            }
        }

        private RangeRead openPinnedRange(String key, long offset, long length, String expectedVersion) throws IOException {
            rangeReads.incrementAndGet();
            pause();
            if (key.equals(failingKey) && offset == failingRangeOffset) throw new IOException("injected failure");
            byte[] content = blobs.get(key);
            if (content == null) throw new IOException("Blob not found: " + key);
            String version = "v" + versions.getOrDefault(key, 0);
            if (expectedVersion != null && !ignoresExpectedVersion && !expectedVersion.equals(version)) {
                throw new IOException(BLOB_CHANGED);
            }
            if (offset + length > content.length) throw new IOException(RANGE_OUTSIDE_BLOB);
            if (key.equals(overwriteAfterFirstRangeOf)) {
                overwriteAfterFirstRangeOf = null;
                byte[] changed = content.clone();
                changed[changed.length / 2] ^= 1;
                blobs.put(key, changed);
                versions.merge(key, 1, Integer::sum);
            }
            return new RangeRead(trickle(java.util.Arrays.copyOfRange(content, (int) offset, (int) (offset + length))),
                version, content.length);
        }

        private java.io.InputStream trickle(byte[] content) {
            maxOpen.accumulateAndGet(open.incrementAndGet(), Math::max);
            return new java.io.InputStream() {
                private int position;
                private volatile boolean closed;

                @Override
                public int read() throws IOException {
                    byte[] one = new byte[1];
                    return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
                }

                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    if (len == 0) return 0;
                    activeCalls.incrementAndGet();
                    try {
                        pause();
                        if (closed) throw new IOException("stream closed");
                        if (position == content.length) return -1;
                        int count = Math.min(Math.min(len, 64 * 1024), content.length - position);
                        System.arraycopy(content, position, b, off, count);
                        position += count;
                        return count;
                    } finally {
                        activeCalls.decrementAndGet();
                    }
                }

                @Override
                public void close() {
                    if (!closed) {
                        closed = true;
                        open.decrementAndGet();
                    }
                }
            };
        }

        @Override
        public java.util.Optional<BlobMetadata> stat(String key) {
            byte[] content = blobs.get(key);
            return content == null ? java.util.Optional.empty() : java.util.Optional.of(new BlobMetadata(key, content.length));
        }

        @Override
        public List<String> list(String prefix) {
            return blobs.keySet().stream().filter(k -> k.startsWith(prefix)).sorted().toList();
        }

        @Override
        public List<BlobMetadata> listWithSizes(String prefix) {
            return list(prefix).stream().map(k -> new BlobMetadata(k, blobs.get(k).length)).toList();
        }

        @Override
        public void put(String key, byte[] content) {
            blobs.put(key, content);
        }
    }

    static final Path DEMO_REPO = Path.of("src/test/resources/fixtures/demo-repo");

    static Map<String, String> golden(String index) {
        return GOLDEN.entrySet().stream().filter(e -> e.getKey().startsWith(index + "/"))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    static Map<String, String> digests(String index, Path dir) throws Exception {
        var result = new java.util.HashMap<String, String>();
        try (var files = Files.list(dir)) {
            for (Path file : files.toList()) {
                byte[] digest = MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file));
                result.put(index + "/" + file.getFileName(), HexFormat.of().formatHex(digest));
            }
        }
        return result;
    }

    /** One file of {@code content} stored as {@code .partN} blobs of {@code partSize} bytes. */
    static ShardSnapshot partedShard(Path tmp, byte[] content, int partSize, String checksum) throws IOException {
        Path repo = Files.createDirectories(tmp.resolve("repo"));
        var file = new RepoFileInfo("__parted", "_0.fdt", content.length, checksum, "10.3.2", null, partSize);
        for (int part = 0; part < file.partCount(); part++) {
            int from = part * partSize;
            Files.write(repo.resolve(file.partName(part)),
                java.util.Arrays.copyOfRange(content, from, Math.min(content.length, from + partSize)));
        }
        return new ShardSnapshot("repo", "", "snap", 1, content.length, List.of(file));
    }
}
