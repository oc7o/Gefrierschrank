package de.sloow.gefrierschrank.syntheticsource;

import de.sloow.gefrierschrank.core.codec.ZstdFrameDecoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Batched reconstruction ({@link SyntheticSourceReconstructor#readSourcesBytes}), which hands
 * Elasticsearch's loaders the ids of a run of documents as its fetch phase does, gives every
 * document exactly the bytes the per-document path gives it: on ES 8.x standard and logsdb
 * synthetic indices, ES 9.3 standard, logsdb and TSDB indices (including ES819 binary doc values
 * and histograms), ZSTD stored fields with either decoder, hard and soft deletes, and multi-segment
 * shards whose batches cross segment boundaries.
 */
class SyntheticBatchReconstructionTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty(IsolatedReconstructionWorker.BATCH_BYTES_PROPERTY);
        System.clearProperty(ZstdFrameDecoder.DECODER_PROPERTY);
    }

    static Stream<String> fixtures() {
        return Stream.of(
            "synth-repo/test_plain_synth", "synth-repo/test_tsdb_synth",
            "zstd-synth-repo/test_zstd_synth", "zstd-synth-repo/test_zstd_logsdb",
            "synth-deletes-repo/test_deletes_lz4", "synth-deletes-repo/test_deletes_zstd",
            "es93-repo/test_es93_logsdb_binary", "es93-repo/test_es93_tsdb_hist");
    }

    @ParameterizedTest
    @MethodSource("fixtures")
    void everyBatchShapeGivesThePerDocumentBytes(String fixtureIndex, @TempDir Path tmp) throws Exception {
        var fixture = SyntheticSourceReconstructorTest.Fixture.materialize(fixtureIndex, tmp);
        try (var reconstructor = fixture.open()) {
            int[] all = IntStream.range(0, reconstructor.docCount()).toArray();
            List<byte[]> perDocument = perDocument(reconstructor, all);
            for (int size : new int[]{1024, 777, 1}) {
                assertBytesEqual(perDocument, batched(reconstructor, all, size), fixtureIndex + " batch " + size);
            }
            // Gaps like those deleted and nested documents leave: skip ids 1 and 3 of every 7.
            int[] sparse = IntStream.range(0, all.length).filter(i -> i % 7 != 1 && i % 7 != 3).toArray();
            List<byte[]> expected = new ArrayList<>();
            for (int doc : sparse) expected.add(perDocument.get(doc));
            assertBytesEqual(expected, batched(reconstructor, sparse, 1024), fixtureIndex + " sparse");
        }
    }

    /** The golden fixtures batched: the 1.0.0 text, not only the per-document path's. */
    @ParameterizedTest
    @MethodSource("goldenFixtures")
    void batchesReproduceTheGolden(String fixtureIndex, @TempDir Path tmp) throws Exception {
        List<String> golden = ReconstructionDirectoryTest.golden(fixtureIndex);
        try (var reconstructor = SyntheticSourceReconstructorTest.Fixture.materialize(fixtureIndex, tmp).open()) {
            List<String> text = new ArrayList<>();
            for (byte[] source : batched(reconstructor, IntStream.range(0, golden.size()).toArray(), 1024)) {
                text.add(new String(source, StandardCharsets.UTF_8));
            }
            assertEquals(golden, text, fixtureIndex);
        }
    }

    static Stream<String> goldenFixtures() {
        return SyntheticSourceReconstructorTest.GOLDEN_INDICES.stream().filter(name -> !name.endsWith("nested"));
    }

    @ParameterizedTest
    @MethodSource("zstdFixtures")
    void thePureJavaZstdDecoderBatchesIdentically(String fixtureIndex, @TempDir Path tmp) throws Exception {
        System.setProperty(ZstdFrameDecoder.DECODER_PROPERTY, "java");
        var fixture = SyntheticSourceReconstructorTest.Fixture.materialize(fixtureIndex, tmp);
        try (var reconstructor = fixture.open()) {
            int[] all = IntStream.range(0, reconstructor.docCount()).toArray();
            assertBytesEqual(perDocument(reconstructor, all), batched(reconstructor, all, 777), fixtureIndex);
        }
    }

    static Stream<String> zstdFixtures() {
        return Stream.of("zstd-synth-repo/test_zstd_synth", "zstd-synth-repo/test_zstd_logsdb",
            "synth-deletes-repo/test_deletes_zstd", "es93-repo/test_es93_logsdb_binary");
    }

    /** A batch stops at the document that reaches the byte limit and the caller continues from there. */
    @Test
    void batchesStopShortAtTheByteLimit(@TempDir Path tmp) throws Exception {
        System.setProperty(IsolatedReconstructionWorker.BATCH_BYTES_PROPERTY, "2000");
        var fixture = SyntheticSourceReconstructorTest.Fixture.materialize("synth-deletes-repo/test_deletes_lz4", tmp);
        try (var reconstructor = fixture.open()) {
            int[] all = IntStream.range(0, reconstructor.docCount()).toArray();
            List<Integer> lengths = new ArrayList<>();
            List<byte[]> batched = new ArrayList<>();
            for (int from = 0; from < all.length; ) {
                byte[][] sources = reconstructor.readSourcesBytes(Arrays.copyOfRange(all, from, all.length));
                assertTrue(sources.length >= 1);
                long bytes = 0;
                for (int i = 0; i < sources.length; i++) {
                    if (i < sources.length - 1) bytes += sources[i].length;
                    batched.add(sources[i]);
                }
                assertTrue(bytes < 2000, "only the last document of a batch crosses the limit");
                lengths.add(sources.length);
                from += sources.length;
            }
            assertTrue(lengths.size() > 5, "short batches: " + lengths);
            assertBytesEqual(perDocument(reconstructor, all), batched, "byte-limited batches");
        }
    }

    /** Batches and the per-document cursor share no loaders, whatever the interleaving. */
    @Test
    void batchesAndSingleDocumentsInterleave(@TempDir Path tmp) throws Exception {
        String name = "synth-deletes-repo/test_deletes_lz4";
        List<String> golden = ReconstructionDirectoryTest.golden(name);
        try (var reconstructor = SyntheticSourceReconstructorTest.Fixture.materialize(name, tmp).open()) {
            List<String> text = new ArrayList<>();
            int doc = 0;
            while (doc < golden.size()) {
                if (doc % 3 == 0) {
                    text.add(reconstructor.readSource(doc++));
                } else {
                    int[] ids = IntStream.range(doc, Math.min(golden.size(), doc + 17)).toArray();
                    for (byte[] source : reconstructor.readSourcesBytes(ids)) {
                        text.add(new String(source, StandardCharsets.UTF_8));
                        doc++;
                    }
                }
            }
            assertEquals(golden, text);
        }
    }

    @Test
    void everyDocumentGetsItsOwnArray(@TempDir Path tmp) throws Exception {
        try (var reconstructor = SyntheticSourceReconstructorTest.Fixture.materialize("synth-repo/test_tsdb_synth", tmp).open()) {
            byte[][] sources = reconstructor.readSourcesBytes(IntStream.range(0, 20).toArray());
            for (int i = 1; i < sources.length; i++) assertNotSame(sources[i - 1], sources[i]);
        }
    }

    @Test
    void idsMustBeStrictlyAscendingAndInRange(@TempDir Path tmp) throws Exception {
        try (var reconstructor = SyntheticSourceReconstructorTest.Fixture.materialize("synth-repo/test_plain_synth", tmp).open()) {
            assertThrows(IllegalArgumentException.class, () -> reconstructor.readSourcesBytes(new int[]{3, 2}));
            assertThrows(IllegalArgumentException.class, () -> reconstructor.readSourcesBytes(new int[]{2, 2}));
            assertThrows(IllegalArgumentException.class, () -> reconstructor.readSourcesBytes(new int[0]));
            assertThrows(IndexOutOfBoundsException.class, () -> reconstructor.readSourcesBytes(new int[]{0, 50}));
            assertEquals(ReconstructionDirectoryTest.golden("synth-repo/test_plain_synth").get(0),
                new String(reconstructor.readSourcesBytes(new int[]{0, 1})[0], StandardCharsets.UTF_8), "usable afterwards");
        }
    }

    /**
     * Nested roots reconstruct with their nested objects, batched exactly as per document; a nested
     * child is never reconstructed: asking for one fails both ways instead of yielding a wrong block.
     */
    @Test
    void nestedRootsBatchLikePerDocumentAndChildrenAreRefused(@TempDir Path tmp) throws Exception {
        try (var reconstructor = SyntheticSourceReconstructorTest.Fixture.materialize(
                "synth-deletes-repo/test_deletes_nested", tmp).open()) {
            List<Integer> roots = new ArrayList<>();
            List<Integer> children = new ArrayList<>();
            List<byte[]> perDocument = new ArrayList<>();
            for (int doc = 0; doc < reconstructor.docCount(); doc++) {
                try {
                    perDocument.add(reconstructor.readSourceBytes(doc));
                    roots.add(doc);
                } catch (IllegalArgumentException child) {
                    assertEquals("doc id is not a root document", child.getMessage());
                    children.add(doc);
                }
            }
            assertTrue(!roots.isEmpty() && !children.isEmpty(), roots + " " + children);
            int[] rootIds = roots.stream().mapToInt(Integer::intValue).toArray();
            for (int size : new int[]{1024, 7, 1}) {
                assertBytesEqual(perDocument, batched(reconstructor, rootIds, size), "nested batch " + size);
            }
            int child = children.get(0);
            int[] withChild = IntStream.concat(IntStream.of(rootIds).filter(r -> r < child), IntStream.of(child)).toArray();
            assertThrows(IllegalArgumentException.class, () -> reconstructor.readSourcesBytes(withChild));
            assertArrayEquals(perDocument.get(0), reconstructor.readSourcesBytes(new int[]{rootIds[0]})[0], "usable afterwards");
        }
    }

    private static List<byte[]> perDocument(SyntheticSourceReconstructor reconstructor, int[] ids) throws IOException {
        List<byte[]> sources = new ArrayList<>();
        for (int doc : ids) sources.add(reconstructor.readSourceBytes(doc));
        return sources;
    }

    /** Whole input in calls of at most {@code size} ids, continuing after a short (byte-limited) batch. */
    static List<byte[]> batched(SyntheticSourceReconstructor reconstructor, int[] ids, int size) throws IOException {
        List<byte[]> sources = new ArrayList<>();
        for (int from = 0; from < ids.length; ) {
            byte[][] batch = reconstructor.readSourcesBytes(Arrays.copyOfRange(ids, from, Math.min(ids.length, from + size)));
            sources.addAll(Arrays.asList(batch));
            from += batch.length;
        }
        return sources;
    }

    private static void assertBytesEqual(List<byte[]> expected, List<byte[]> actual, String what) {
        assertEquals(expected.size(), actual.size(), what);
        for (int i = 0; i < expected.size(); i++) assertArrayEquals(expected.get(i), actual.get(i), what + " doc " + i);
    }
}
