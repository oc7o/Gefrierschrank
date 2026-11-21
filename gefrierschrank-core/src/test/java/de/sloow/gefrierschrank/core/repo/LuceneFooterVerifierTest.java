package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

class LuceneFooterVerifierTest {

    /** Body bytes followed by the big-endian CRC32 of everything before it, like a Lucene footer. */
    static byte[] luceneFile(int length, long seed) {
        byte[] file = new byte[length];
        new SplittableRandom(seed).nextBytes(file);
        var crc = new CRC32();
        crc.update(file, 0, length - 8);
        ByteBuffer.wrap(file, length - 8, 8).putLong(crc.getValue());
        return file;
    }

    static String checksumOf(byte[] file) {
        return Long.toString(ByteBuffer.wrap(file, file.length - 8, 8).getLong(), 36);
    }

    private static void feed(LuceneFooterVerifier verifier, byte[] file, int... cuts) throws IOException {
        int from = 0;
        for (int cut : cuts) {
            verifier.update(from, Arrays.copyOfRange(file, from, cut));
            from = cut;
        }
        verifier.update(from, Arrays.copyOfRange(file, from, file.length));
    }

    @Test
    void acceptsAValidFileFedInOneChunk() throws IOException {
        byte[] file = luceneFile(100, 1);
        var verifier = new LuceneFooterVerifier(file.length, checksumOf(file));
        verifier.update(0, file);
        verifier.finish();
    }

    @Test
    void acceptsAFooterStraddlingChunkBoundaries() throws IOException {
        byte[] file = luceneFile(40, 2);
        for (int cut = 30; cut < 40; cut++) {
            var verifier = new LuceneFooterVerifier(file.length, checksumOf(file));
            feed(verifier, file, cut, Math.min(cut + 3, 40));
            verifier.finish();
        }
    }

    @Test
    void rejectsACorruptedFooterStraddlingChunkBoundaries() throws IOException {
        byte[] file = luceneFile(40, 3);
        String checksum = checksumOf(file);
        file[36] ^= 0x10;
        var verifier = new LuceneFooterVerifier(file.length, checksum);
        feed(verifier, file, 35);
        assertMessage(LuceneFooterVerifier.CHECKSUM_MISMATCH, verifier);
    }

    @Test
    void rejectsAMatchingCrcWithADifferentMetadataChecksum() throws IOException {
        byte[] file = luceneFile(64, 4);
        var verifier = new LuceneFooterVerifier(file.length, "0" + checksumOf(file));
        verifier.update(0, file);
        assertMessage(LuceneFooterVerifier.CHECKSUM_MISMATCH, verifier);
    }

    @Test
    void rejectsAMatchingMetadataChecksumWhenTheBodyDoesNotMatchIt() throws IOException {
        byte[] file = luceneFile(64, 5);
        String checksum = checksumOf(file);
        file[10] ^= 1;
        var verifier = new LuceneFooterVerifier(file.length, checksum);
        verifier.update(0, file);
        assertMessage(LuceneFooterVerifier.CHECKSUM_MISMATCH, verifier);
    }

    @Test
    void rejectsBytesBeyondTheMetadataLengthImmediately() throws IOException {
        byte[] file = luceneFile(64, 6);
        var verifier = new LuceneFooterVerifier(file.length - 1, null);
        var failure = assertThrows(IOException.class, () -> verifier.update(0, file));
        assertEquals(LuceneFooterVerifier.LENGTH_MISMATCH, failure.getMessage());
    }

    @Test
    void rejectsFewerBytesThanTheMetadataLength() throws IOException {
        byte[] file = luceneFile(64, 7);
        var verifier = new LuceneFooterVerifier(file.length + 1, null);
        verifier.update(0, file);
        assertMessage(LuceneFooterVerifier.LENGTH_MISMATCH, verifier);
    }

    @Test
    void zeroLengthChunksAndFilesAreHandled() throws IOException {
        var empty = new LuceneFooterVerifier(0, null);
        empty.update(0, new byte[0]);
        empty.finish();

        byte[] file = luceneFile(32, 8);
        var verifier = new LuceneFooterVerifier(file.length, checksumOf(file));
        verifier.update(0, Arrays.copyOfRange(file, 0, 20));
        verifier.update(20, new byte[0]);
        verifier.update(20, Arrays.copyOfRange(file, 20, 32));
        verifier.finish();
    }

    @Test
    void rejectsShortFilesWhenAChecksumIsRecorded() throws IOException {
        byte[] file = new byte[15];
        var verifier = new LuceneFooterVerifier(file.length, "abc");
        verifier.update(0, file);
        assertMessage(LuceneFooterVerifier.NO_FOOTER, verifier);
        var emptyWithChecksum = new LuceneFooterVerifier(0, "abc");
        assertMessage(LuceneFooterVerifier.NO_FOOTER, emptyWithChecksum);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "  "})
    void blankChecksumMeansTheLengthCheckOnly(String checksum) throws IOException {
        byte[] garbage = {1, 2, 3};
        var verifier = new LuceneFooterVerifier(garbage.length, checksum);
        verifier.update(0, garbage);
        verifier.finish();
        var truncated = new LuceneFooterVerifier(garbage.length, checksum);
        truncated.update(0, new byte[]{1, 2});
        assertMessage(LuceneFooterVerifier.LENGTH_MISMATCH, truncated);
    }

    @Test
    void acceptsRangesArrivingOutOfOrderFromSeveralThreads() throws Exception {
        byte[] file = luceneFile(5_000_003, 9);
        var verifier = new LuceneFooterVerifier(file.length, checksumOf(file));
        List<int[]> ranges = new ArrayList<>();
        for (int from = 0; from < file.length; from += 65_537) ranges.add(new int[]{from, Math.min(file.length, from + 65_537)});
        Collections.shuffle(ranges, new java.util.Random(9));
        var pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int[] range : ranges) {
                futures.add(pool.submit(() -> {
                    for (int from = range[0]; from < range[1]; from += 4096) {
                        int to = Math.min(range[1], from + 4096);
                        verifier.update(from, file, from, to - from);
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) future.get();
        } finally {
            pool.shutdown();
        }
        verifier.finish();
    }

    @Test
    void rejectsGapsAndOverlaps() throws IOException {
        byte[] file = luceneFile(64, 10);
        var gap = new LuceneFooterVerifier(file.length, null);
        gap.update(0, Arrays.copyOfRange(file, 0, 20));
        gap.update(30, Arrays.copyOfRange(file, 30, 64));
        assertMessage(LuceneFooterVerifier.LENGTH_MISMATCH, gap);

        var duplicate = new LuceneFooterVerifier(file.length, null);
        duplicate.update(0, file);
        duplicate.update(0, Arrays.copyOfRange(file, 0, 10));
        assertMessage(LuceneFooterVerifier.LENGTH_MISMATCH, duplicate);

        var overlap = new LuceneFooterVerifier(file.length, null);
        overlap.update(0, Arrays.copyOfRange(file, 0, 40));
        overlap.update(30, Arrays.copyOfRange(file, 30, 64));
        assertMessage(LuceneFooterVerifier.LENGTH_MISMATCH, overlap);
    }

    private static void assertMessage(String expected, LuceneFooterVerifier verifier) {
        var failure = assertThrows(IOException.class, verifier::finish);
        assertEquals(expected, failure.getMessage());
    }
}
