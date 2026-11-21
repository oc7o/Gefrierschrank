package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import java.util.SplittableRandom;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

class Crc32CombineTest {

    private static final int MIB = 1 << 20;

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 7, 8, 9, 16 * MIB - 1, 16 * MIB, 16 * MIB + 1})
    void combiningCrcsOfRandomSplitsEqualsCrcOfTheWhole(int length) {
        var random = new SplittableRandom(length);
        byte[] data = new byte[length];
        random.nextBytes(data);
        long whole = crc(data, 0, length);
        int trials = length > MIB ? 4 : 64;
        for (int trial = 0; trial < trials; trial++) {
            int split = length == 0 ? 0 : random.nextInt(length + 1);
            long combined = Crc32Combine.combine(crc(data, 0, split), crc(data, split, length - split), length - split);
            assertEquals(whole, combined, "split at " + split);
        }
    }

    @Test
    void combiningManyPiecesInOrderEqualsCrcOfTheWhole() {
        var random = new SplittableRandom(7);
        byte[] data = new byte[3 * MIB + 5];
        random.nextBytes(data);
        long combined = 0;
        int position = 0;
        while (position < data.length) {
            int piece = Math.min(data.length - position, random.nextInt(0, 700_000));
            combined = Crc32Combine.combine(combined, crc(data, position, piece), piece);
            position += piece;
        }
        assertEquals(crc(data, 0, data.length), combined);
    }

    @Test
    void emptySecondPieceLeavesTheFirstCrc() {
        assertEquals(0xCBF43926L, Crc32Combine.combine(0xCBF43926L, 0, 0));
        assertEquals(0xCBF43926L, Crc32Combine.combine(0, 0xCBF43926L, 9));
    }

    @Test
    void rejectsNegativeLength() {
        assertThrows(IllegalArgumentException.class, () -> Crc32Combine.combine(0, 0, -1));
    }

    private static long crc(byte[] data, int offset, int length) {
        var crc = new CRC32();
        crc.update(data, offset, length);
        return crc.getValue();
    }
}
