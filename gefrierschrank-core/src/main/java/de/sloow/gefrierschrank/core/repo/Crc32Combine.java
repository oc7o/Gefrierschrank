package de.sloow.gefrierschrank.core.repo;

/**
 * CRC32 of a concatenation from the CRCs of its parts: a port of zlib's {@code crc32_combine64}
 * (1.2.12+, x2nmodp variant), which {@link java.util.zip.CRC32} does not offer. Lets byte ranges
 * of one file be checksummed independently and still be verified against the single footer CRC.
 */
final class Crc32Combine {

    /** Reflected CRC-32 polynomial. */
    private static final int POLY = 0xEDB88320;
    /** x^(2^n) modulo POLY for n = 0..31. */
    private static final int[] X2N_TABLE = new int[32];

    static {
        int p = 1 << 30;
        X2N_TABLE[0] = p;
        for (int n = 1; n < 32; n++) X2N_TABLE[n] = p = multModP(p, p);
    }

    private Crc32Combine() {}

    /** CRC32 of {@code A || B}, given {@code crc1 = CRC32(A)}, {@code crc2 = CRC32(B)} and {@code len2 = |B|}. */
    static long combine(long crc1, long crc2, long len2) {
        if (len2 < 0) throw new IllegalArgumentException("negative length");
        return (multModP(x2nModP(len2, 3), (int) crc1) ^ (int) crc2) & 0xFFFFFFFFL;
    }

    /** a(x) * b(x) modulo POLY; {@code a} must be non-zero. */
    private static int multModP(int a, int b) {
        int m = 1 << 31;
        int p = 0;
        for (;;) {
            if ((a & m) != 0) {
                p ^= b;
                if ((a & (m - 1)) == 0) break;
            }
            m >>>= 1;
            b = (b & 1) != 0 ? (b >>> 1) ^ POLY : b >>> 1;
        }
        return p;
    }

    /** x^(n * 2^k) modulo POLY. */
    private static int x2nModP(long n, int k) {
        int p = 1 << 31;
        while (n != 0) {
            if ((n & 1) != 0) p = multModP(X2N_TABLE[k & 31], p);
            n >>>= 1;
            k++;
        }
        return p;
    }
}
