package de.sloow.gefrierschrank.core.codec;

import com.github.luben.zstd.Zstd;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Differential test: frames produced by libzstd (zstd-jni) for Elasticsearch-shaped stored-field
 * blocks decode byte-exact through both the native and the pure-Java path, and corrupted input
 * never escapes as anything but {@link ZstdFrameDecoder.MalformedFrameException}.
 */
class ZstdFrameDecoderTest {

    static final int ES_DEFAULT_BLOCK = 14 * 1024;
    static final int ES_BEST_COMPRESSION_BLOCK = 240 * 1024;
    private static final byte SENTINEL = (byte) 0xA5;

    @AfterEach
    void clearDecoderProperty() {
        System.clearProperty(ZstdFrameDecoder.DECODER_PROPERTY);
    }

    static Stream<Arguments> blocks() {
        List<Arguments> cases = new ArrayList<>();
        for (int size : new int[]{ES_DEFAULT_BLOCK, ES_BEST_COMPRESSION_BLOCK}) {
            for (int level : new int[]{-5, 1, 3, 9, 19}) {
                cases.add(Arguments.of(size, level));
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0} bytes at level {1}")
    @MethodSource("blocks")
    void decodesEsShapedBlocksByteExactOnBothPaths(int size, int level) throws IOException {
        byte[] original = esShapedBlock(size, level * 31L + size);
        byte[] frame = Zstd.compress(original, level);

        for (ZstdFrameDecoder decoder : List.of(nativeDecoder(), javaDecoder())) {
            byte[] dst = sentinelFilled(original.length + 64);
            int written = decoder.decompress(frame, frame.length, dst, original.length);
            assertEquals(original.length, written, "native=" + decoder.usesNative());
            assertArrayEquals(original, Arrays.copyOf(dst, original.length), "native=" + decoder.usesNative());
            assertSentinelsIntact(dst, original.length);
        }
    }

    @Test
    void honoursSourceAndDestinationOffsets() throws IOException {
        byte[] original = esShapedBlock(ES_DEFAULT_BLOCK, 7);
        byte[] frame = Zstd.compress(original, 3);
        byte[] src = new byte[frame.length + 20];
        System.arraycopy(frame, 0, src, 11, frame.length);
        for (ZstdFrameDecoder decoder : List.of(nativeDecoder(), javaDecoder())) {
            byte[] dst = sentinelFilled(original.length + 40);
            assertEquals(original.length, decoder.decompress(src, 11, frame.length, dst, 17, original.length));
            assertArrayEquals(original, Arrays.copyOfRange(dst, 17, 17 + original.length));
            for (int i = 0; i < 17; i++) assertEquals(SENTINEL, dst[i]);
            assertSentinelsIntact(dst, 17 + original.length);
        }
    }

    @Test
    void edgeCasesMatchOnBothPaths() throws IOException {
        Random random = new Random(42);
        byte[] incompressible = new byte[100_000];
        random.nextBytes(incompressible);
        byte[] oneByteRun = new byte[300_000];
        Arrays.fill(oneByteRun, (byte) 'x');
        List<byte[]> inputs = List.of(new byte[]{'{', '}'}, incompressible, oneByteRun, new byte[1 << 20]);
        for (byte[] original : inputs) {
            byte[] frame = Zstd.compress(original, 3);
            for (ZstdFrameDecoder decoder : List.of(nativeDecoder(), javaDecoder())) {
                byte[] dst = sentinelFilled(original.length + 8);
                assertEquals(original.length, decoder.decompress(frame, frame.length, dst, original.length));
                assertArrayEquals(original, Arrays.copyOf(dst, original.length));
                assertSentinelsIntact(dst, original.length);
            }
        }
    }

    @Test
    void frameLargerThanDestinationIsMalformedAndNeverOverflows() throws IOException {
        byte[] original = esShapedBlock(ES_BEST_COMPRESSION_BLOCK, 3);
        byte[] frame = Zstd.compress(original, 3);
        for (ZstdFrameDecoder decoder : List.of(nativeDecoder(), javaDecoder())) {
            int dstLen = original.length - 1;
            byte[] dst = sentinelFilled(original.length + 64);
            assertThrows(ZstdFrameDecoder.MalformedFrameException.class,
                () -> decoder.decompress(frame, frame.length, dst, dstLen), "native=" + decoder.usesNative());
            assertSentinelsIntact(dst, dstLen);
        }
    }

    @Test
    void truncatedFramesNeverDecodeToTheFullLength() throws IOException {
        byte[] original = esShapedBlock(ES_BEST_COMPRESSION_BLOCK, 5);
        byte[] frame = Zstd.compress(original, 3);
        for (ZstdFrameDecoder decoder : List.of(nativeDecoder(), javaDecoder())) {
            for (int cut : new int[]{frame.length - 1, frame.length - 4, frame.length / 2, 10, 4, 1}) {
                byte[] dst = sentinelFilled(original.length + 16);
                try {
                    int written = decoder.decompress(frame, cut, dst, original.length);
                    assertNotEquals(original.length, written, "truncated at " + cut + ", native=" + decoder.usesNative());
                } catch (ZstdFrameDecoder.MalformedFrameException expected) {
                    // the usual outcome
                }
                assertSentinelsIntact(dst, original.length);
            }
        }
    }

    @Test
    void seededBitFlipsEitherDecodeOrThrowMalformedFrameOnly() throws IOException {
        Random random = new Random(20260927);
        int mutations = 0;
        for (int level : new int[]{1, 3, 9}) {
            byte[] original = esShapedBlock(ES_DEFAULT_BLOCK * 2, level);
            byte[] frame = Zstd.compress(original, level);
            ZstdFrameDecoder nativeDecoder = nativeDecoder();
            ZstdFrameDecoder javaDecoder = javaDecoder();
            for (int i = 0; i < 800; i++) {
                byte[] mutated = frame.clone();
                int flips = 1 + random.nextInt(3);
                for (int f = 0; f < flips; f++) {
                    mutated[random.nextInt(mutated.length)] ^= (byte) (1 << random.nextInt(8));
                }
                for (ZstdFrameDecoder decoder : List.of(nativeDecoder, javaDecoder)) {
                    byte[] dst = sentinelFilled(original.length + 32);
                    try {
                        int written = decoder.decompress(mutated, mutated.length, dst, original.length);
                        assertTrue(written >= 0 && written <= original.length);
                    } catch (ZstdFrameDecoder.MalformedFrameException expected) {
                        // corrupted input must surface only as this IOException
                    } catch (Throwable other) {
                        fail("level " + level + " mutation " + i + " native=" + decoder.usesNative()
                            + " escaped as " + other.getClass().getName(), other);
                    }
                    assertSentinelsIntact(dst, original.length);
                }
                mutations++;
            }
        }
        assertTrue(mutations >= 2000);
    }

    @Test
    void forcedJavaDecoderIsSelectedByTheSystemProperty() {
        assertFalse(javaDecoder().usesNative());
        assertTrue(ZstdFrameDecoder.nativeAvailable(), "zstd-jni loads on development machines");
        assertTrue(nativeDecoder().usesNative());
    }

    /**
     * A missing {@code ZstdNativePath} makes zstd-jni fail exactly like a noexec tmpdir does
     * (UnsatisfiedLinkError from System.load), deterministically and without Docker.
     */
    @Test
    void childJvmWithoutNativeLibraryFallsBackAndWarnsOnce(@TempDir Path tmp) throws Exception {
        byte[] original = esShapedBlock(ES_BEST_COMPRESSION_BLOCK, 11);
        Path frameFile = tmp.resolve("frame.zst");
        Files.write(frameFile, Zstd.compress(original, 3));
        Path originalFile = tmp.resolve("original.bin");
        Files.write(originalFile, original);

        Result auto = runChild(tmp, "auto", frameFile, originalFile);
        assertEquals(0, auto.exit(), auto.stderr());
        assertEquals("decoded ok native=false available=false", auto.stdout().strip());
        assertEquals(1, countWarnings(auto.stderr()), auto.stderr());
        assertTrue(auto.stderr().contains("(UnsatisfiedLinkError)"), auto.stderr());

        // Forced java never runs the probe: no warning until nativeAvailable() is asked.
        Result forced = runChild(tmp, "java", frameFile, originalFile);
        assertEquals(0, forced.exit(), forced.stderr());
        assertEquals("decoded ok native=false warnings-before-probe=0 available=false", forced.stdout().strip());
        assertEquals(1, countWarnings(forced.stderr()), forced.stderr());
    }

    private static int countWarnings(String stderr) {
        return (int) stderr.lines().filter(l -> l.startsWith("zstd-jni native library unavailable")).count();
    }

    private record Result(int exit, String stdout, String stderr) {}

    private static Result runChild(Path tmp, String mode, Path frame, Path original) throws Exception {
        List<String> command = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-DZstdNativePath=" + tmp.resolve("missing-libzstd-jni.so"),
            "-cp", System.getProperty("java.class.path")));
        if (mode.equals("java")) command.add("-D" + ZstdFrameDecoder.DECODER_PROPERTY + "=java");
        command.addAll(List.of(ChildMain.class.getName(), mode, frame.toString(), original.toString()));
        Path out = tmp.resolve(mode + ".out");
        Path err = tmp.resolve(mode + ".err");
        Process process = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile()).start();
        assertTrue(process.waitFor(60, TimeUnit.SECONDS));
        return new Result(process.exitValue(), Files.readString(out), Files.readString(err));
    }

    /** Runs in the child JVM. Decodes through several instances to show the probe runs once. */
    public static final class ChildMain {
        public static void main(String[] args) throws Exception {
            byte[] frame = Files.readAllBytes(Path.of(args[1]));
            byte[] original = Files.readAllBytes(Path.of(args[2]));
            java.io.ByteArrayOutputStream capturedErr = new java.io.ByteArrayOutputStream();
            java.io.PrintStream realErr = System.err;
            System.setErr(new java.io.PrintStream(new TeeStream(realErr, capturedErr), true));
            boolean usesNative = false;
            for (int i = 0; i < 3; i++) {
                ZstdFrameDecoder decoder = new ZstdFrameDecoder();
                usesNative |= decoder.usesNative();
                byte[] dst = new byte[original.length];
                if (decoder.decompress(frame, frame.length, dst, dst.length) != original.length
                        || !Arrays.equals(original, dst)) {
                    System.out.println("mismatch");
                    System.exit(2);
                }
            }
            String status = "decoded ok native=" + usesNative;
            if (args[0].equals("java")) {
                long before = capturedErr.toString(StandardCharsets.UTF_8).lines()
                    .filter(l -> l.startsWith("zstd-jni native library unavailable")).count();
                status += " warnings-before-probe=" + before;
            }
            System.out.println(status + " available=" + ZstdFrameDecoder.nativeAvailable());
        }
    }

    private static final class TeeStream extends java.io.OutputStream {
        private final java.io.OutputStream a;
        private final java.io.OutputStream b;

        TeeStream(java.io.OutputStream a, java.io.OutputStream b) {
            this.a = a;
            this.b = b;
        }

        @Override
        public void write(int value) throws IOException {
            a.write(value);
            b.write(value);
        }
    }

    static ZstdFrameDecoder nativeDecoder() {
        System.clearProperty(ZstdFrameDecoder.DECODER_PROPERTY);
        ZstdFrameDecoder decoder = new ZstdFrameDecoder();
        assertTrue(decoder.usesNative());
        return decoder;
    }

    static ZstdFrameDecoder javaDecoder() {
        System.setProperty(ZstdFrameDecoder.DECODER_PROPERTY, "java");
        try {
            return new ZstdFrameDecoder();
        } finally {
            System.clearProperty(ZstdFrameDecoder.DECODER_PROPERTY);
        }
    }

    private static byte[] sentinelFilled(int length) {
        byte[] dst = new byte[length];
        Arrays.fill(dst, SENTINEL);
        return dst;
    }

    private static void assertSentinelsIntact(byte[] dst, int from) {
        for (int i = from; i < dst.length; i++) {
            if (dst[i] != SENTINEL) fail("decoder wrote past its destination window at " + i);
        }
    }

    /** Concatenated ECS-like JSON documents, cut to exactly {@code size} bytes like a stored-field block. */
    static byte[] esShapedBlock(int size, long seed) {
        Random random = new Random(seed);
        String[] hosts = {"web-01.example.internal", "web-02.example.internal", "db-07.example.internal"};
        String[] paths = {"/api/v1/orders", "/api/v2/users/search", "/static/app.js", "/health"};
        StringBuilder block = new StringBuilder(size + 2048);
        int doc = 0;
        while (block.length() < size) {
            block.append("{\"@timestamp\":\"2026-09-").append(10 + random.nextInt(18)).append("T")
                .append(10 + random.nextInt(13)).append(':').append(10 + random.nextInt(49)).append(":00.")
                .append(100 + random.nextInt(899)).append("Z\",\"host\":{\"name\":\"")
                .append(hosts[random.nextInt(hosts.length)]).append("\"},\"url\":{\"path\":\"")
                .append(paths[random.nextInt(paths.length)]).append('/').append(random.nextInt(100_000))
                .append("\"},\"http\":{\"response\":{\"status_code\":").append(random.nextBoolean() ? 200 : 404)
                .append(",\"bytes\":").append(random.nextInt(1_000_000)).append("}},\"event\":{\"id\":\"")
                .append(Long.toHexString(random.nextLong())).append("\",\"sequence\":").append(doc++)
                .append("},\"message\":\"request served in ").append(random.nextInt(5000)).append(" ms\"}");
        }
        return Arrays.copyOf(block.toString().getBytes(StandardCharsets.UTF_8), size);
    }
}
