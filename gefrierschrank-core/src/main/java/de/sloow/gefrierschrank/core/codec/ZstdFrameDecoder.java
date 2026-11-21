package de.sloow.gefrierschrank.core.codec;

import com.github.luben.zstd.Zstd;
import io.airlift.compress.zstd.ZstdDecompressor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Decodes one zstd frame from a byte array into a byte array, through zstd-jni's native library
 * when it can be loaded and through aircompressor's pure-Java decoder otherwise.
 *
 * <p>zstd-jni extracts its native library into {@code java.io.tmpdir} on first use. On hosts
 * where that directory is mounted {@code noexec} (hardened Linux, Docker {@code --tmpfs}, read-only
 * images) the load fails with an {@link Error} that no per-shard {@code catch (Exception)} can
 * contain, so the first ZSTD block used to kill the whole run. The native library is therefore
 * probed once per process, lazily on the first ZSTD block, and every failure - native or pure
 * Java - surfaces as an {@link IOException}. {@code -Dgefrierschrank.zstd.decoder=java} skips the
 * probe and never touches zstd-jni.
 *
 * <p>Both decoders produce identical bytes for valid frames. Elasticsearch writes its frames
 * without a checksum, so neither decoder detects every corruption; the repository CRC32 check
 * during shard materialization remains the corruption guard.
 *
 * <p>Instances are not thread-safe (the pure-Java decoder keeps per-instance tables). The API
 * uses only {@code byte[]} and JDK types so the isolated Elasticsearch class loader delegates this
 * class to the application loader and both worlds share one probe.
 */
public final class ZstdFrameDecoder {

    /** System property selecting the decoder; {@code java} forces the pure-Java decoder. */
    public static final String DECODER_PROPERTY = "gefrierschrank.zstd.decoder";

    private final boolean useNative;
    private ZstdDecompressor javaDecoder;

    public ZstdFrameDecoder() {
        this.useNative = !"java".equals(System.getProperty(DECODER_PROPERTY)) && NativeProbe.AVAILABLE;
    }

    /** Whether zstd-jni's native library loaded in this process. Runs the probe on first call. */
    public static boolean nativeAvailable() {
        return NativeProbe.AVAILABLE;
    }

    /** Whether this instance decodes through zstd-jni. */
    public boolean usesNative() {
        return useNative;
    }

    /**
     * Decodes the frame in {@code src[srcOff, srcOff+srcLen)} into {@code dst[dstOff, dstOff+dstLen)}
     * and returns the number of bytes written. Never writes outside that destination window.
     *
     * @throws MalformedFrameException if the input is not a decodable frame or its content does
     *                                 not fit into {@code dstLen} bytes
     * @throws IOException             if no decoder can run in this JVM
     */
    public int decompress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff, int dstLen) throws IOException {
        if (useNative) {
            long written;
            try {
                written = Zstd.decompressByteArray(dst, dstOff, dstLen, src, srcOff, srcLen);
            } catch (RuntimeException e) {
                throw new MalformedFrameException(e);
            }
            if (written < 0 || written > dstLen) {
                throw new MalformedFrameException(null);
            }
            return (int) written;
        }
        try {
            if (javaDecoder == null) {
                javaDecoder = new ZstdDecompressor();
            }
            return javaDecoder.decompress(src, srcOff, srcLen, dst, dstOff, dstLen);
        } catch (RuntimeException e) {
            // Not only MalformedInputException: aircompressor also throws e.g.
            // ArrayIndexOutOfBoundsException on some corrupted frames.
            throw new MalformedFrameException(e);
        } catch (LinkageError e) {
            // aircompressor relies on sun.misc.Unsafe; a JDK that denies it must fail this shard,
            // not the JVM.
            throw new IOException("no usable zstd decoder", e);
        }
    }

    /** Same as {@link #decompress(byte[], int, int, byte[], int, int)} with zero offsets. */
    public int decompress(byte[] src, int srcLen, byte[] dst, int dstLen) throws IOException {
        return decompress(src, 0, srcLen, dst, 0, dstLen);
    }

    /** The input is not a zstd frame whose content fits the destination. The message is fixed. */
    public static final class MalformedFrameException extends IOException {
        MalformedFrameException(Throwable cause) {
            super("malformed zstd frame", cause);
        }
    }

    /** Lazy holder: LZ4-only runs never load zstd-jni. */
    private static final class NativeProbe {
        static final boolean AVAILABLE = probe();

        private static boolean probe() {
            try {
                byte[] sample = "gefrierschrank zstd-jni probe ".repeat(8).getBytes(StandardCharsets.US_ASCII);
                byte[] frame = Zstd.compress(sample, 1);
                byte[] decoded = Zstd.decompress(frame, sample.length);
                if (!Arrays.equals(sample, decoded)) {
                    throw new IllegalStateException("zstd-jni round trip mismatch");
                }
                return true;
            } catch (LinkageError | RuntimeException e) {
                // LinkageError: UnsatisfiedLinkError (noexec tmpdir, missing ZstdNativePath),
                // ExceptionInInitializerError ("Cannot unpack"), NoClassDefFoundError after either.
                // RuntimeException: IllegalCallerException/SecurityException where JNI is denied.
                System.err.println("zstd-jni native library unavailable (" + e.getClass().getSimpleName()
                    + "); decoding ZSTD stored fields with the slower pure-Java decoder; if java.io.tmpdir"
                    + " is mounted noexec pass -DZstdTempFolder=<exec-capable dir>");
                return false;
            }
        }
    }
}
