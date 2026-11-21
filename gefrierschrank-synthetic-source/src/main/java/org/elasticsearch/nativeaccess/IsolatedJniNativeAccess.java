package org.elasticsearch.nativeaccess;

import com.github.luben.zstd.ZstdException;
import de.sloow.gefrierschrank.core.codec.ZstdFrameDecoder;
import org.elasticsearch.nativeaccess.lib.ZstdLibrary;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Project-written (not vendored) {@link NativeAccess} for the isolated Elasticsearch world: zstd
 * works, everything else behaves exactly like {@link NoopNativeAccess}.
 *
 * <p>ES 9.3's {@code ES819TSDBDocValuesFormat} compresses BINARY doc values (wildcard, histogram,
 * binary, range, counted_keyword and ignored-above keyword values in logsdb/TSDB indices) with
 * {@code ZstdCompressionMode}, which decompresses through {@code NativeAccess.instance().getZstd()}.
 * Standalone, ES cannot find its bundled libzstd and falls back to {@code NoopNativeAccess}, whose
 * {@code getZstd()} is {@code null}, so reading the first binary doc value of such a shard would
 * throw a NullPointerException. {@code NativeAccessHolder} has no hook to
 * choose a provider, so {@code IsolatedEsClassLoader} defines its own holder class whose
 * {@code INSTANCE} is this class (see {@code IsolatedNativeAccessHolder}). It lives in ES's package
 * to reach the package-private {@link Zstd} constructor and {@link NoopNativeAccess}; the
 * {@code org.elasticsearch.} prefix makes the isolated loader define it, never the ambient one.
 *
 * <p>Buffers are plain heap buffers with a no-op close: no Arena (still preview on JDK 21) and no
 * native memory that could leak when a per-shard loader is dropped.
 */
final class IsolatedJniNativeAccess implements NativeAccess {

    private final NativeAccess noop = new NoopNativeAccess();
    private final Zstd zstd = new Zstd(new JniZstdLibrary());

    @Override
    public boolean definitelyRunningAsRoot() {
        return noop.definitelyRunningAsRoot();
    }

    @Override
    public ProcessLimits getProcessLimits() {
        return noop.getProcessLimits();
    }

    @Override
    public void tryLockMemory() {
        noop.tryLockMemory();
    }

    @Override
    public boolean isMemoryLocked() {
        return noop.isMemoryLocked();
    }

    @Override
    public void tryInstallExecSandbox() {
        noop.tryInstallExecSandbox();
    }

    @Override
    public ExecSandboxState getExecSandboxState() {
        return noop.getExecSandboxState();
    }

    @Override
    public Systemd systemd() {
        return noop.systemd();
    }

    @Override
    public OptionalLong allocatedSizeInBytes(Path path) {
        return noop.allocatedSizeInBytes(path);
    }

    @Override
    public void tryPreallocate(Path path, long size) {
        noop.tryPreallocate(path, size);
    }

    @Override
    public Optional<VectorSimilarityFunctions> getVectorSimilarityFunctions() {
        return noop.getVectorSimilarityFunctions();
    }

    @Override
    public Zstd getZstd() {
        return zstd;
    }

    @Override
    public CloseableByteBuffer newSharedBuffer(int len) {
        return new HeapBuffer(ByteBuffer.allocate(len));
    }

    @Override
    public CloseableByteBuffer newConfinedBuffer(int len) {
        return new HeapBuffer(ByteBuffer.allocate(len));
    }

    private record HeapBuffer(ByteBuffer buffer) implements CloseableByteBuffer {
        @Override
        public void close() {}
    }

    /**
     * Mirrors ES's {@code JdkZstdLibrary} contract: operates on {@code [position, position+remaining)}
     * of each buffer without moving either position (ES reads results back with absolute or
     * position-relative gets from the original position), bounds output by {@code dst.remaining()},
     * and never returns an error code as a size. Errors are thrown as
     * {@link IllegalArgumentException}, the type ES's {@link Zstd} wrapper throws for error codes: a
     * zstd error enum returned as a "size" could equal the expected length and be accepted silently.
     */
    static final class JniZstdLibrary implements ZstdLibrary {

        // One decoder per thread: the pure-Java fallback keeps per-instance tables. Values are
        // ambient classes, so a thread's map never pins this isolated loader.
        private final ThreadLocal<ZstdFrameDecoder> decoders = ThreadLocal.withInitial(ZstdFrameDecoder::new);

        @Override
        public long compressBound(int srcLen) {
            // ZSTD_COMPRESSBOUND, computed here so the read path never needs the native library.
            long size = srcLen;
            return size + (size >>> 8) + (size < (128 << 10) ? ((128 << 10) - size) >>> 11 : 0);
        }

        /** Only reached by tests that write ES819 segments; reconstruction never compresses. */
        @Override
        public long compress(CloseableByteBuffer dst, CloseableByteBuffer src, int level) {
            ByteBuffer d = arrayBacked(dst.buffer());
            ByteBuffer s = arrayBacked(src.buffer());
            try {
                return com.github.luben.zstd.Zstd.compressByteArray(d.array(), d.arrayOffset() + d.position(), d.remaining(),
                    s.array(), s.arrayOffset() + s.position(), s.remaining(), level);
            } catch (ZstdException e) {
                throw new IllegalArgumentException("zstd compression failed", e);
            }
        }

        @Override
        public boolean isError(long code) {
            // ZSTD_isError: codes are the last ZSTD_error_maxCode (120) values of size_t. The
            // methods above never return one; kept exact for completeness.
            return Long.compareUnsigned(code, -120L) > 0;
        }

        @Override
        public String getErrorName(long code) {
            return "zstd error " + (-code);
        }

        @Override
        public long decompress(CloseableByteBuffer dst, CloseableByteBuffer src) {
            ByteBuffer d = arrayBacked(dst.buffer());
            ByteBuffer s = arrayBacked(src.buffer());
            try {
                return decoders.get().decompress(s.array(), s.arrayOffset() + s.position(), s.remaining(),
                    d.array(), d.arrayOffset() + d.position(), d.remaining());
            } catch (IOException e) {
                throw new IllegalArgumentException("zstd decompression failed", e);
            }
        }

        private static ByteBuffer arrayBacked(ByteBuffer buffer) {
            if (!buffer.hasArray() || buffer.isReadOnly()) {
                throw new IllegalStateException("zstd buffers must be writable heap buffers");
            }
            return buffer;
        }
    }
}
