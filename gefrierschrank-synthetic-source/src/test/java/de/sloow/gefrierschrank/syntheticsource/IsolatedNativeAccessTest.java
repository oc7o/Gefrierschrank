package de.sloow.gefrierschrank.syntheticsource;

import com.github.luben.zstd.Zstd;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The isolated world's {@code NativeAccess} has working zstd (ES 9.3 ES819 binary doc values need
 * it) and otherwise behaves like ES's {@code NoopNativeAccess}; the ambient world keeps ES's own
 * holder class untouched.
 */
class IsolatedNativeAccessTest {

    private static final String HOLDER = "org.elasticsearch.nativeaccess.NativeAccessHolder";
    private static final String PROBE = "de.sloow.gefrierschrank.syntheticsource.isolated.Es819BinaryDocValuesProbe";

    @Test
    void isolatedNativeAccessHasZstdAndNoopParity() throws Exception {
        try (IsolatedEsClassLoader loader = IsolatedEsClassLoader.create()) {
            call(loader, "bootstrap");
            @SuppressWarnings("unchecked")
            List<String> facts = (List<String>) call(loader, "nativeAccessFacts");
            assertEquals(List.of(
                "class=org.elasticsearch.nativeaccess.IsolatedJniNativeAccess",
                "definedByThisLoader=true",
                "holderLoader=true",
                "zstd=true",
                "vectorSimilarityFunctions=false",
                "execSandboxState=NONE",
                "definitelyRunningAsRoot=false",
                "isMemoryLocked=false"), facts);
        }
    }

    @Test
    void ambientHolderIsElasticsearchsOwnClass() throws Exception {
        ClassLoader ambient = IsolatedNativeAccessTest.class.getClassLoader();
        Class<?> ambientHolder = Class.forName(HOLDER, false, ambient);
        String location = ambientHolder.getProtectionDomain().getCodeSource().getLocation().toString();
        assertTrue(location.contains("elasticsearch-native"), location);
        List<URL> copies = Collections.list(ambient.getResources(HOLDER.replace('.', '/') + ".class"));
        assertEquals(1, copies.size(), "no second NativeAccessHolder.class may exist on the class path: " + copies);

        try (IsolatedEsClassLoader loader = IsolatedEsClassLoader.create()) {
            Class<?> isolatedHolder = Class.forName(HOLDER, false, loader);
            assertNotSame(ambientHolder, isolatedHolder);
            assertSame(loader, isolatedHolder.getClassLoader());
            var codeSource = isolatedHolder.getProtectionDomain().getCodeSource();
            assertTrue(codeSource == null || codeSource.getLocation() == null,
                "the isolated holder is defined from emitted bytes, not loaded from a jar: " + codeSource);
        }
    }

    @Test
    void twoLoadersGetDistinctHoldersAndClosingOneLeavesTheOtherDecoding() throws Exception {
        byte[] original = esShapedBytes(50_000, 3);
        byte[] block = block(Zstd.compress(original, 3));
        IsolatedEsClassLoader first = IsolatedEsClassLoader.create();
        try (IsolatedEsClassLoader second = IsolatedEsClassLoader.create()) {
            call(first, "bootstrap");
            call(second, "bootstrap");
            Class<?> firstHolder = Class.forName(HOLDER, false, first);
            Class<?> secondHolder = Class.forName(HOLDER, false, second);
            assertNotSame(firstHolder, secondHolder);
            Object firstInstance = instance(first);
            Object secondInstance = instance(second);
            assertNotSame(firstInstance, secondInstance);
            assertNotSame(firstInstance.getClass(), secondInstance.getClass());

            assertArrayEquals(original, esDecompress(first, block, original.length, 0, original.length));
            first.close();
            assertArrayEquals(original, esDecompress(second, block, original.length, 0, original.length));
        } finally {
            first.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 4096, 14336, 131072, 245760, 262145})
    void elasticsearchZstdCompressionModeRoundTripsByteExact(int size) throws Exception {
        byte[] original = esShapedBytes(size, size);
        try (IsolatedEsClassLoader loader = IsolatedEsClassLoader.create()) {
            call(loader, "bootstrap");
            List<byte[]> blocks = List.of(
                block(Zstd.compress(original, 1)),
                block(Zstd.compress(original, 3)),
                (byte[]) call(loader, "esCompress", original, 1),
                (byte[]) call(loader, "esCompress", original, 3));
            for (byte[] block : blocks) {
                int mid = size / 2;
                assertArrayEquals(original, esDecompress(loader, block, size, 0, size));
                assertArrayEquals(Arrays.copyOfRange(original, mid, mid + (size - mid) / 2),
                    esDecompress(loader, block, size, mid, (size - mid) / 2));
                assertArrayEquals(new byte[0], esDecompress(loader, block, size, size, 0));
            }
        }
    }

    @Test
    void corruptOrMisdeclaredFramesFailLoudly() throws Exception {
        byte[] original = new byte[100];
        for (int i = 0; i < original.length; i++) original[i] = (byte) (i * 31 + 7);
        byte[] frame = Zstd.compress(original, 1);
        byte[] block = block(frame);
        try (IsolatedEsClassLoader loader = IsolatedEsClassLoader.create()) {
            call(loader, "bootstrap");
            assertArrayEquals(original, esDecompress(loader, block, 100, 0, 100));

            byte[] badMagic = block.clone();
            badMagic[block.length - frame.length] ^= 0x5A;
            assertFailure(loader, badMagic, 100, "java.lang.IllegalArgumentException");
            assertFailure(loader, block(Arrays.copyOf(frame, frame.length - 3)), 100, null);
            // A zstd-jni error code must never pass as a size: dstSize_tooSmall is 70.
            assertFailure(loader, block, 70, "java.lang.IllegalArgumentException");
            assertFailure(loader, block, 20, "java.lang.IllegalArgumentException");
            assertFailure(loader, block, 130, "org.apache.lucene.index.CorruptIndexException");
        }
    }

    /**
     * Writes a segment with ES's own ES819 writer (ZSTD binary blocks) and reads it back. The same
     * read in a loader without the holder substitution fails with a NullPointerException, which
     * proves the probe really decodes ZSTD blocks.
     */
    @Test
    void es819ZstdBinaryDocValuesRoundTripOnlyWithTheSubstitution(@TempDir Path tmp) throws Exception {
        int docs = 5000;
        try (IsolatedEsClassLoader loader = IsolatedEsClassLoader.create()) {
            call(loader, "bootstrap");
            call(loader, "write", tmp, docs);
            String result = (String) call(loader, "read", tmp, docs);
            int present = 0;
            for (int doc = 0; doc < docs; doc++) if (doc % 7 != 3) present++;
            assertEquals("checked=" + present + " url=ES819TSDB/BINARY", result);
        }

        URL[] urls;
        try (IsolatedEsClassLoader loader = IsolatedEsClassLoader.create()) {
            urls = loader.getURLs();
        }
        try (URLClassLoader plain = new ChildFirstWithoutSubstitution(urls)) {
            call(plain, "initLoggingOnly");
            Throwable failure = assertThrows(InvocationTargetException.class,
                () -> call(plain, "read", tmp, docs)).getCause();
            assertEquals(NullPointerException.class, failure.getClass());
            assertEquals("org.elasticsearch.index.codec.zstd.ZstdCompressionMode$ZstdDecompressor",
                failure.getStackTrace()[0].getClassName());

            Throwable guard = assertThrows(InvocationTargetException.class, () -> call(plain, "bootstrap")).getCause();
            assertEquals(IllegalStateException.class, guard.getClass());
            assertTrue(guard.getMessage().contains("NativeAccessHolder substitution did not apply"), guard.getMessage());
        }
    }

    private static void assertFailure(ClassLoader loader, byte[] block, int originalLength, String expectedClass) {
        InvocationTargetException failure = assertThrows(InvocationTargetException.class,
            () -> call(loader, "esDecompress", block, originalLength, 0, Math.min(10, originalLength)));
        if (expectedClass != null) {
            assertEquals(expectedClass, failure.getCause().getClass().getName(), String.valueOf(failure.getCause()));
        }
    }

    private static Object instance(ClassLoader loader) throws Exception {
        Class<?> nativeAccess = Class.forName("org.elasticsearch.nativeaccess.NativeAccess", true, loader);
        return nativeAccess.getMethod("instance").invoke(null);
    }

    private static byte[] esDecompress(ClassLoader loader, byte[] block, int originalLength, int offset, int length)
            throws Exception {
        return (byte[]) call(loader, "esDecompress", block, originalLength, offset, length);
    }

    static Object call(ClassLoader loader, String method, Object... args) throws Exception {
        Class<?> probe = Class.forName(PROBE, true, loader);
        for (Method candidate : probe.getMethods()) {
            if (candidate.getName().equals(method) && candidate.getParameterCount() == args.length) {
                Thread thread = Thread.currentThread();
                ClassLoader previous = thread.getContextClassLoader();
                thread.setContextClassLoader(loader);
                try {
                    return candidate.invoke(null, args);
                } finally {
                    thread.setContextClassLoader(previous);
                }
            }
        }
        throw new NoSuchMethodException(method);
    }

    private static byte[] block(byte[] frame) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int value = frame.length;
        while ((value & ~0x7F) != 0) {
            out.write((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.write(value);
        out.writeBytes(frame);
        return out.toByteArray();
    }

    private static byte[] esShapedBytes(int size, long seed) {
        Random random = new Random(seed);
        StringBuilder text = new StringBuilder(size + 256);
        while (text.length() < size) {
            text.append("{\"@timestamp\":\"2026-09-27T12:").append(10 + random.nextInt(49))
                .append(":00Z\",\"url\":{\"path\":\"/api/v1/items/").append(random.nextInt(100_000))
                .append("\"},\"status\":").append(random.nextBoolean() ? 200 : 503).append("}\n");
        }
        return Arrays.copyOf(text.toString().getBytes(StandardCharsets.UTF_8), size);
    }

    /** Child-first like IsolatedEsClassLoader, but defines ES's own NativeAccessHolder (negative control). */
    private static final class ChildFirstWithoutSubstitution extends URLClassLoader {
        ChildFirstWithoutSubstitution(URL[] urls) {
            super(urls, IsolatedNativeAccessTest.class.getClassLoader());
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            synchronized (getClassLoadingLock(name)) {
                Class<?> c = findLoadedClass(name);
                if (c == null) {
                    if (name.startsWith("org.apache.lucene.") || name.startsWith("org.elasticsearch.")
                            || name.startsWith("de.sloow.gefrierschrank.syntheticsource.isolated.")) {
                        try {
                            c = findClass(name);
                        } catch (ClassNotFoundException e) {
                            c = super.loadClass(name, false);
                        }
                    } else {
                        c = super.loadClass(name, false);
                    }
                }
                if (resolve) resolveClass(c);
                return c;
            }
        }
    }
}
