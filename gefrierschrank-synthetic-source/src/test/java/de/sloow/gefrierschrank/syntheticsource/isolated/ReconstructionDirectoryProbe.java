package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test helper that must be defined by the isolated class loader it manipulates (hence this
 * package): replaces that loader's memory-mapped directory factory with one whose file opens fail
 * like a mapping that ran out of address space. Called reflectively with JDK types only.
 */
public final class ReconstructionDirectoryProbe {

    private static final AtomicInteger MAPPING_ATTEMPTS = new AtomicInteger();

    private ReconstructionDirectoryProbe() {}

    /** {@code kind} "io" throws Lucene's IOException for a failed map, "oom" the JDK's OutOfMemoryError. */
    public static void failMapping(String kind) {
        MAPPING_ATTEMPTS.set(0);
        IsolatedReconstructionWorkerImpl.memoryMappedDirectories = path -> {
            MAPPING_ATTEMPTS.incrementAndGet();
            return new FilterDirectory(new MMapDirectory(path)) {
                @Override
                public IndexInput openInput(String name, IOContext context) throws IOException {
                    if (kind.equals("oom")) throw new OutOfMemoryError("Map failed");
                    throw new IOException("Map failed: injected mapping failure for " + name);
                }
            };
        };
    }

    public static int mappingAttempts() {
        return MAPPING_ATTEMPTS.get();
    }

    public static void restore() {
        IsolatedReconstructionWorkerImpl.memoryMappedDirectories = MMapDirectory::new;
    }

    /** Simple class name of the directory an open worker reads through. */
    public static String directoryOf(Object worker) throws ReflectiveOperationException {
        Field field = IsolatedReconstructionWorkerImpl.class.getDeclaredField("directory");
        field.setAccessible(true);
        return FilterDirectory.unwrap((Directory) field.get(worker)).getClass().getSimpleName();
    }
}
