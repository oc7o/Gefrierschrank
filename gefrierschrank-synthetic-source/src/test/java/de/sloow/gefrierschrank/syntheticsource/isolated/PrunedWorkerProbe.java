package de.sloow.gefrierschrank.syntheticsource.isolated;

import de.sloow.gefrierschrank.core.repo.PrunedFormatAccessException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Test helper of PrunedSegmentReadersTest, defined by the isolated loader: looks into an open
 * worker's guarded reader. JDK types only across the boundary.
 */
public final class PrunedWorkerProbe {

    private PrunedWorkerProbe() {}

    public static boolean tripped(Object worker) throws ReflectiveOperationException {
        return tripwire(worker).tripped();
    }

    /** Reads the postings of {@code field} on every leaf and swallows the guard's exception, as careless code would. */
    public static int swallowPostingsAccess(Object worker, String field) throws ReflectiveOperationException, IOException {
        int swallowed = 0;
        for (LeafReaderContext leaf : reader(worker).leaves()) {
            try {
                leaf.reader().terms(field);
            } catch (PrunedFormatAccessException expected) {
                swallowed++;
            }
        }
        return swallowed;
    }

    private static PrunedSegmentReaders.Tripwire tripwire(Object worker) throws ReflectiveOperationException {
        return (PrunedSegmentReaders.Tripwire) field(worker, "tripwire");
    }

    private static DirectoryReader reader(Object worker) throws ReflectiveOperationException {
        return (DirectoryReader) field(worker, "reader");
    }

    private static Object field(Object worker, String name) throws ReflectiveOperationException {
        Field field = IsolatedReconstructionWorkerImpl.class.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(worker);
    }
}
