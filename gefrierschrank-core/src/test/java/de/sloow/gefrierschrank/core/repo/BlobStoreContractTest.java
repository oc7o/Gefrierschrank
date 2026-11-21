package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** Contract tests every BlobStore implementation must satisfy. */
abstract class BlobStoreContractTest {

    /** Root as a string: filesystem path or object-store key prefix. */
    protected abstract BlobStore create(String root) throws IOException;

    @TempDir
    Path tempDir;

    protected String root() {
        return tempDir.toString();
    }

    @Test
    void statReturnsMetadataForExistingBlob() throws IOException {
        BlobStore store = create(root());
        write(store, "a/b.txt", "hello world");
        Optional<BlobMetadata> meta = store.stat("a/b.txt");
        assertTrue(meta.isPresent());
        assertEquals(11, meta.get().size());
    }

    @Test
    void statEmptyForMissingBlob() throws IOException {
        BlobStore store = create(root());
        assertTrue(store.stat("nope").isEmpty());
    }

    @Test
    void readReturnsFullContent() throws IOException {
        BlobStore store = create(root());
        write(store, "x.bin", "äöü bytes");
        assertEquals("äöü bytes", new String(store.read("x.bin").readAllBytes()));
    }

    @Test
    void readThrowsForMissingBlob() throws IOException {
        BlobStore store = create(root());
        assertThrows(IOException.class, () -> store.read("missing"));
    }

    @Test
    void listReturnsSortedKeysUnderPrefix() throws IOException {
        BlobStore store = create(root());
        write(store, "indices/abc/0/__file1", "a");
        write(store, "indices/abc/0/__file2", "b");
        write(store, "indices/abc/0/snap-x.dat", "c");
        write(store, "indices/abc/meta-y.dat", "d");
        write(store, "index-1", "e");

        assertEquals(List.of(
            "indices/abc/0/__file1",
            "indices/abc/0/__file2",
            "indices/abc/0/snap-x.dat"
        ), store.list("indices/abc/0/"));
    }

    @Test
    void openRangeReturnsExactBytesAtTheStartAMiddleOffsetAndTheLastByte() throws IOException {
        BlobStore store = create(root());
        write(store, "r.bin", "0123456789");
        assertEquals("012", readRange(store, "r.bin", 0, 3, null));
        assertEquals("456", readRange(store, "r.bin", 4, 3, null));
        assertEquals("9", readRange(store, "r.bin", 9, 1, null));
        assertEquals("0123456789", readRange(store, "r.bin", 0, 10, null));
        try (var range = store.openRange("r.bin", 2, 2, null)) {
            if (range.objectLength() >= 0) assertEquals(10, range.objectLength());
        }
    }

    @Test
    void openRangeOutsideTheBlobIsAnError() throws IOException {
        BlobStore store = create(root());
        write(store, "r.bin", "0123456789");
        assertThrows(IOException.class, () -> readRange(store, "r.bin", 10, 1, null));
        assertThrows(IOException.class, () -> readRange(store, "r.bin", 8, 5, null));
        assertThrows(IOException.class, () -> readRange(store, "missing", 0, 1, null));
        assertThrows(IllegalArgumentException.class, () -> store.openRange("r.bin", 0, 0, null));
    }

    @Test
    void openRangeRefusesToStitchDifferentBlobVersions() throws IOException {
        BlobStore store = create(root());
        write(store, "r.bin", "0123456789");
        String version;
        try (var first = store.openRange("r.bin", 0, 5, null)) {
            assertEquals("01234", new String(first.in().readAllBytes()));
            version = first.version();
        }
        assertNotNull(version, "this store must be able to pin ranges to one blob version");
        assertEquals("56789", readRange(store, "r.bin", 5, 5, version));
        write(store, "r.bin", "abcdefghijklmnop");
        assertThrows(IOException.class, () -> readRange(store, "r.bin", 5, 5, version));
    }

    static String readRange(BlobStore store, String key, long offset, long length, String version) throws IOException {
        try (var range = store.openRange(key, offset, length, version)) {
            return new String(range.in().readAllBytes());
        }
    }

    private void write(BlobStore store, String key, String content) throws IOException {
        if (store instanceof FileSystemBlobStore fs) {
            Path file = Path.of(root()).resolve(key);
            Files.createDirectories(file.getParent());
            Files.writeString(file, content);
        } else {
            store.put(key, content.getBytes());
        }
    }
}
