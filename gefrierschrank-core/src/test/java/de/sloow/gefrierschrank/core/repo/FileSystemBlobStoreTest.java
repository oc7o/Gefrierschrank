package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.*;

/** Runs the BlobStore contract against the filesystem implementation. */
class FileSystemBlobStoreTest extends BlobStoreContractTest {

    @Override
    protected BlobStore create(String root) {
        return new FileSystemBlobStore(Path.of(root));
    }

    @Test
    void aFileReplacedBySameSizeContentWithTheSameModificationTimeIsANewVersion(@TempDir Path dir) throws IOException {
        Path blob = dir.resolve("r.bin");
        Files.writeString(blob, "0123456789");
        var store = new FileSystemBlobStore(dir);
        String version;
        try (var first = store.openRange("r.bin", 0, 5, null)) {
            version = first.version();
        }
        Path replacement = Files.writeString(dir.resolve("r.tmp"), "abcdefghij");
        Files.setLastModifiedTime(replacement, Files.getLastModifiedTime(blob));
        Files.move(replacement, blob, REPLACE_EXISTING, ATOMIC_MOVE);
        var failure = assertThrows(IOException.class, () -> readRange(store, "r.bin", 5, 5, version));
        assertEquals(BlobStore.BLOB_CHANGED, failure.getMessage());
    }

    /**
     * A file replaced while a range opens must never be served under the other file's version, or
     * ranges of two files could be stitched together. Each replacement is a new file that stays
     * linked, so no inode number, and therefore no version, is reused.
     */
    @Test
    void rangesPinnedToAVersionNeverReturnAReplacementsBytes(@TempDir Path dir) throws Exception {
        int size = 64;
        Path blob = dir.resolve("r.bin");
        Files.write(blob, generation(0, size));
        var store = new FileSystemBlobStore(dir);
        var stop = new AtomicBoolean();
        var replacements = new AtomicInteger();
        var replacer = Thread.ofPlatform().start(() -> {
            try {
                for (int g = 1; !stop.get() && g <= 300; g++) {
                    Path kept = Files.write(dir.resolve("g" + g), generation(g, size));
                    Path link = Files.createLink(dir.resolve("link"), kept);
                    Files.move(link, blob, REPLACE_EXISTING, ATOMIC_MOVE);
                    replacements.incrementAndGet();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        int pinnedPairs = 0;
        try {
            while (replacer.isAlive()) {
                byte[] first;
                String version;
                try (var range = store.openRange("r.bin", 0, size, null)) {
                    first = range.in().readAllBytes();
                    version = range.version();
                } catch (IOException changed) {
                    assertEquals(BlobStore.BLOB_CHANGED, changed.getMessage());
                    continue;
                }
                try (var pinned = store.openRange("r.bin", 0, size, version)) {
                    assertArrayEquals(first, pinned.in().readAllBytes(), "bytes of another file under version " + version);
                    pinnedPairs++;
                } catch (IOException changed) {
                    assertEquals(BlobStore.BLOB_CHANGED, changed.getMessage());
                }
            }
        } finally {
            stop.set(true);
            replacer.join();
        }
        assertEquals(300, replacements.get());
        assertTrue(pinnedPairs > 100, pinnedPairs + " pinned pairs");
    }

    private static byte[] generation(int g, int size) {
        byte[] content = new byte[size];
        Arrays.fill(content, (byte) (g % 251));
        return content;
    }

    /** The interface default serves ranges by skipping, and cannot pin a version. */
    @Test
    void defaultOpenRangeSkipsToTheOffsetAndReadsExactlyTheLength(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("r.bin"), "0123456789");
        var delegate = new FileSystemBlobStore(dir);
        BlobStore store = new BlobStore() {
            public Optional<BlobMetadata> stat(String key) { return delegate.stat(key); }
            public InputStream read(String key) throws IOException { return delegate.read(key); }
            public List<String> list(String prefix) { return delegate.list(prefix); }
            public List<BlobMetadata> listWithSizes(String prefix) { return delegate.listWithSizes(prefix); }
            public void put(String key, byte[] content) { delegate.put(key, content); }
        };
        try (var range = store.openRange("r.bin", 4, 3, null)) {
            assertEquals("456", new String(range.in().readAllBytes()));
            assertNull(range.version());
            assertEquals(-1, range.objectLength());
        }
        assertEquals("9", readRange(store, "r.bin", 9, 1, null));
        assertThrows(IOException.class, () -> readRange(store, "r.bin", 10, 1, null));
        assertThrows(IOException.class, () -> readRange(store, "r.bin", 8, 5, null));
    }
}
