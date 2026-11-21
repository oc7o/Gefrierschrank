package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * es7_no_source was created and snapshotted by ES 7.17.9 with {@code _source.enabled: false}.
 * Before the preflight, every run downloaded and CRC-checked the whole shard and quarantined it
 * with a misleading IndexFormatTooOldException; now the index fails from metadata alone.
 */
class SourceDisabledExportTest {

    private static final Path ES7_FEATURES = Path.of("../gefrierschrank-core/src/test/resources/fixtures/es7-features-repo");

    @Test
    void aSourceDisabledEs7IndexFailsBeforeAnyShardDataOrTableIsTouched(@TempDir Path tmp) throws Exception {
        var store = new RecordingBlobStore(new FileSystemBlobStore(ES7_FEATURES));
        var exporter = new FullIndexExporter(new RepositorySources(new RepositorySources.Source("primary", store)),
            new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
        Path table = tmp.resolve("es7_no_source");

        var failure = assertThrows(ExportPreflight.SourceDisabledException.class,
            () -> exporter.run("es7_no_source", table.toString(), RunReportListener.NO_OP));

        assertTrue(store.bytesRead.keySet().stream().noneMatch(key -> key.contains("/__")),
            "no shard data blob may be read: " + store.bytesRead.keySet());
        assertEquals(0, store.shardBytes(), "shard data bytes read");
        assertFalse(Files.exists(table), "neither the document table nor _export_metadata may be created");
        assertFalse(failure.getMessage().contains("es7_no_source"), failure.getMessage());
        assertFalse(failure.getMessage().contains("name"), failure.getMessage());
    }

    @Test
    void aStoredSourceIndexOfTheSameRepositoryStillExports(@TempDir Path tmp) throws Exception {
        var exporter = new FullIndexExporter(
            new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(ES7_FEATURES))),
            new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));

        RunReport report = exporter.run("es7_hard_deletes", tmp.resolve("es7_hard_deletes").toString(), RunReportListener.NO_OP);

        assertEquals(1, report.committedCount());
        assertEquals(27, report.rowsWrittenThisRun());
    }

    /** Records the bytes read per key; shard data blobs are the ones under a shard directory. */
    private static final class RecordingBlobStore implements BlobStore {
        final Map<String, AtomicLong> bytesRead = new ConcurrentHashMap<>();
        private final BlobStore delegate;

        RecordingBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        long shardBytes() {
            return bytesRead.entrySet().stream().filter(e -> e.getKey().matches("indices/[^/]+/\\d+/.*"))
                .mapToLong(e -> e.getValue().get()).sum();
        }

        @Override
        public Optional<BlobMetadata> stat(String key) {
            bytesRead.computeIfAbsent(key, k -> new AtomicLong());
            return delegate.stat(key);
        }

        @Override
        public InputStream read(String key) throws IOException {
            AtomicLong counter = bytesRead.computeIfAbsent(key, k -> new AtomicLong());
            return new FilterInputStream(delegate.read(key)) {
                @Override
                public int read() throws IOException {
                    int b = super.read();
                    if (b >= 0) counter.incrementAndGet();
                    return b;
                }

                @Override
                public int read(byte[] buffer, int offset, int length) throws IOException {
                    int n = super.read(buffer, offset, length);
                    if (n > 0) counter.addAndGet(n);
                    return n;
                }
            };
        }

        @Override
        public List<String> list(String prefix) {
            return delegate.list(prefix);
        }

        @Override
        public List<BlobMetadata> listWithSizes(String prefix) {
            return delegate.listWithSizes(prefix);
        }

        @Override
        public void put(String key, byte[] content) {
            throw new UnsupportedOperationException("the source repository is read-only");
        }
    }
}
