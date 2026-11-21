package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.repo.BlobMetadata;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The vector plan must stay ahead of shard dispatch once a work pool exists: at --parallelism 4 an
 * index whose vectors are rehydrated completes exactly as at parallelism 1, and one whose exclusion
 * is refused (here by the kill switch) still fails before any shard blob is read and before any
 * table is created.
 */
class ExcludedSourceVectorsParallelExportTest {

    private static final Path FIXTURES = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    private static final String INDEX = "test_es93_vectors_default";

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void rehydratedVectorsCompleteAtEveryParallelism(int parallelism, @TempDir Path tmp) throws Exception {
        var result = VectorExportJvm.export(FIXTURES.resolve("es93-repo"), tmp, List.of(INDEX), parallelism, 1,
            java.util.Map.of());
        var report = result.reports().get(INDEX + "#1");
        assertEquals(1, report.path("committedCount").asInt(), report.toString());
        assertEquals(20, report.path("rowsWrittenThisRun").asLong(), report.toString());
        assertTrue(VectorExportJvm.rows(tmp.resolve(INDEX)).values().stream()
            .allMatch(row -> row.kind().equals("stored+vectors")));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void refusedVectorsFailTheIndexBeforeAnyShardDataOrTableIsTouched(int parallelism, @TempDir Path tmp) {
        var store = new RecordingBlobStore(new FileSystemBlobStore(FIXTURES.resolve("es93-repo")));
        var sources = new RepositorySources(new RepositorySources.Source("primary", store));
        var exporter = new FullIndexExporter(sources, new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
        exporter.configure(new ExportSettings(SnapshotSelection.DEFAULT, parallelism, 0));
        Path table = tmp.resolve(INDEX);

        String previous = System.getProperty(SourceVectorsPlan.REHYDRATE_PROPERTY);
        System.setProperty(SourceVectorsPlan.REHYDRATE_PROPERTY, "false");
        IOException failure;
        try {
            failure = assertThrows(IOException.class, () -> exporter.run(INDEX, table.toString(), RunReportListener.NO_OP));
        } finally {
            if (previous == null) System.clearProperty(SourceVectorsPlan.REHYDRATE_PROPERTY);
            else System.setProperty(SourceVectorsPlan.REHYDRATE_PROPERTY, previous);
        }

        assertEquals("ExcludedSourceVectorsException", failure.getClass().getSimpleName());
        assertTrue(store.touched.stream().noneMatch(key -> key.contains("/__")),
            "no shard data blob may be read: " + store.touched);
        assertFalse(Files.exists(table), "neither the document table nor _export_metadata may be created");
    }

    /** Records every key whose content or metadata is requested. */
    private static final class RecordingBlobStore implements BlobStore {
        final List<String> touched = new CopyOnWriteArrayList<>();
        private final BlobStore delegate;

        RecordingBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<BlobMetadata> stat(String key) {
            touched.add(key);
            return delegate.stat(key);
        }

        @Override
        public InputStream read(String key) throws IOException {
            touched.add(key);
            return delegate.read(key);
        }

        @Override
        public RangeRead openRange(String key, long offset, long length, String expectedVersion) throws IOException {
            touched.add(key);
            return delegate.openRange(key, offset, length, expectedVersion);
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
