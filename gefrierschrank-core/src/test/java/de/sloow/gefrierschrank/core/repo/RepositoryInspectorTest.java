package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Repository inspection against a fixture repo generated from ES 7.17.9, 8.17.0 and 9.3.0
 * sharing one MinIO bucket (so index-N contains all three snapshots).
 */
class RepositoryInspectorTest {

    private static RepositoryInspector inspector;

    @BeforeAll
    static void setup() {
        var store = new FileSystemBlobStore(Path.of("src/test/resources/fixtures/demo-repo"));
        inspector = new RepositoryInspector(store);
    }

    @Test
    void readsLatestGeneration() throws IOException {
        RepositoryOverview overview = inspector.overview();
        assertEquals(2, overview.latestGeneration());
        assertEquals("7.12.0", overview.minVersion());
    }

    @Test
    void listsAllThreeSnapshots() throws IOException {
        RepositoryOverview overview = inspector.overview();
        assertEquals(Set.of("snap_7_17_9", "snap_8_17_0", "snap_9_3_0"),
            overview.snapshots().stream().map(SnapshotOverview::name).collect(Collectors.toSet()));

        var byName = overview.snapshots().stream()
            .collect(Collectors.toMap(SnapshotOverview::name, s -> s));
        SnapshotOverview es7 = byName.get("snap_7_17_9");
        assertEquals("7.17.9", es7.version());
        assertEquals(1, es7.state());
        assertEquals("R78Q0cvSRYSueg5T5Q1s6w", es7.uuid());

        assertEquals(-1, es7.indexVersion());

        // ES has written version "8.11.0" into every snapshot entry since 8.11; index_version
        // is what tells the 8.17 and 9.3 snapshots apart.
        assertEquals(8_521_000, byName.get("snap_8_17_0").indexVersion());
        assertEquals(9_060_000, byName.get("snap_9_3_0").indexVersion());
    }

    @Test
    void listsIndicesWithRepoUuidAndShardGenerations() throws IOException {
        RepositoryOverview overview = inspector.overview();
        assertEquals(Set.of("test_data_7_17_9", "test_data_8_17_0", "test_data_9_3_0"),
            overview.indices().stream().map(IndexOverview::name).collect(Collectors.toSet()));

        var byName = overview.indices().stream()
            .collect(Collectors.toMap(IndexOverview::name, i -> i));
        IndexOverview es7 = byName.get("test_data_7_17_9");
        assertEquals("rCI54kLnSKCo4USf20Y76Q", es7.repoUuid());
        assertEquals(List.of("iN_WCfl1SGO3Oc9GODVqcw"), es7.shardGenerations());
        assertEquals(List.of("R78Q0cvSRYSueg5T5Q1s6w"), es7.snapshotUuids());
    }

    @Test
    void resolvesShardFilesWithPhysicalNamesAndLuceneVersions() throws IOException {
        ShardSnapshot shard = inspector.shardSnapshot("test_data_7_17_9", 0);

        assertEquals("rCI54kLnSKCo4USf20Y76Q", shard.repoUuid());
        assertEquals(4, shard.files().size());
        assertEquals(196803, shard.totalSize());

        var byPhysical = shard.files().stream()
            .collect(Collectors.toMap(RepoFileInfo::physicalName, f -> f));
        RepoFileInfo cfs = byPhysical.get("_0.cfs");
        assertEquals(195588, cfs.length());
        assertEquals("8.11.1", cfs.writtenBy());
        assertTrue(cfs.blobName().startsWith("__"));

        // .si and segments_N are embedded as meta_hash, not stored as blobs
        assertTrue(byPhysical.get("_0.si").blobName().startsWith("v__"));
        assertTrue(byPhysical.get("segments_3").blobName().startsWith("v__"));
        assertFalse(byPhysical.get("_0.si").metaHash().isEmpty());
    }

    @Test
    void resolvesEs9ShardFiles() throws IOException {
        ShardSnapshot shard = inspector.shardSnapshot("test_data_9_3_0", 0);
        assertTrue(shard.files().size() >= 2);
        assertEquals(shard.files().stream().mapToLong(RepoFileInfo::length).sum(), shard.totalSize());
        assertTrue(shard.totalSize() > 0);
        assertTrue(shard.files().stream().anyMatch(f -> f.physicalName().endsWith(".cfs")));
    }

    @Test
    void findsAnIndexSnapshotByUuidOrByName() throws IOException {
        assertEquals("R78Q0cvSRYSueg5T5Q1s6w",
            inspector.snapshotOfIndex("test_data_7_17_9", "R78Q0cvSRYSueg5T5Q1s6w").uuid());
        assertEquals("R78Q0cvSRYSueg5T5Q1s6w", inspector.snapshotOfIndex("test_data_7_17_9", "snap_7_17_9").uuid());
    }

    @Test
    void refusesUnknownSnapshotsAndSnapshotsWithoutTheIndex() {
        var unknown = assertThrows(IOException.class, () -> inspector.snapshotOfIndex("test_data_7_17_9", "nope"));
        assertEquals("Snapshot is absent from repository catalog", unknown.getMessage());
        var other = assertThrows(IOException.class, () -> inspector.snapshotOfIndex("test_data_7_17_9", "snap_9_3_0"));
        assertEquals("Snapshot does not contain the requested index", other.getMessage());
    }

    /**
     * index-N lists every snapshot/index/shard-generation a repository has ever had, so on a
     * large, long-lived repository it can be a multi-MB document. Re-fetching and re-parsing it
     * per call would download it once per shard in a dump-to-delta or survey run over many
     * shards. One RepositoryInspector must fetch it at most once.
     */
    @Test
    void indexNIsFetchedAtMostOnceAcrossManyCalls() throws IOException {
        var counting = new CountingBlobStore(new FileSystemBlobStore(Path.of("src/test/resources/fixtures/demo-repo")));
        var freshInspector = new RepositoryInspector(counting);

        freshInspector.overview();
        freshInspector.shardSnapshot("test_data_7_17_9", 0);
        freshInspector.shardSnapshot("test_data_8_17_0", 0);
        freshInspector.indexMetadata("test_data_9_3_0");
        freshInspector.overview();

        assertEquals(1, counting.readsOf("index-2"),
            "index-2 must be fetched once and reused across overview()/shardSnapshot()/indexMetadata()");
    }

    /** Counts reads per key; delegates everything else to a real store. */
    private static final class CountingBlobStore implements BlobStore {
        private final BlobStore delegate;
        private final java.util.Map<String, Integer> reads = new java.util.HashMap<>();

        CountingBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        int readsOf(String key) {
            return reads.getOrDefault(key, 0);
        }

        @Override
        public java.util.Optional<BlobMetadata> stat(String key) {
            return delegate.stat(key);
        }

        @Override
        public java.io.InputStream read(String key) throws IOException {
            reads.merge(key, 1, Integer::sum);
            return delegate.read(key);
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
            delegate.put(key, content);
        }
    }
}
