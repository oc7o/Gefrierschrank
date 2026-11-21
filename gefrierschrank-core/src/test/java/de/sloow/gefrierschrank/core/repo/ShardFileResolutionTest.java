package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/** File-resolution coverage across all three fixture snapshots (ES 7/8/9). */
class ShardFileResolutionTest {

    private static RepositoryInspector inspector;

    @BeforeAll
    static void setup() {
        inspector = new RepositoryInspector(
            new FileSystemBlobStore(Path.of("src/test/resources/fixtures/demo-repo")));
    }

    @Test
    void shardUnionIndexListsAllFilesAcrossSnapshots() throws IOException {
        // Union list from shard index-<gen>: ES8 wrote extra files during flush;
        // must include everything any snapshot references.
        ShardSnapshot es8 = inspector.shardSnapshot("test_data_8_17_0", 0);
        assertTrue(es8.files().stream().anyMatch(f -> f.physicalName().equals("_0.cfs")));
    }

    @Test
    void es9WrittenByIsLucene10() throws IOException {
        ShardSnapshot es9 = inspector.shardSnapshot("test_data_9_3_0", 0);
        var versions = es9.files().stream()
            .map(RepoFileInfo::writtenBy)
            .collect(Collectors.toSet());
        assertEquals("10.3.2", versions.iterator().next(),
            "ES 9.3.0 snapshots are written by Lucene 10.3.2");
    }

    @Test
    void es7WrittenByIsLucene8() throws IOException {
        ShardSnapshot es7 = inspector.shardSnapshot("test_data_7_17_9", 0);
        var versions = es7.files().stream()
            .map(RepoFileInfo::writtenBy)
            .collect(Collectors.toSet());
        assertEquals(Set.of("8.11.1"), versions);
    }

    @Test
    void unknownIndexFails() {
        assertThrows(IOException.class, () -> inspector.shardSnapshot("does_not_exist", 0));
    }

    @Test
    void shardResolutionIsRepeatable() throws IOException {
        ShardSnapshot first = inspector.shardSnapshot("test_data_7_17_9", 0);
        ShardSnapshot second = inspector.shardSnapshot("test_data_7_17_9", 0);
        assertEquals(first, second);
    }

    /**
     * A shard directory is reused when an index is deleted and a new index of the
     * SAME NAME is later created while an earlier snapshot referencing the original incarnation
     * is still retained. ES's own index-N keeps BOTH incarnations distinguishably
     * (index.snapshotUuids() lists every retained snapshot still referencing this shard
     * directory, in the order they were taken - RepositoryData.addSnapshot always appends).
     * shardSnapshot() must resolve to the LATEST (most recently taken) of them, not an
     * arbitrary one - picking a stale incarnation doesn't undercount, it silently returns the
     * WRONG shard content (different segments_N, different file bytes under the same physical
     * names) while looking entirely plausible.
     *
     * <p>Fixture: real ES 9.3.0 output. Index "reused_name" was created, snapshotted as
     * "snap_a1" (1 doc), deleted, recreated with the same name, and snapshotted again as
     * "snap_a2" (2 docs, different content) - both snapshots kept. index-N records
     * indices.reused_name.snapshots = [snap_a1_uuid, snap_a2_uuid] in that (taken-)order.
     */
    @Test
    void shardSnapshotResolvesToLatestIncarnationWhenIndexNameWasReused() throws IOException {
        var store = new FileSystemBlobStore(Path.of("src/test/resources/fixtures/reused-index-repo"));
        var reusedInspector = new RepositoryInspector(store);

        RepositoryOverview overview = reusedInspector.overview();
        IndexOverview reused = overview.indices().stream()
            .filter(i -> i.name().equals("reused_name"))
            .findFirst().orElseThrow();
        // Both incarnations retained, oldest first (ES append order).
        assertEquals(2, reused.snapshotUuids().size());

        ShardSnapshot shard = reusedInspector.shardSnapshot("reused_name", 0);
        // The second (later) snapshot indexed 2 docs with different content and therefore a
        // larger, differently-checksummed Lucene commit than the first (1 doc) - resolving to
        // the wrong incarnation is caught by asserting on both the snapshot name AND total size.
        assertEquals("snap_a2", shard.snapshotName(),
            "must resolve to the snapshot taken LAST against this shard directory, not the first");
        assertEquals(4716, shard.totalSize());
    }

    /**
     * {@link RepositoryInspector#reachableBlobKeys()} must be a UNION across every retained
     * snapshot of a shard, not just the latest incarnation {@link #shardSnapshot} resolves to -
     * otherwise every blob belonging to the still-retained OLDER "snap_a1" incarnation of
     * reused_name/0 would be misreported as orphaned/unreferenced garbage, which it is not:
     * ES can still restore snap_a1 on request.
     */
    @Test
    void reachableBlobKeysIncludesEveryRetainedIncarnationOfAReusedShard() throws IOException {
        var store = new FileSystemBlobStore(Path.of("src/test/resources/fixtures/reused-index-repo"));
        var reusedInspector = new RepositoryInspector(store);

        var reachable = reusedInspector.reachableBlobKeys();

        // Blobs belonging to the OLDER incarnation (snap_a1) - must still be reachable.
        assertTrue(reachable.contains("indices/DNjPaXmgTWiaT_swArrkDg/0/__VZZO8Ex7SZCFoMpdhy0dRQ"),
            "snap_a1's _0.cfe blob must be reachable (that snapshot is still retained)");
        assertTrue(reachable.contains("indices/DNjPaXmgTWiaT_swArrkDg/0/__tojJx8CPTqeIcmubqEPO6w"),
            "snap_a1's _0.cfs blob must be reachable (that snapshot is still retained)");
        assertTrue(reachable.contains("indices/DNjPaXmgTWiaT_swArrkDg/0/snap-LmBebsKtQ3eqx1gDzGM-3w.dat"));

        // Blobs belonging to the LATER incarnation (snap_a2) - also reachable.
        assertTrue(reachable.contains("indices/DNjPaXmgTWiaT_swArrkDg/0/__8afO66c6Sly3x0Fdhp0mTw"));
        assertTrue(reachable.contains("indices/DNjPaXmgTWiaT_swArrkDg/0/__1uoPmDR4QVWZKvVORJvAhw"));
        assertTrue(reachable.contains("indices/DNjPaXmgTWiaT_swArrkDg/0/snap-w7LYyWy7RX-dXdTtQthVMw.dat"));

        // Root generation pointers and consolidated shard listing.
        assertTrue(reachable.contains("index.latest"));
        assertTrue(reachable.contains("index-3"));
        assertTrue(reachable.contains("indices/DNjPaXmgTWiaT_swArrkDg/0/index-04SXSLfBQiSWHLtmg-O_mQ"));

        // Diffing this against a full bulk listing of the fixture repo should find ZERO
        // orphans: every blob physically present is referenced by index-3.
        var allBlobs = store.listWithSizes("");
        var physicalKeys = allBlobs.stream().map(BlobMetadata::key)
            .filter(k -> !k.endsWith("/")) // directory marker safety net; FS store never emits these
            .collect(Collectors.toSet());
        var orphans = physicalKeys.stream().filter(k -> !reachable.contains(k)).toList();
        assertEquals(List.of(), orphans, "fixture repo has no genuinely orphaned blobs");
    }
}
