package de.sloow.gefrierschrank.lucene10;

import de.sloow.gefrierschrank.core.repo.CommitPoint;
import de.sloow.gefrierschrank.core.repo.FetchPlan;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.ShardFetch;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.NIOFSDirectory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every fixture shard Lucene 10 reads, opened pruned from exactly the files its plan fetches,
 * against the normal open of the fully materialized shard: same leaves and doc bases, same
 * liveness and the same source, id, routing, kind and slot per document.
 */
class OpenPrunedFixtureTest {

    static final Path CORE = Path.of("../gefrierschrank-core/src/test/resources/fixtures");
    static final Path SYNTH = Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures");
    static final AtomicInteger SELECTIVE = new AtomicInteger();
    static final AtomicInteger SHARDS = new AtomicInteger();
    static final java.util.Set<String> SPI_ONLY_PRUNED = java.util.concurrent.ConcurrentHashMap.newKeySet();

    record FixtureShard(String label, Path repo, String index, String snapshotUuid, int shardId) {
        @Override public String toString() { return label; }
    }

    static List<FixtureShard> shards() throws IOException {
        var repos = List.of(CORE.resolve("demo-repo"), CORE.resolve("reused-index-repo"), CORE.resolve("frozen-repo"),
            CORE.resolve("es7-features-repo"), CORE.resolve("es7-upgraded-repo"), CORE.resolve("es7-best-compression-repo"),
            CORE.resolve("retired-shell-repo/before"), CORE.resolve("retired-shell-repo/after"),
            Path.of("../gefrierschrank-cli/src/test/resources/fixtures/ilm-shell-repo"), SYNTH.resolve("synth-repo"), SYNTH.resolve("synth-deletes-repo"),
            SYNTH.resolve("zstd-synth-repo"), SYNTH.resolve("es93-repo"));
        var shards = new ArrayList<FixtureShard>();
        for (Path repo : repos) {
            var inspector = new RepositoryInspector(new FileSystemBlobStore(repo));
            for (var index : inspector.overview().indices()) {
                // es7_completion's postings format lives in lucene-suggest, which only the CLI classpath carries.
                if (index.name().equals("es7_completion")) continue;
                for (String snapshot : index.snapshotUuids()) {
                    for (int shardId = 0; shardId < index.shardGenerations().size(); shardId++) {
                        ShardSnapshot shard;
                        try {
                            shard = inspector.shardSnapshot(index.name(), shardId, snapshot);
                        } catch (IOException missing) {
                            continue;
                        }
                        boolean hasCommit = shard.files().stream().anyMatch(f -> f.physicalName().startsWith("segments_"));
                        boolean readable = shard.files().stream().map(RepoFileInfo::writtenBy).filter(v -> v != null)
                            .allMatch(v -> Integer.parseInt(v.substring(0, v.indexOf('.'))) >= 8);
                        if (hasCommit && readable) {
                            shards.add(new FixtureShard(repo.toString().replaceAll(".*/fixtures/", "") + "/" + index.name() + "/" + snapshot + "/" + shardId,
                                repo, index.name(), snapshot, shardId));
                        }
                    }
                }
            }
        }
        return shards;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("shards")
    void thePlannedFilesOpenPrunedLikeTheFullShard(FixtureShard fixture, @TempDir Path tmp) throws Exception {
        var blobs = new FileSystemBlobStore(fixture.repo());
        ShardSnapshot shard = new RepositoryInspector(blobs).shardSnapshot(fixture.index(), fixture.shardId(), fixture.snapshotUuid());
        Path full = tmp.resolve("full");
        new ShardMaterializer(blobs).materialize(shard, full);

        Path partial = tmp.resolve("partial");
        ShardFetch fetch = new ShardMaterializer(blobs).begin(shard, partial);
        fetch.fetch(file -> SelectiveFetch.isCommitFile(file.physicalName()));
        CommitPoint commit = CommitPointReader.read(partial);
        FetchPlan plan = SelectiveFetch.plan(commit, shard, true);
        fetch.fetch(plan.include());
        SHARDS.incrementAndGet();
        if (plan.selective()) SELECTIVE.incrementAndGet();

        Path prunedDir = plan.selective() ? partial : full;
        List<String> expected;
        try (var reader = new Lucene10ShardReader(full)) {
            expected = PrunedCodecTest.observe(reader);
        } catch (IllegalArgumentException missingSpiFormat) {
            // A vector format this module does not register: the pruned open never resolves it.
            // Such indices take the full plan in the export (excluded-vector rule), so nothing more to compare.
            assertTrue(missingSpiFormat.getMessage().contains("KnnVectorsFormat"), missingSpiFormat.getMessage());
            try (var reader = Lucene10ShardReader.openPruned(prunedDir)) {
                assertTrue(reader.docCount() > 0);
            }
            SPI_ONLY_PRUNED.add(fixture.label());
            return;
        }
        try (var reader = Lucene10ShardReader.openPruned(prunedDir)) {
            assertEquals(expected, PrunedCodecTest.observe(reader));
        }
        try (var fullDirectory = new NIOFSDirectory(full); var prunedDirectory = new NIOFSDirectory(prunedDir)) {
            var commits = DirectoryReader.listCommits(fullDirectory);
            try (var normal = DirectoryReader.open(commits.get(commits.size() - 1), 8, null);
                 var pruned = Lucene10ShardReader.openPrunedReader(prunedDirectory)) {
                assertEquals(PrunedCodecTest.leafLayout(normal), PrunedCodecTest.leafLayout(pruned));
            }
        }
        long liveByCommit = 0;
        for (CommitPoint.Segment segment : commit.segments()) {
            assertTrue(segment.liveDocs() != CommitPoint.UNKNOWN, "ES flags its soft-deletes field where it has one: " + segment);
            liveByCommit += segment.liveDocs();
        }
        assertEquals(liveByCommit > 0, expected.stream().anyMatch(line -> line.contains(" live=true ")),
            "the commit's live count agrees with the reader on whether anything is live");
        if (plan.selective()) {
            try (var files = Files.list(partial)) {
                assertEquals(shard.files().stream().filter(plan.include()).count(), files.count());
            }
            assertTrue(fetch.fetchedBytes() <= shard.totalSize());
        }
    }

    @AfterAll
    static void someFixtureShardsArePlannedSelectively() {
        assertTrue(SHARDS.get() == 0 || SELECTIVE.get() > 0, "selective " + SELECTIVE + " of " + SHARDS);
        assertFalse(SELECTIVE.get() == SHARDS.get() && SHARDS.get() > 0, "synthetic fixtures take the full plan");
        assertTrue(SPI_ONLY_PRUNED.stream().allMatch(label -> label.startsWith("es93-repo/test_es93_vectors")), SPI_ONLY_PRUNED.toString());
    }
}
