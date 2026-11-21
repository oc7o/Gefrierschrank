package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FetchPlan;
import de.sloow.gefrierschrank.core.repo.PrunedFormatAccessException;
import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

import static de.sloow.gefrierschrank.delta.SelectiveShardFixture.STORED_METADATA;
import static de.sloow.gefrierschrank.delta.SelectiveShardFixture.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardOpeningTest {

    private String saved;

    @BeforeEach
    void save() {
        saved = System.getProperty(SelectiveFetch.PROPERTY);
    }

    @AfterEach
    void restore() {
        if (saved == null) System.clearProperty(SelectiveFetch.PROPERTY);
        else System.setProperty(SelectiveFetch.PROPERTY, saved);
    }

    static Set<String> listing(Path dir) throws IOException {
        try (var files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString()).collect(Collectors.toSet());
        }
    }

    static Set<String> names(ShardSnapshot shard, java.util.function.Predicate<RepoFileInfo> include) {
        return shard.files().stream().filter(include).map(RepoFileInfo::physicalName).collect(Collectors.toSet());
    }

    @Test
    void withoutTheFlagTheShardDownloadsInOneRoundAndOpensNormally(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("s"));
        var opener = new SelectiveShardFixture.Opener();
        var tracker = new ShardOpening.Tracker();
        assertEquals("none", tracker.fetchMode());
        var opened = ShardOpening.open(new ShardMaterializer(new SelectiveShardFixture.CountingStore(shard.repo())), opener,
            shard.snapshot(), tmp.resolve("dir"), "idx", STORED_METADATA, tracker);
        try (ShardReader reader = opened.rawReader()) {
            assertInstanceOf(Lucene10ShardReader.class, reader);
        }
        assertEquals(FetchPlan.Mode.FULL, opened.plan());
        assertFalse(opened.selective());
        assertEquals(0, opener.commitReads.get());
        assertEquals(1, opener.opens.get());
        assertEquals("full", tracker.fetchMode());
        assertEquals(shard.downloadBytes(), tracker.bytesFetched());
        assertEquals(names(shard.snapshot(), f -> true), listing(tmp.resolve("dir")));
    }

    @Test
    void withTheFlagAnEligibleShardOpensPrunedOverItsPlannedFiles(@TempDir Path tmp) throws Exception {
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("s"));
        var opener = new SelectiveShardFixture.Opener();
        var tracker = new ShardOpening.Tracker();
        var opened = ShardOpening.open(new ShardMaterializer(new SelectiveShardFixture.CountingStore(shard.repo())), opener,
            shard.snapshot(), tmp.resolve("dir"), "idx", STORED_METADATA, tracker);
        opened.rawReader().close();
        assertTrue(opened.selective());
        assertEquals(1, opener.commitReads.get());
        assertEquals(1, opener.prunedOpens.get());
        assertEquals(0, opener.opens.get());
        assertEquals("selective", tracker.fetchMode());
        Set<String> pruned = names(shard.snapshot(), f -> shard.prunableBlobs().contains(f.blobName()));
        assertFalse(pruned.isEmpty());
        Set<String> expected = names(shard.snapshot(), f -> !pruned.contains(f.physicalName()));
        assertEquals(expected, listing(tmp.resolve("dir")));
        assertFalse(opened.fetch().complete());
    }

    @Test
    void withTheFlagAnIneligibleShardTakesTheFullPlanAfterItsCommitFiles(@TempDir Path tmp) throws Exception {
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("s"));
        for (var metadata : new ArrayList<>(java.util.Arrays.asList(parse("{\"settings\":{\"index.mode\":\"logsdb\"},\"mappings\":{}}"), null))) {
            var opener = new SelectiveShardFixture.Opener();
            var tracker = new ShardOpening.Tracker();
            Path dir = tmp.resolve("dir-" + (metadata == null));
            var opened = ShardOpening.open(new ShardMaterializer(new SelectiveShardFixture.CountingStore(shard.repo())), opener,
                shard.snapshot(), dir, "idx", metadata, tracker);
            opened.rawReader().close();
            assertEquals(FetchPlan.Mode.FULL, opened.plan());
            assertEquals(1, opener.commitReads.get(), "the preflight runs on the flag, eligible or not");
            assertEquals(1, opener.opens.get());
            assertEquals("full", tracker.fetchMode());
            assertEquals(names(shard.snapshot(), f -> true), listing(dir));
        }
    }

    @Test
    void aRuntimeWithoutCommitFactsTakesTheFullPlan(@TempDir Path tmp) throws Exception {
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("s"));
        DumpToDeltaPipeline.ShardReaderOpener plain = (s, dir) -> new Lucene10ShardReader(dir);
        var opened = ShardOpening.open(new ShardMaterializer(new SelectiveShardFixture.CountingStore(shard.repo())), plain,
            shard.snapshot(), tmp.resolve("dir"), "idx", STORED_METADATA);
        opened.rawReader().close();
        assertEquals(FetchPlan.Mode.FULL, opened.plan());
        assertTrue(opened.fetch().complete());
    }

    @Test
    void aFailingPrunedOpenCompletesInPlace(@TempDir Path tmp) throws Exception {
        System.setProperty(SelectiveFetch.PROPERTY, "true");
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("s"));
        var opener = new SelectiveShardFixture.Opener() {
            @Override
            public ShardReader openPruned(ShardSnapshot s, Path dir, com.fasterxml.jackson.databind.JsonNode metadata) throws Exception {
                throw new PrunedFormatAccessException();
            }
        };
        var tracker = new ShardOpening.Tracker();
        var opened = ShardOpening.open(new ShardMaterializer(new SelectiveShardFixture.CountingStore(shard.repo())), opener,
            shard.snapshot(), tmp.resolve("dir"), "idx", STORED_METADATA, tracker);
        opened.rawReader().close();
        assertTrue(opened.selective());
        assertTrue(opened.fetch().complete());
        assertEquals("completed", tracker.fetchMode());
        assertEquals(shard.downloadBytes(), tracker.bytesFetched());

        var broken = new SelectiveShardFixture.Opener() {
            @Override
            public ShardReader openPruned(ShardSnapshot s, Path dir, com.fasterxml.jackson.databind.JsonNode metadata) throws Exception {
                throw new PrunedFormatAccessException();
            }

            @Override
            public ShardReader open(ShardSnapshot s, Path dir) throws Exception {
                throw new IOException("normal open failed too");
            }
        };
        var failure = assertThrows(IOException.class, () -> ShardOpening.open(new ShardMaterializer(
            new SelectiveShardFixture.CountingStore(shard.repo())), broken, shard.snapshot(), tmp.resolve("dir2"), "idx", STORED_METADATA));
        assertEquals("normal open failed too", failure.getMessage());
        assertInstanceOf(PrunedFormatAccessException.class, failure.getSuppressed()[0]);
    }

    @Test
    void anInvalidFileNameFailsBeforeAnyDownload(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("s"));
        var files = new ArrayList<>(shard.snapshot().files());
        files.add(new RepoFileInfo("__x", "../evil", 1, null, "10.3.2", null));
        var bad = new ShardSnapshot("repo", SelectiveShardFixture.PREFIX, "snap", 1, 0, files);
        var store = new SelectiveShardFixture.CountingStore(shard.repo());
        var tracker = new ShardOpening.Tracker();
        assertThrows(IOException.class, () -> ShardOpening.open(new ShardMaterializer(store), new SelectiveShardFixture.Opener(),
            bad, tmp.resolve("dir"), "idx", STORED_METADATA, tracker));
        assertEquals("none", tracker.fetchMode());
        assertEquals(0, tracker.bytesFetched());
        assertEquals(Set.of(), store.blobsRead());
    }
}
