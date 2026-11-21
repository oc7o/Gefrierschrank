package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import io.delta.standalone.DeltaLog;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.*;

/** The snapshot guard armed by requireArchiveSnapshot holds for every later commit, atomically. */
class ArchiveGuardTest {

    static final ArchiveContext SNAP_1 = new ArchiveContext("idx", "rid", "snap-1", "one", "idx", "snap-1");
    static final ArchiveContext SNAP_2 = new ArchiveContext("idx", "rid", "snap-2", "two", "idx", "snap-2");

    @AfterEach
    void resetLogStore() {
        FaultInjectingLogStore.reset();
    }

    @Test
    void armedWriterRejectsAnotherSnapshotBeforeWritingAnyParquet(@TempDir Path target) throws Exception {
        var conf = new Configuration();
        String table = target.toString();
        var writerA = new DeltaTableWriter(table);
        writerA.requireArchiveSnapshot(conf, SNAP_1, 2);
        writeShard(new DeltaTableWriter(table), conf, SNAP_2, 0);
        var filesOfB = ParquetFooters.parquetFilesOnDisk(target);

        var untouched = new Iterator<String>() {
            public boolean hasNext() { throw new AssertionError("documents must not be read"); }
            public String next() { throw new AssertionError("documents must not be read"); }
        };
        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class,
            () -> writerA.write(conf, untouched, ArchiveContext.mapping(), SNAP_1.key(1)));
        assertEquals(filesOfB, ParquetFooters.parquetFilesOnDisk(target));
        assertEquals(Set.of("snap-2"), snapshotUuids(conf, table));
    }

    @Test
    void commitRacingAnotherSnapshotNeverLands(@TempDir Path target) throws Exception {
        var conf = new Configuration();
        String table = target.toString();
        var writerA = new DeltaTableWriter(table);
        writerA.requireArchiveSnapshot(conf, SNAP_1, 2);
        var raced = new AtomicBoolean();
        writerA.beforeCommit = () -> {
            if (raced.getAndSet(true)) return;
            try {
                writeShard(new DeltaTableWriter(table), conf, SNAP_2, 0);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        };

        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class, () -> writeShard(writerA, conf, SNAP_1, 0));
        assertTrue(raced.get());
        assertEquals(0, DeltaLog.forTable(conf, table).update().getVersion(), "only the other snapshot's commit");
        assertEquals(Set.of("snap-2"), snapshotUuids(conf, table));
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
    }

    /**
     * On an existing table only the whole-table read makes the racing commit a conflict: without
     * it, Delta would retry the commit on top of the other snapshot's version without asking us.
     */
    @Test
    void commitRacingAnotherSnapshotOnAnExistingTableNeverLands(@TempDir Path target) throws Exception {
        var conf = new Configuration();
        String table = target.toString();
        var writerA = new DeltaTableWriter(table);
        writerA.requireArchiveSnapshot(conf, SNAP_1, 3);
        writeShard(writerA, conf, SNAP_1, 0);
        var raced = new AtomicBoolean();
        writerA.beforeCommit = () -> {
            if (raced.getAndSet(true)) return;
            try {
                writeShard(new DeltaTableWriter(table), conf, SNAP_2, 0);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        };

        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class, () -> writeShard(writerA, conf, SNAP_1, 1));
        assertTrue(raced.get());
        assertEquals(1, DeltaLog.forTable(conf, table).update().getVersion(), "shard 1 of snap-1 did not land");
        assertEquals(Set.of(SNAP_1.key(0), SNAP_2.key(0)), new DeltaTableWriter(table).committedShardKeys(conf));
        assertEquals(ParquetFooters.addFilePaths(conf, table), ParquetFooters.parquetFilesOnDisk(target));
    }

    @Test
    void writersOfTheSameSnapshotInterleaveFreely(@TempDir Path target) throws Exception {
        var conf = new Configuration();
        String table = target.toString();
        var writerA = new DeltaTableWriter(table);
        var writerB = new DeltaTableWriter(table);
        writerA.requireArchiveSnapshot(conf, SNAP_1, 6);
        writerB.requireArchiveSnapshot(conf, SNAP_1, 6);
        for (int shard = 0; shard < 6; shard++) {
            assertEquals(2, writeShard(shard % 2 == 0 ? writerA : writerB, conf, SNAP_1, shard));
        }
        var keys = new java.util.HashSet<String>();
        for (int shard = 0; shard < 6; shard++) keys.add(SNAP_1.key(shard));
        assertEquals(keys, writerA.committedShardKeys(conf));
        new DeltaTableWriter(table).requireArchiveSnapshot(conf, SNAP_1, 6);
    }

    @Test
    void verificationReadsOnlyNewVersions(@TempDir Path target) throws Exception {
        var conf = FaultInjectingLogStore.enable(new Configuration());
        String table = target.toString();
        var writer = new DeltaTableWriter(table);
        for (int shard = 0; shard < 25; shard++) writeShard(writer, conf, SNAP_1, shard);
        var guard = new DeltaTableWriter(table).requireArchiveSnapshot(conf, SNAP_1, 40);
        assertEquals(24, guard.verifiedVersion());
        writeShard(writer, conf, SNAP_1, 25);
        writeShard(writer, conf, SNAP_1, 26);

        var log = DeltaLog.forTable(conf, table);
        assertEquals(26, log.update().getVersion());
        FaultInjectingLogStore.reset();
        guard.verifyThrough(log, 26);
        assertEquals(List.of(25L, 26L), FaultInjectingLogStore.deltaVersionsRead());
        assertEquals(26, guard.verifiedVersion());

        FaultInjectingLogStore.reset();
        guard.verifyThrough(log, 26);
        assertEquals(List.of(), FaultInjectingLogStore.deltaVersionsRead(), "nothing new, nothing read");
    }

    @Test
    void missingVersionsFallBackToAFullCheck(@TempDir Path target) throws Exception {
        var conf = FaultInjectingLogStore.enable(new Configuration());
        String table = target.toString();
        var writer = new DeltaTableWriter(table);
        for (int shard = 0; shard < 6; shard++) writeShard(writer, conf, SNAP_1, shard);
        var guard = new DeltaTableWriter(table).requireArchiveSnapshot(conf, SNAP_1, 40);
        assertEquals(5, guard.verifiedVersion());
        var keysOfBoth = new java.util.HashSet<>(expectedKeys(SNAP_1, 40));
        keysOfBoth.add(SNAP_2.key(0));
        var tolerant = new ArchiveGuard(keysOfBoth, DeltaTableWriter.archiveSchema());
        for (int shard = 6; shard < 25; shard++) writeShard(writer, conf, SNAP_1, shard);
        writeShard(new DeltaTableWriter(table), conf, SNAP_2, 0);
        // Versions 0-9 as log cleanup would leave them: covered by the checkpoints at 10 and 20.
        removeCommitFiles(target, LongStream.rangeClosed(0, 9));

        var log = DeltaLog.forTable(conf, table);
        assertEquals(25, log.update().getVersion());
        FaultInjectingLogStore.reset();
        assertThrows(DeltaTableWriter.ArchiveTargetConflictException.class, () -> guard.verifyThrough(log, 25));
        assertTrue(FaultInjectingLogStore.deltaVersionsRead().containsAll(List.of(21L, 22L, 23L, 24L, 25L)),
            "the full check replays the snapshot: " + FaultInjectingLogStore.deltaVersionsRead());
        tolerant.verifyThrough(log, 25);
        assertEquals(25, tolerant.verifiedVersion());
    }

    @Test
    void archiveTablesKeepTheirSchemaTagsAndTransactionIds(@TempDir Path target) throws Exception {
        var conf = new Configuration();
        String table = target.toString();
        var writer = new DeltaTableWriter(table);
        for (int shard = 0; shard < 3; shard++) writeShard(writer, conf, SNAP_1, shard);
        var log = DeltaLog.forTable(conf, table);
        assertEquals(DeltaTableWriter.archiveSchema(), log.update().getMetadata().getSchema());
        for (var file : log.snapshot().getAllFiles()) assertTrue(file.getTags().get(ShardKey.TAG).startsWith("archive-v1:"));
        var changes = log.getChanges(0, true);
        for (int version = 0; version < 3; version++) {
            var actions = changes.next().getActions();
            var appIds = actions.stream().filter(a -> a instanceof io.delta.standalone.actions.SetTransaction)
                .map(a -> ((io.delta.standalone.actions.SetTransaction) a).getAppId()).toList();
            var tags = actions.stream().filter(a -> a instanceof io.delta.standalone.actions.AddFile)
                .map(a -> ((io.delta.standalone.actions.AddFile) a).getTags().get(ShardKey.TAG)).distinct().toList();
            assertEquals(1, appIds.size());
            assertEquals(List.of(appIds.get(0)), tags, "each commit's files carry its SetTransaction appId");
            assertEquals(SNAP_1.key(version), appIds.get(0));
        }
    }

    static long writeShard(DeltaTableWriter writer, Configuration conf, ArchiveContext context, int shard) throws IOException {
        var rows = new ArrayList<String>();
        for (int doc = 0; doc < 2; doc++) {
            rows.add(context.document("{\"shard\":" + shard + ",\"doc\":" + doc + "}", shard,
                new DocumentMetadata("id-" + shard + "-" + doc, null, "stored", doc)));
        }
        return writer.write(conf, rows.iterator(), ArchiveContext.mapping(), context.key(shard));
    }

    static Set<String> snapshotUuids(Configuration conf, String table) throws IOException {
        var uuids = new java.util.HashSet<String>();
        try (var rows = DeltaLog.forTable(conf, table).update().open()) {
            while (rows.hasNext()) uuids.add(rows.next().getString("_snapshot_uuid"));
        }
        return uuids;
    }

    static List<String> expectedKeys(ArchiveContext context, int shards) throws IOException {
        var keys = new ArrayList<String>();
        for (int shard = 0; shard < shards; shard++) keys.add(context.key(shard));
        return keys;
    }

    static void removeCommitFiles(Path table, LongStream versions) {
        versions.forEach(version -> {
            try {
                Files.delete(table.resolve("_delta_log").resolve(String.format("%020d.json", version)));
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        });
    }
}
