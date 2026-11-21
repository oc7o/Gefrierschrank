package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotSelectionTest {

    private static final String INDEX = "logs";
    private static final String REPO_ID = "repo-index-id";

    @Test
    void parseRecognizesKeywordsAndTreatsEverythingElseAsANameOrUuid() {
        assertSame(SnapshotSelection.AUTO, SnapshotSelection.parse("auto"));
        assertSame(SnapshotSelection.LATEST, SnapshotSelection.parse("latest"));
        assertEquals(SnapshotSelection.explicit("snap_x"), SnapshotSelection.parse("snap_x"));
        assertEquals("explicit", SnapshotSelection.parse("snap_x").label());
        assertThrows(IllegalArgumentException.class, () -> SnapshotSelection.parse(" "));
        assertThrows(IllegalArgumentException.class, () -> new SnapshotSelection(SnapshotSelection.Mode.AUTO, "x"));
    }

    @Test
    void theDefaultIsAuto() {
        assertSame(SnapshotSelection.AUTO, SnapshotSelection.DEFAULT);
        assertSame(SnapshotSelection.DEFAULT, ExportSettings.DEFAULT.snapshotSelection());
        assertEquals(1, ExportSettings.DEFAULT.parallelism());
    }

    @Test
    void singleSnapshotKeysPinThatSnapshot() throws Exception {
        var context = context("snap-a");
        var keys = List.of(context.key(0), context.key(2));
        assertEquals(Optional.of("snap-a"),
            FullIndexExporter.resumableSnapshot(keys, INDEX, REPO_ID, List.of("snap-old", "snap-a", "snap-new")));
    }

    @Test
    void theMetadataKeyAlonePinsItsSnapshot() throws Exception {
        assertEquals(Optional.of("snap-a"),
            FullIndexExporter.resumableSnapshot(Set.of(context("snap-a").key(-1)), INDEX, REPO_ID, List.of("snap-a")));
    }

    @Test
    void mountedKeysPinTheMountedNamesSnapshotNotTheBackingOne() throws Exception {
        var mounted = new ArchiveContext(INDEX, REPO_ID, "mount-snapshot", "m", "backing-index", "backing-uuid");
        assertEquals(Optional.of("mount-snapshot"), FullIndexExporter.resumableSnapshot(
            List.of(mounted.key(0)), INDEX, REPO_ID, List.of("mount-snapshot")));
    }

    @Test
    void keysNamingTwoSnapshotsDoNotPin() throws Exception {
        var keys = List.of(context("snap-a").key(0), context("snap-b").key(1));
        assertEquals(Optional.empty(),
            FullIndexExporter.resumableSnapshot(keys, INDEX, REPO_ID, List.of("snap-a", "snap-b")));
    }

    @Test
    void aDifferentRepositoryIndexIdDoesNotPin() throws Exception {
        var foreign = new ArchiveContext(INDEX, "recreated-index-id", "snap-a", "n", INDEX, "snap-a");
        assertEquals(Optional.empty(),
            FullIndexExporter.resumableSnapshot(List.of(foreign.key(0)), INDEX, REPO_ID, List.of("snap-a")));
    }

    @Test
    void aDifferentIndexNameDoesNotPin() throws Exception {
        var other = new ArchiveContext("other", REPO_ID, "snap-a", "n", "other", "snap-a");
        assertEquals(Optional.empty(),
            FullIndexExporter.resumableSnapshot(List.of(other.key(0)), INDEX, REPO_ID, List.of("snap-a")));
    }

    @Test
    void aSnapshotNoLongerRetainedDoesNotPin() throws Exception {
        assertEquals(Optional.empty(), FullIndexExporter.resumableSnapshot(
            List.of(context("snap-a").key(0)), INDEX, REPO_ID, List.of("snap-b")));
    }

    @Test
    void oneUnparseableKeyDoesNotPin() throws Exception {
        var keys = List.of(context("snap-a").key(0), ShardKey.of(INDEX, 1, "snap-a"));
        assertEquals(Optional.empty(), FullIndexExporter.resumableSnapshot(keys, INDEX, REPO_ID, List.of("snap-a")));
    }

    @Test
    void noKeysDoNotPin() {
        assertEquals(Optional.empty(), FullIndexExporter.resumableSnapshot(List.of(), INDEX, REPO_ID, List.of("snap-a")));
    }

    // --- a mounted index's table whose shell snapshot retention deleted ---

    private static final String SHELL = "partial-logs";
    private static final String RETIRED = "retired-shell-snapshot";
    private static final String BACKING_INDEX = "logs-000001";
    private static final String BACKING = "backing-snapshot";
    private static final ArchiveContext SHELL_TABLE =
        new ArchiveContext(SHELL, REPO_ID, RETIRED, "slm-1", BACKING_INDEX, BACKING);
    private static final FullIndexExporter.MetadataRow ROW =
        new FullIndexExporter.MetadataRow(SHELL, RETIRED, "slm-1", BACKING);

    private static Optional<FullIndexExporter.Label> retired(List<String> documentKeys, List<String> metadataKeys,
                                                             List<FullIndexExporter.MetadataRow> rows,
                                                             List<String> retained, String latestSourceIndex,
                                                             String latestSourceSnapshot, int chain) {
        return FullIndexExporter.retiredShell(documentKeys, metadataKeys, rows, SHELL, REPO_ID, retained,
            latestSourceIndex, latestSourceSnapshot, chain);
    }

    private static Optional<FullIndexExporter.Label> retired(List<String> documentKeys, List<String> metadataKeys,
                                                             List<FullIndexExporter.MetadataRow> rows) {
        return retired(documentKeys, metadataKeys, rows, List.of("slm-2"), BACKING_INDEX, BACKING, 2);
    }

    @Test
    void aRetiredShellSnapshotLabelsItsTableWhenTheBackingDataIsUnchanged() throws Exception {
        var label = new FullIndexExporter.Label(RETIRED, "slm-1");
        assertEquals(Optional.of(label), retired(List.of(SHELL_TABLE.key(0), SHELL_TABLE.key(1)),
            List.of(SHELL_TABLE.key(-1)), List.of(ROW)), "complete or partly written");
        assertEquals(Optional.of(label), retired(List.of(), List.of(SHELL_TABLE.key(-1)), List.of(ROW)),
            "only the metadata row was committed");
        assertTrue(FullIndexExporter.RESUME_RETIRED_SHELLS, "resume retired shells under auto");
    }

    @Test
    void aNonMountedTableIsNeverRelabelled() throws Exception {
        var own = new ArchiveContext(SHELL, REPO_ID, RETIRED, "slm-1", SHELL, RETIRED);
        var ownRow = new FullIndexExporter.MetadataRow(SHELL, RETIRED, "slm-1", RETIRED);
        assertEquals(Optional.empty(), retired(List.of(own.key(0)), List.of(own.key(-1)), List.of(ownRow),
            List.of("slm-2"), SHELL, RETIRED, 2), "its data was the snapshot's own and can have changed since");
        assertEquals(Optional.empty(), retired(List.of(SHELL_TABLE.key(0)), List.of(SHELL_TABLE.key(-1)), List.of(ROW),
            List.of("slm-2"), BACKING_INDEX, BACKING, 1), "the latest snapshot holds the index's own data");
    }

    @Test
    void aDifferentBackingIndexOrSnapshotIsRefused() throws Exception {
        var keys = List.of(SHELL_TABLE.key(0));
        var metadata = List.of(SHELL_TABLE.key(-1));
        assertEquals(Optional.empty(), retired(keys, metadata, List.of(ROW), List.of("slm-2"), BACKING_INDEX, "other-backing", 2));
        assertEquals(Optional.empty(), retired(keys, metadata, List.of(ROW), List.of("slm-2"), "logs-000002", BACKING, 2));
    }

    @Test
    void mixedKeysAreRefused() throws Exception {
        var otherShell = new ArchiveContext(SHELL, REPO_ID, "another-shell", "slm-0", BACKING_INDEX, BACKING);
        var otherBacking = new ArchiveContext(SHELL, REPO_ID, RETIRED, "slm-1", BACKING_INDEX, "older-backing");
        var metadata = List.of(SHELL_TABLE.key(-1));
        assertEquals(Optional.empty(), retired(List.of(SHELL_TABLE.key(0), otherShell.key(1)), metadata, List.of(ROW)));
        assertEquals(Optional.empty(), retired(List.of(SHELL_TABLE.key(0), otherBacking.key(1)), metadata, List.of(ROW)));
        assertEquals(Optional.empty(), retired(List.of(otherShell.key(0)), metadata, List.of(ROW)));
        assertEquals(Optional.empty(), retired(List.of(SHELL_TABLE.key(0), ShardKey.of(SHELL, 1, RETIRED)), metadata,
            List.of(ROW)), "a legacy key");
        var foreign = new ArchiveContext(SHELL, "recreated-index-id", RETIRED, "slm-1", BACKING_INDEX, BACKING);
        assertEquals(Optional.empty(), retired(List.of(foreign.key(0)), List.of(foreign.key(-1)), List.of(ROW)));
        var otherName = new ArchiveContext("partial-other", REPO_ID, RETIRED, "slm-1", BACKING_INDEX, BACKING);
        assertEquals(Optional.empty(), retired(List.of(otherName.key(0)), List.of(otherName.key(-1)), List.of(ROW)));
        assertEquals(Optional.empty(), retired(List.of(SHELL_TABLE.key(-1)), metadata, List.of(ROW)),
            "shard -1 is the metadata table's key only");
        assertEquals(Optional.empty(), retired(List.of(), List.of(SHELL_TABLE.key(0)), List.of(ROW)));
    }

    @Test
    void aMissingOrMismatchingMetadataRowIsRefused() throws Exception {
        var keys = List.of(SHELL_TABLE.key(0));
        var metadata = List.of(SHELL_TABLE.key(-1));
        assertEquals(Optional.empty(), retired(keys, List.of(), List.of()));
        assertEquals(Optional.empty(), retired(keys, metadata, List.of()));
        assertEquals(Optional.empty(), retired(keys, metadata, List.of(ROW, ROW)), "exactly one row");
        for (var row : List.of(
            new FullIndexExporter.MetadataRow(SHELL, "slm-2", "slm-2", BACKING),
            new FullIndexExporter.MetadataRow(SHELL, RETIRED, "slm-1", "other-backing"),
            new FullIndexExporter.MetadataRow("partial-other", RETIRED, "slm-1", BACKING),
            new FullIndexExporter.MetadataRow(SHELL, RETIRED, null, BACKING),
            new FullIndexExporter.MetadataRow(SHELL, RETIRED, " ", BACKING),
            new FullIndexExporter.MetadataRow(null, null, null, null))) {
            assertEquals(Optional.empty(), retired(keys, metadata, List.of(row)), row.toString());
        }
    }

    @Test
    void aStillRetainedShellSnapshotIsResumedNormallyNotRelabelled() throws Exception {
        assertEquals(Optional.empty(), retired(List.of(SHELL_TABLE.key(0)), List.of(SHELL_TABLE.key(-1)), List.of(ROW),
            List.of(RETIRED, "slm-2"), BACKING_INDEX, BACKING, 2));
        assertEquals(Optional.of(RETIRED), FullIndexExporter.resumableSnapshot(List.of(SHELL_TABLE.key(0)), SHELL,
            REPO_ID, List.of(RETIRED, "slm-2")));
    }

    @Test
    void exportSettingsRejectOutOfRangeValues() {
        assertThrows(IllegalArgumentException.class, () -> new ExportSettings(SnapshotSelection.AUTO, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new ExportSettings(SnapshotSelection.AUTO, 257, 0));
        assertThrows(IllegalArgumentException.class, () -> new ExportSettings(SnapshotSelection.AUTO, 2, -1));
        assertThrows(NullPointerException.class, () -> new ExportSettings(null, 1, 0));
        assertDoesNotThrow(() -> new ExportSettings(SnapshotSelection.LATEST, 256, 1));
    }

    private static ArchiveContext context(String snapshotUuid) {
        return new ArchiveContext(INDEX, REPO_ID, snapshotUuid, "name-" + snapshotUuid, INDEX, snapshotUuid);
    }
}
