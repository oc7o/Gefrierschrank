package de.sloow.gefrierschrank.delta;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;

import java.util.Iterator;
import java.util.Set;

/**
 * The archive snapshot a writer was checked against, kept current with every new table version.
 * A shard commit verifies the versions its transaction read, and that transaction read the whole
 * table, so any later version fails the commit instead of slipping past this check: the check is
 * atomic with the commit. Mutated only under the writer's commit lock.
 */
final class ArchiveGuard {

    private final Set<String> expectedKeys;
    private final StructType schema;
    private long verifiedVersion = -1;

    ArchiveGuard(Set<String> expectedKeys, StructType schema) {
        this.expectedKeys = Set.copyOf(expectedKeys);
        this.schema = schema;
    }

    long verifiedVersion() {
        return verifiedVersion;
    }

    /** Full check of one snapshot: the archive schema, and every file tagged with an expected key. */
    void verify(Snapshot snapshot) throws DeltaTableWriter.ArchiveTargetConflictException {
        if (snapshot.getVersion() >= 0) {
            if (!schema.equals(snapshot.getMetadata().getSchema())) throw new DeltaTableWriter.ArchiveTargetConflictException();
            for (AddFile file : snapshot.getAllFiles()) requireExpected(file);
        }
        verifiedVersion = Math.max(verifiedVersion, snapshot.getVersion());
    }

    /**
     * Checks the versions after the last verified one, up to {@code version}, reading only their
     * commit files. Falls back to a full check of that version when the log no longer has them.
     */
    void verifyThrough(DeltaLog log, long version) throws DeltaTableWriter.ArchiveTargetConflictException {
        if (version <= verifiedVersion) return;
        long next = verifiedVersion + 1;
        try {
            Iterator<VersionLog> changes = log.getChanges(next, true);
            while (next <= version && changes.hasNext()) {
                VersionLog change = changes.next();
                if (change.getVersion() != next) break;
                for (Action action : change.getActions()) {
                    if (action instanceof AddFile file) requireExpected(file);
                    else if (action instanceof Metadata metadata && !schema.equals(metadata.getSchema()))
                        throw new DeltaTableWriter.ArchiveTargetConflictException();
                }
                next++;
            }
        } catch (DeltaTableWriter.ArchiveTargetConflictException conflict) {
            throw conflict;
        } catch (RuntimeException missingVersions) {
            // getChanges fails on versions removed by log cleanup; the full check below covers them
        }
        if (next <= version) {
            Snapshot snapshot = log.snapshot();
            verify(snapshot.getVersion() == version ? snapshot : log.getSnapshotForVersionAsOf(version));
        } else {
            verifiedVersion = version;
        }
    }

    private void requireExpected(AddFile file) throws DeltaTableWriter.ArchiveTargetConflictException {
        if (file.getTags() == null || !expectedKeys.contains(file.getTags().get(ShardKey.TAG)))
            throw new DeltaTableWriter.ArchiveTargetConflictException();
    }
}
