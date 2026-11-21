package de.sloow.gefrierschrank.delta;

/**
 * Resume-key format for shard-level Delta commit tagging. Centralized here so the
 * "build the key" side (DumpToDeltaPipeline, before materializing a shard) and the
 * "read the key back" side (DeltaTableWriter's AddFile tag, and the pre-scan of
 * already-committed shards) can never drift out of sync - a format mismatch between
 * the two would silently break resumability.
 */
final class ShardKey {

    /** Tag key under which the shard identity is stored on every committed AddFile. */
    static final String TAG = "gefrierschrank.shard";

    private ShardKey() {}

    /**
     * Includes snapshotName, not just indexName+shardId: if someone resumes against a
     * different ES snapshot of the same index name (e.g. a newer backup taken later),
     * keying only on index+shard would make the tool wrongly believe those shards are
     * already done, silently skipping real work and producing an incomplete table.
     */
    static String of(String indexName, int shardId, String snapshotName) {
        return indexName + "#" + shardId + "#" + snapshotName;
    }
}
