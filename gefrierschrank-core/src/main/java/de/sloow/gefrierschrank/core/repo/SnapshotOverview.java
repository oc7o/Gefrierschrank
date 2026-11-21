package de.sloow.gefrierschrank.core.repo;

/**
 * {@code startTimeMillis} is -1 when the repository format doesn't carry it (very old
 * repositories); used to pick the CURRENT/most-recently-taken snapshot when several retained
 * snapshots reference the same index (e.g. an index name reused after delete+recreate, or a
 * shard re-flushed between snapshots) - see {@link RepositoryInspector#shardSnapshot}.
 *
 * <p>{@code version} is the snapshot's {@code version} field, which Elasticsearch has written as
 * "8.11.0" for every snapshot since 8.11; {@code indexVersion} is its {@code index_version}
 * (e.g. 8521000 for 8.17, 9060000 for 9.3), or -1 for snapshots taken before 8.11.
 */
public record SnapshotOverview(String name, String uuid, int state, String version, long startTimeMillis,
                               long indexVersion) {

    public SnapshotOverview(String name, String uuid, int state, String version, long startTimeMillis) {
        this(name, uuid, state, version, startTimeMillis, -1);
    }
}
