package de.sloow.gefrierschrank.core.repo;

import java.util.List;

/**
 * Full contents of a shard-level {@code snap-<snapshot-uuid>.dat}: every file in one
 * shard snapshot. {@code shardPrefix} is the repository-internal directory the file
 * blob names resolve against.
 */
public record ShardSnapshot(
    String repoUuid,
    String shardPrefix,
    String snapshotName,
    int indexVersion,
    long totalSize,
    List<RepoFileInfo> files
) {}
