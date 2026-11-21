package de.sloow.gefrierschrank.core.repo;

import java.util.function.Predicate;

/**
 * Which files of a shard to download. {@code FULL} is every file, as without selective fetch;
 * {@code SELECTIVE} leaves out the files of index structures the export never opens;
 * {@code SYNTHETIC_CANDIDATE} is the first step of a synthetic-source shard's plan: the files every
 * plan keeps, after which the fetch probe decides which structures to add (see
 * {@link SelectiveFetch#syntheticPlan}). {@code reason}: the fixed token why a selective-fetch
 * candidate is fetched in full, or null.
 */
public record FetchPlan(Mode mode, Predicate<RepoFileInfo> include, long plannedBytes, String reason) {

    public enum Mode { FULL, SELECTIVE, SYNTHETIC_CANDIDATE }

    public FetchPlan(Mode mode, Predicate<RepoFileInfo> include, long plannedBytes) {
        this(mode, include, plannedBytes, null);
    }

    public static FetchPlan full(ShardSnapshot shard) {
        return full(shard, null);
    }

    public static FetchPlan full(ShardSnapshot shard, String reason) {
        return new FetchPlan(Mode.FULL, file -> true, shard.files().stream().mapToLong(RepoFileInfo::length).sum(), reason);
    }

    public boolean selective() {
        return mode == Mode.SELECTIVE;
    }
}
