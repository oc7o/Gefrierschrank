package de.sloow.gefrierschrank.core.repo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;

/**
 * The download of one shard directory in rounds, from {@link ShardMaterializer#begin}: each
 * round downloads and verifies the matching files not downloaded yet, exactly as a full
 * materialize does. Files are only ever added under their own names, so readers already open on
 * other files of the directory are unaffected by a later round. Thread-safe: rounds are
 * serialized, and a file is downloaded at most once unless its round failed.
 */
public final class ShardFetch {

    private final ShardMaterializer materializer;
    private final String shardPrefix;
    private final List<ShardMaterializer.Target> targets;
    private final boolean[] fetched;
    private final LongAdder arrived = new LongAdder();
    private boolean completed;

    ShardFetch(ShardMaterializer materializer, String shardPrefix, List<ShardMaterializer.Target> targets) {
        this.materializer = materializer;
        this.shardPrefix = shardPrefix;
        this.targets = List.copyOf(targets);
        this.fetched = new boolean[targets.size()];
    }

    /**
     * Downloads the files {@code include} accepts that are not downloaded yet. A failed round
     * leaves its files unfetched (a later round downloads them again from the start).
     */
    public synchronized void fetch(Predicate<RepoFileInfo> include) throws IOException {
        List<ShardMaterializer.Target> round = new ArrayList<>();
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < targets.size(); i++) {
            if (!fetched[i] && include.test(targets.get(i).file())) {
                round.add(targets.get(i));
                positions.add(i);
            }
        }
        if (round.isEmpty()) return;
        materializer.fetch(shardPrefix, round, arrived);
        for (int position : positions) fetched[position] = true;
    }

    /** Downloads every file not downloaded yet; returns the bytes downloaded by all rounds. */
    public synchronized long fetchAll() throws IOException {
        fetch(file -> true);
        return fetchedBytes();
    }

    /**
     * Completes a selectively fetched directory in place: downloads and verifies every file not
     * downloaded yet. Idempotent; marks the fetch as {@link #completed}.
     */
    public synchronized void ensureComplete() throws IOException {
        completed = true;
        fetch(file -> true);
    }

    /** Whether {@link #ensureComplete} ran. */
    public synchronized boolean completed() {
        return completed;
    }

    /** Whether every file of the shard is downloaded. */
    public synchronized boolean complete() {
        for (boolean done : fetched) if (!done) return false;
        return true;
    }

    /**
     * Bytes of repository blobs that arrived for this shard, embedded files excluded; during or
     * after a failed round this includes what that round downloaded before it failed.
     */
    public long fetchedBytes() {
        return arrived.sum();
    }
}
