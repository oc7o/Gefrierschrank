package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.CommitPoint;
import de.sloow.gefrierschrank.core.repo.FetchPlan;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.ShardFetch;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.syntheticsource.SyntheticFetchProbe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Set;

/**
 * Downloads and opens one shard for dump-to-delta. Without {@code --selective-fetch} this is the
 * full download and the normal open, exactly as before. With it, the commit files come first:
 * the commit-point preflight may fail the shard there, and an eligible stored-source shard then
 * downloads only the files its plan names and opens them pruned. A shard whose live documents
 * Elasticsearch's loaders reconstruct downloads the files every plan keeps and then asks the fetch
 * probe which further structures the reconstruction reads, adding them until it asks for nothing
 * new (or is fetched in full for a fixed reason). A pruned open that fails completes the download
 * in place and opens normally. Returns the unwrapped runtime reader.
 */
final class ShardOpening {

    /**
     * The plan and the reader opened over its files. {@code fullReason}: why a synthetic-source
     * candidate was fetched in full, or null; {@code probes}: the probe calls of a synthetic-source
     * candidate, with their wall time and the structures kept.
     */
    record Opened(ShardReader rawReader, ShardFetch fetch, FetchPlan.Mode plan, String fullReason, Probing probes) {
        Opened(ShardReader rawReader, ShardFetch fetch, FetchPlan.Mode plan) {
            this(rawReader, fetch, plan, null, null);
        }

        boolean selective() {
            return plan == FetchPlan.Mode.SELECTIVE;
        }
    }

    /** What the fetch probe of a synthetic-source candidate did; content-free. */
    record Probing(int calls, long millis, Set<SelectiveFetch.Structure> kept) {}

    /** The fetch probe; a test seam. */
    @FunctionalInterface
    interface Prober {
        SyntheticFetchProbe.Result probe(Path shardDir, String indexName, JsonNode metadata);
    }

    static volatile Prober prober = SyntheticFetchProbe::probe;

    /** What an attempt downloaded, readable also after opening failed: the report's {@code fetch} and {@code bytesFetched}. */
    static final class Tracker {
        private volatile ShardFetch fetch;
        private volatile boolean downloading;
        private volatile FetchPlan.Mode plan;
        private volatile boolean preflightRefused;

        long bytesFetched() {
            ShardFetch current = fetch;
            return current == null ? 0 : current.fetchedBytes();
        }

        String fetchMode() {
            if (preflightRefused) return "preflight";
            ShardFetch current = fetch;
            if (current == null || !downloading) return "none";
            if (plan != FetchPlan.Mode.SELECTIVE) return "full";
            return current.completed() ? "completed" : "selective";
        }

        boolean preflightRefused() {
            return preflightRefused;
        }
    }

    private ShardOpening() {}

    static Opened open(ShardMaterializer materializer, DumpToDeltaPipeline.ShardReaderOpener opener, ShardSnapshot shard,
                       Path shardDir, String indexName, JsonNode metadata) throws Exception {
        return open(materializer, opener, shard, shardDir, indexName, metadata, new Tracker());
    }

    static Opened open(ShardMaterializer materializer, DumpToDeltaPipeline.ShardReaderOpener opener, ShardSnapshot shard,
                       Path shardDir, String indexName, JsonNode metadata, Tracker tracker) throws Exception {
        return open(materializer, opener, shard, shardDir, indexName, metadata, SelectiveFetch.preflightEnabled(), tracker);
    }

    /** Test seam: {@code preflight} in place of {@link SelectiveFetch#preflightEnabled()}. */
    static Opened open(ShardMaterializer materializer, DumpToDeltaPipeline.ShardReaderOpener opener, ShardSnapshot shard,
                       Path shardDir, String indexName, JsonNode metadata, boolean preflight, Tracker tracker)
            throws Exception {
        ShardFetch fetch = materializer.begin(shard, shardDir);
        tracker.fetch = fetch;
        boolean selective = SelectiveFetch.enabled() && SelectiveEligibility.of(indexName, metadata);
        CommitPoint commit = null;
        if (preflight || selective) {
            tracker.downloading = true;
            fetch.fetch(file -> SelectiveFetch.isCommitFile(file.physicalName()));
            try {
                commit = opener.readCommitPoint(shard, shardDir);
            } catch (Exception unreadable) {
                // The full download and the normal open surface the real error, if there is one.
                commit = null;
            }
            if (commit != null && preflight) {
                try {
                    CommitPointPreflight.require(commit, metadata, indexName, shardDir);
                } catch (Exception refused) {
                    tracker.preflightRefused = true;
                    throw refused;
                }
            }
        }
        if (commit != null && SelectiveFetch.enabled() && SelectiveEligibility.reconstructs(metadata, commit)) {
            return openReconstructed(opener, shard, shardDir, indexName, metadata, commit, fetch, tracker);
        }
        FetchPlan plan = commit != null ? SelectiveFetch.plan(commit, shard, selective) : FetchPlan.full(shard);
        tracker.plan = plan.mode();
        tracker.downloading = true;
        fetch.fetch(plan.include());
        if (!plan.selective()) return new Opened(opener.open(shard, shardDir, metadata), fetch, plan.mode());
        return new Opened(openPruned(opener, shard, shardDir, metadata, fetch), fetch, plan.mode());
    }

    /**
     * The synthetic-source plan: the files every plan keeps, then the probe's fixpoint. Each probe
     * round runs with no other reader open on the directory and sees real files for every structure
     * fetched so far; a structure it asks for is fetched (verified as every download) and the probe
     * asks again. The plan is the set of structures once a round asks for nothing new. A failing
     * round that asked for nothing is conclusive (FULL, probe-failed); one that asked for something
     * may have failed on the stubs' empty answers and is retried with those structures. Monotone
     * over five structures: at most six probe calls and five fetch rounds.
     */
    private static Opened openReconstructed(DumpToDeltaPipeline.ShardReaderOpener opener, ShardSnapshot shard, Path shardDir,
                                            String indexName, JsonNode metadata, CommitPoint commit, ShardFetch fetch,
                                            Tracker tracker) throws Exception {
        FetchPlan first = SelectiveFetch.syntheticPlan(commit, shard,
            SelectiveEligibility.vectorsFromVectorFiles(indexName, metadata));
        if (first.mode() == FetchPlan.Mode.FULL) return openFull(opener, shard, shardDir, metadata, fetch, tracker, first.reason(), null);
        tracker.plan = FetchPlan.Mode.SELECTIVE;
        tracker.downloading = true;
        fetch.fetch(first.include());
        Set<SelectiveFetch.Structure> kept = EnumSet.noneOf(SelectiveFetch.Structure.class);
        long started = System.nanoTime();
        int calls = 0;
        while (true) {
            SyntheticFetchProbe.Result result = prober.probe(shardDir, indexName, metadata);
            calls++;
            Set<SelectiveFetch.Structure> missing = EnumSet.noneOf(SelectiveFetch.Structure.class);
            for (String name : result.touched()) missing.add(SelectiveFetch.Structure.valueOf(name));
            missing.removeAll(kept);
            Probing probing = new Probing(calls, (System.nanoTime() - started) / 1_000_000, Set.copyOf(kept));
            if (missing.isEmpty()) {
                if (result.failed()) {
                    return openFull(opener, shard, shardDir, metadata, fetch, tracker, SelectiveFetch.FULL_PROBE_FAILED, probing);
                }
                return new Opened(openPruned(opener, shard, shardDir, metadata, fetch), fetch, FetchPlan.Mode.SELECTIVE, null, probing);
            }
            if (calls > SelectiveFetch.Structure.values().length) {
                return openFull(opener, shard, shardDir, metadata, fetch, tracker, SelectiveFetch.FULL_PROBE_UNSTABLE, probing);
            }
            kept.addAll(missing);
            fetch.fetch(SelectiveFetch.keeping(commit, shard, kept).include());
        }
    }

    private static Opened openFull(DumpToDeltaPipeline.ShardReaderOpener opener, ShardSnapshot shard, Path shardDir,
                                   JsonNode metadata, ShardFetch fetch, Tracker tracker, String reason, Probing probing)
            throws Exception {
        tracker.plan = FetchPlan.Mode.FULL;
        tracker.downloading = true;
        fetch.fetchAll();
        return new Opened(opener.open(shard, shardDir, metadata), fetch, FetchPlan.Mode.FULL, reason, probing);
    }

    private static ShardReader openPruned(DumpToDeltaPipeline.ShardReaderOpener opener, ShardSnapshot shard, Path shardDir,
                                          JsonNode metadata, ShardFetch fetch) throws Exception {
        try {
            return opener.openPruned(shard, shardDir, metadata);
        } catch (IOException | RuntimeException prunedOpenFailed) {
            try {
                fetch.ensureComplete();
                return opener.open(shard, shardDir, metadata);
            } catch (Exception | Error normalOpenFailed) {
                normalOpenFailed.addSuppressed(prunedOpenFailed);
                throw normalOpenFailed;
            }
        }
    }
}
