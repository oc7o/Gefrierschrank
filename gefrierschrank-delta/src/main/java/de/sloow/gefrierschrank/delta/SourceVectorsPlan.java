package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.syntheticsource.SourceVectors;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Per-index decision whether the stored {@code _source} is complete with respect to vectors.
 *
 * <p>Indices created on ES 9.2+ keep indexed {@code dense_vector} and stored {@code sparse_vector}
 * values out of the stored source by default. Such an index ({@link SourceVectors.Verdict#EXCLUDED})
 * is exported with the vectors put back by Elasticsearch's own loaders per shard
 * ({@link SourceVectorsShardReader}); its rows say so in {@code _source_kind}. An index whose
 * exclusion cannot be rehydrated ({@code rank_vectors}, {@code semantic_text}) or evaluated
 * ({@link SourceVectors.Verdict#UNSUPPORTED}) fails before any table, metadata row or shard
 * download is created, as does every excluding index with {@link #REHYDRATE_PROPERTY} {@code false}.
 * Evaluated once per index metadata instance ({@link #forIndex}).
 *
 * <p>Cheap ambient checks settle the common cases without Elasticsearch: no vector-typed field,
 * an explicit {@code index.mapping.exclude_source_vectors: false}, or an index created before
 * {@code IndexVersions.EXCLUDE_SOURCE_VECTORS_DEFAULT} without the setting explicitly true (ES's
 * exact default). Everything else is decided by ES's mapper code through {@link SourceVectors}.
 */
public final class SourceVectorsPlan {

    /** {@code IndexVersions.EXCLUDE_SOURCE_VECTORS_DEFAULT} in ES 9.3.0 (first 9.2.0 index version is 9039001). */
    static final int EXCLUDE_SOURCE_VECTORS_DEFAULT = 9_035_000;
    static final String EXCLUDE_SETTING = "index.mapping.exclude_source_vectors";

    /**
     * Kill switch: {@code -Dgefrierschrank.vectors.rehydrate=false} refuses every index that excludes
     * vectors, as 1.1.0 did, instead of rehydrating them. Read at every decision; default on.
     */
    public static final String REHYDRATE_PROPERTY = "gefrierschrank.vectors.rehydrate";

    /** AddFile tag on every file of a rehydrating index; a table holding files without it is refused. */
    static final String FILE_TAG = "gefrierschrank.sourceVectors";
    static final String FILE_TAG_VALUE = "rehydrated-v1";

    private static final Memo MEMO = new Memo();

    /** Stand-in for {@link SourceVectors#probe} so tests can prove when the isolated world is (not) used. */
    public interface Prober {
        SourceVectors.Result probe(String indexName, JsonNode metadata) throws IOException;
    }

    private final SourceVectors.Verdict verdict;
    private final Set<String> excludedFields;
    private final Map<String, String> candidates;

    private SourceVectorsPlan(SourceVectors.Verdict verdict, Set<String> excludedFields, Map<String, String> candidates) {
        this.verdict = verdict;
        this.excludedFields = excludedFields;
        this.candidates = candidates;
    }

    /**
     * The index's plan, failing it when its excluded vectors cannot be rehydrated
     * ({@link SourceVectors.Verdict#UNSUPPORTED}) or rehydration is switched off. Called once per
     * index, before anything is written or downloaded.
     */
    public static SourceVectorsPlan require(String indexName, JsonNode metadata) throws ExcludedSourceVectorsException {
        SourceVectorsPlan plan = forIndex(indexName, metadata);
        plan.requireExportable();
        return plan;
    }

    /** Fails unless the stored source is complete or its excluded vectors are rehydrated. */
    void requireExportable() throws ExcludedSourceVectorsException {
        if (verdict == SourceVectors.Verdict.UNSUPPORTED) throw new ExcludedSourceVectorsException();
        if (verdict == SourceVectors.Verdict.EXCLUDED && !rehydrationEnabled()) throw new ExcludedSourceVectorsException(true);
    }

    /**
     * {@link #evaluate} with Elasticsearch's probe, once per metadata instance: the preflight, the
     * pipeline, every shard's reader and the read-ahead estimate of one export share one evaluation.
     * Null metadata is a {@link #none()} plan.
     */
    public static SourceVectorsPlan forIndex(String indexName, JsonNode metadata) {
        if (metadata == null) return none();
        SourceVectorsPlan known = MEMO.get(metadata);
        return known != null ? known : MEMO.putIfAbsent(metadata, evaluate(indexName, metadata, SourceVectors::probe));
    }

    /** A plan that rehydrates nothing. */
    public static SourceVectorsPlan none() {
        return none(Map.of());
    }

    static boolean rehydrationEnabled() {
        return !"false".equalsIgnoreCase(System.getProperty(REHYDRATE_PROPERTY, "true").strip());
    }

    /** Whether the export puts excluded vectors back: {@link SourceVectors.Verdict#EXCLUDED} with the kill switch on. */
    public boolean rehydrates() {
        return verdict == SourceVectors.Verdict.EXCLUDED && rehydrationEnabled();
    }

    static SourceVectorsPlan evaluate(String indexName, JsonNode metadata, Prober prober) {
        Map<String, String> candidates = metadata == null ? Map.of() : SourceVectors.vectorFieldCandidates(metadata);
        if (candidates.isEmpty()) return none(candidates);
        String explicit = RepositorySources.setting(metadata, EXCLUDE_SETTING);
        if ("false".equalsIgnoreCase(explicit)) return none(candidates);
        if (!"true".equalsIgnoreCase(explicit) && createdBeforeExclusionDefault(metadata)) return none(candidates);
        try {
            SourceVectors.Result result = prober.probe(indexName, metadata);
            return new SourceVectorsPlan(result.verdict(), result.excludedFields(), candidates);
        } catch (Exception e) {
            return new SourceVectorsPlan(SourceVectors.Verdict.UNSUPPORTED, Set.of(), candidates);
        }
    }

    private static boolean createdBeforeExclusionDefault(JsonNode metadata) {
        String created = RepositorySources.setting(metadata, "index.version.created");
        if (created == null) return false;
        try {
            return Integer.parseInt(created.strip()) < EXCLUDE_SOURCE_VECTORS_DEFAULT;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static SourceVectorsPlan none(Map<String, String> candidates) {
        return new SourceVectorsPlan(SourceVectors.Verdict.NONE, Set.of(), candidates);
    }

    public SourceVectors.Verdict verdict() {
        return verdict;
    }

    /** The fields Elasticsearch keeps out of the stored source, by full path. */
    public Set<String> excludedFields() {
        return excludedFields;
    }

    /** Vector-typed fields found in the mapping: full path to type. */
    Map<String, String> candidates() {
        return candidates;
    }

    /**
     * The index's stored {@code _source} lacks vectors that cannot be rehydrated, or rehydration is
     * switched off. Fixed message: field names and document content never reach reports.
     */
    public static final class ExcludedSourceVectorsException extends IOException
            implements de.sloow.gefrierschrank.core.FailureDiagnostics.SafeMessage {
        public ExcludedSourceVectorsException() {
            super("stored _source excludes vector fields that cannot be rehydrated (rank_vectors, semantic_text, "
                + "or the exclusion could not be evaluated), so the index is not exported");
        }

        /** The 1.1.0 refusal, for every excluding index while {@link #REHYDRATE_PROPERTY} is false. */
        ExcludedSourceVectorsException(boolean rehydrationSwitchedOff) {
            super("stored _source excludes vector fields (index.mapping.exclude_source_vectors); "
                + "exporting it would lose their values, so the index is not exported");
        }
    }

    /** Plans by metadata identity, held only as long as the metadata itself. */
    private static final class Memo {
        private final Map<Integer, List<Entry>> entries = new HashMap<>();
        private final ReferenceQueue<JsonNode> cleared = new ReferenceQueue<>();

        private static final class Entry extends WeakReference<JsonNode> {
            final int hash;
            final SourceVectorsPlan plan;

            Entry(JsonNode metadata, SourceVectorsPlan plan, ReferenceQueue<JsonNode> queue) {
                super(metadata, queue);
                this.hash = System.identityHashCode(metadata);
                this.plan = plan;
            }
        }

        synchronized SourceVectorsPlan get(JsonNode metadata) {
            expunge();
            for (Entry entry : entries.getOrDefault(System.identityHashCode(metadata), List.of())) {
                if (entry.get() == metadata) return entry.plan;
            }
            return null;
        }

        /** The plan already held for {@code metadata}, else {@code plan} after recording it. */
        synchronized SourceVectorsPlan putIfAbsent(JsonNode metadata, SourceVectorsPlan plan) {
            SourceVectorsPlan known = get(metadata);
            if (known != null) return known;
            entries.computeIfAbsent(System.identityHashCode(metadata), hash -> new ArrayList<>())
                .add(new Entry(metadata, plan, cleared));
            return plan;
        }

        private void expunge() {
            for (var reference = cleared.poll(); reference != null; reference = cleared.poll()) {
                Entry entry = (Entry) reference;
                List<Entry> bucket = entries.get(entry.hash);
                if (bucket == null) continue;
                bucket.remove(entry);
                if (bucket.isEmpty()) entries.remove(entry.hash);
            }
        }
    }
}
