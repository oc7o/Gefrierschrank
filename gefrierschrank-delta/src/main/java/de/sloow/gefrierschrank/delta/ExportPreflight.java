package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.syntheticsource.SourceVectors;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Index-level checks that run on the resolved (backing) index metadata before any table,
 * metadata row or shard download exists. {@link #require} fails indices that provably cannot be
 * exported, so they no longer download and quarantine every shard on every run;
 * {@link #classify} gives the {@code survey} verdict from the same predicates, so survey and
 * export cannot drift apart. Stateless and thread-safe.
 */
public final class ExportPreflight {

    /**
     * Narrowed in 1.2.0: an index that is provably unexportable FAILS before any
     * download: created by ES 6.x or older; {@code _source.enabled: false} in a snapshot written by
     * ES 7 or older, or {@code index.mapping.source.mode: disabled} on an index created before 8.0
     * (no segment can hold stored source, none can be reconstructed); an ES 8+ index created with
     * that setting whose mapping the reconstruction coverage rule refuses. Exit 1 with
     * {@code --index}, FAILED and exit 4 with {@code --all-indices}; an empty such index no longer
     * completes with 0 rows. {@code false} restores the per-shard download-then-quarantine
     * behaviour; the excluded-vectors check runs either way.
     */
    public static final boolean FAIL_BEFORE_DOWNLOAD = true;

    /**
     * {@code index.version.created} below this is ES 6.x or older: Lucene 7 segments, which no
     * reader here opens (created-major floor 8). ES 7.0.0 pre-releases created 7000001-7000099,
     * so the floor is 7_000_000, not 7_000_099.
     */
    static final long MIN_CREATED_VERSION = 7_000_000;

    /**
     * Field types the isolated MapperService used for synthetic-source reconstruction can build:
     * Elasticsearch core, mapper-extras and the nine vendored x-pack types. A synthetic index
     * using any other type fails reconstruction with "No mapper found for type". Kept equal to
     * the registry by ExportPreflightTest.
     */
    public static final Set<String> SYNTHETIC_MAPPER_TYPES = Set.of(
        "aggregate_metric_double", "alias", "binary", "boolean", "byte", "completion", "constant_keyword",
        "counted_keyword", "date", "date_nanos", "date_range", "dense_vector", "double", "double_range",
        "flattened", "float", "float_range", "geo_point", "geo_shape", "half_float", "histogram", "integer",
        "integer_range", "ip", "ip_range", "keyword", "long", "long_range", "match_only_text", "nested",
        "object", "passthrough", "rank_feature", "rank_features", "scaled_float", "search_as_you_type",
        "shape", "short", "sparse_vector", "text", "token_count", "unsigned_long", "version", "wildcard");

    private ExportPreflight() {}

    /**
     * Whether a synthetic-source index using {@code type} can export, as {@link #classify} judges
     * it: the type needs a mapper. Nested objects export with their roots, as Elasticsearch renders them.
     */
    public static boolean syntheticExportable(String type) {
        return SYNTHETIC_MAPPER_TYPES.contains(type);
    }

    /**
     * Fails the index, in this order, when it was created by ES 6.x or older
     * ({@link IndexCreatedTooOldException}); when its {@code _source} was provably never stored
     * ({@link SourceDisabledException}, {@code DISABLED}); for an index created with
     * {@code index.mapping.source.mode: disabled} on ES 8+ ({@code DISABLED_FROM_CREATION}), when
     * the mapping-level coverage rule refuses it ({@code UnreconstructableSourceException} or the
     * reconstruction setup's own failure); then when ES excluded vectors from the stored source
     * that cannot be rehydrated ({@code rank_vectors}, {@code semantic_text}, an exclusion that
     * cannot be evaluated) or while rehydration is switched off
     * ({@link SourceVectorsPlan.ExcludedSourceVectorsException}, {@link SourceVectorsPlan#require}).
     * An index whose excluded vectors are rehydrated passes. Missing or unparseable metadata falls
     * through to the per-shard path.
     */
    public static void require(String indexName, RepositorySources.Resolved resolved) throws IOException {
        JsonNode metadata = resolved.metadata();
        if (FAIL_BEFORE_DOWNLOAD) {
            if (createdTooOld(metadata)) throw new IndexCreatedTooOldException();
            SourceRetention retention = SourceRetention.of(metadata, snapshotMajor(resolved));
            if (retention == SourceRetention.DISABLED) throw new SourceDisabledException();
            if (retention == SourceRetention.DISABLED_FROM_CREATION)
                de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage.requireMappingReconstructable(indexName, metadata);
            if (retention == SourceRetention.DISABLED_FROM_CREATION || retention == SourceRetention.DISABLED_SINCE_UNKNOWN) {
                String note = de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage.unmappedValuesNote(indexName, metadata);
                if (note != null) System.err.println(note);
            }
        }
        SourceVectorsPlan.require(indexName, metadata);
    }

    /** What dump-to-delta will do with an index, derived from the same predicates as {@link #require}. */
    public enum Kind {
        /** Stored {@code _source}, exported byte-exact. */
        EXPORTABLE,
        /** Synthetic source: rows are reconstructed, not byte-exact. */
        EXPORTABLE_SYNTHETIC,
        /** Refused before download, or every shard quarantines; {@link Verdict#reason()} says why. */
        NOT_EXPORTABLE,
        /**
         * {@code _source} is disabled on an ES 8+ snapshot (changed on an existing index, or the
         * index was created with {@code index.mapping.source.mode: disabled}). Stored documents
         * export byte-exact; documents without stored {@code _source} are reconstructed through
         * Elasticsearch's loaders (synthetic, not byte-exact) where every field holding data has
         * doc values or a stored field; a shard where that fails quarantines.
         */
        WARN_SOURCE_DISABLED_ES8,
        /**
         * Metadata or the vector probe could not be evaluated. Never exportable by default. The
         * real {@link SourceVectors#probe} never throws: it reports its own failures as
         * UNSUPPORTED, which the export refuses too, so they classify as
         * NOT_EXPORTABLE(excluded_vectors). probe_failed comes only from a prober that throws.
         */
        UNKNOWN
    }

    /**
     * {@code reason} is a fixed token for NOT_EXPORTABLE and UNKNOWN (too_old, source_disabled,
     * excluded_vectors, semantic_text_synthetic, unsupported_synthetic_type, metadata_unreadable,
     * probe_failed), else null. {@code detail} is a short content-free
     * explanation: setting names, field type names, class names.
     */
    public record Verdict(Kind kind, String reason, String detail) {}

    /** {@link #classify(String, RepositorySources.Resolved, SourceVectorsPlan.Prober)} with the real vector probe. */
    public static Verdict classify(String indexName, RepositorySources.Resolved resolved) {
        return classify(indexName, resolved, SourceVectors::probe);
    }

    /**
     * Never throws: anything unexpected is {@link Kind#UNKNOWN}. An index that keeps its stored
     * {@code _source} and whose excluded vectors are rehydrated is {@link Kind#EXPORTABLE} with a
     * detail naming how many vector fields come back from the vector files; one whose {@code _source}
     * was disabled after creation gets the verdict of that state, as its source-less documents
     * have nothing to rehydrate into.
     */
    public static Verdict classify(String indexName, RepositorySources.Resolved resolved, SourceVectorsPlan.Prober prober) {
        try {
            return classify(indexName, resolved.metadata(), snapshotMajor(resolved), prober);
        } catch (RuntimeException e) {
            return new Verdict(Kind.UNKNOWN, "metadata_unreadable", e.getClass().getSimpleName());
        }
    }

    static Verdict classify(String indexName, JsonNode metadata, int snapshotMajor, SourceVectorsPlan.Prober prober) {
        if (metadata == null || !metadata.isObject())
            return new Verdict(Kind.UNKNOWN, "metadata_unreadable", "no index metadata object");
        if (createdTooOld(metadata))
            return new Verdict(Kind.NOT_EXPORTABLE, "too_old",
                "created by Elasticsearch 6.x or older (index.version.created " + createdVersion(metadata) + ")");
        SourceRetention retention = SourceRetention.of(metadata, snapshotMajor);
        if (retention == SourceRetention.DISABLED)
            return new Verdict(Kind.NOT_EXPORTABLE, "source_disabled", "_source was never stored");

        boolean[] probeFailed = {false};
        SourceVectorsPlan vectors = SourceVectorsPlan.evaluate(indexName, metadata, (name, meta) -> {
            try {
                return prober.probe(name, meta);
            } catch (Exception | LinkageError e) {
                probeFailed[0] = true;
                return new SourceVectors.Result(SourceVectors.Verdict.UNSUPPORTED, Set.of());
            }
        });
        if (probeFailed[0]) return new Verdict(Kind.UNKNOWN, "probe_failed", "vector probe failed");
        int excluded = vectors.excludedFields().size();
        if (vectors.rehydrates()) {
            // Rehydration needs stored _source; any other retention falls through to its own verdict.
            if (retention == SourceRetention.RETAINED)
                return new Verdict(Kind.EXPORTABLE, null, excluded + " vector field(s) rehydrated from the vector files"
                    + " (index.mapping.exclude_source_vectors)");
        } else if (vectors.verdict() == SourceVectors.Verdict.EXCLUDED) {
            return new Verdict(Kind.NOT_EXPORTABLE, "excluded_vectors", excluded + " vector field" + (excluded == 1 ? "" : "s")
                + " kept out of _source (index.mapping.exclude_source_vectors; rehydration switched off)");
        } else if (vectors.verdict() == SourceVectors.Verdict.UNSUPPORTED) {
            return new Verdict(Kind.NOT_EXPORTABLE, "excluded_vectors",
                "rank_vectors or semantic_text kept out of _source, or the exclusion could not be evaluated");
        }

        if (retention == SourceRetention.DISABLED_SINCE_UNKNOWN)
            return new Verdict(Kind.WARN_SOURCE_DISABLED_ES8, null,
                "_source disabled after creation (ES 8+): documents written since are reconstructed (synthetic, not byte-exact)"
                    + " where every field holding data has doc values or a stored field; other such shards quarantine");
        if (retention == SourceRetention.DISABLED_FROM_CREATION)
            return new Verdict(Kind.WARN_SOURCE_DISABLED_ES8, null,
                "_source disabled from creation (index.mapping.source.mode: disabled): every document is reconstructed"
                    + " (synthetic, not byte-exact) where the mapping allows it; a mapping-level refusal fails the index before"
                    + " download, a per-segment one quarantines the shard");

        String synthetic = syntheticSourceSignal(metadata);
        if (synthetic == null) return new Verdict(Kind.EXPORTABLE, null, "");
        Set<String> types = fieldTypes(documentMapping(metadata));
        if (types.contains("semantic_text"))
            return new Verdict(Kind.NOT_EXPORTABLE, "semantic_text_synthetic", "semantic_text under " + synthetic);
        Set<String> unsupported = new TreeSet<>(types);
        unsupported.removeAll(SYNTHETIC_MAPPER_TYPES);
        if (!unsupported.isEmpty())
            return new Verdict(Kind.NOT_EXPORTABLE, "unsupported_synthetic_type",
                "no synthetic mapper for type " + String.join(" ", unsupported));
        return new Verdict(Kind.EXPORTABLE_SYNTHETIC, null, "reconstructed, not byte-exact (" + synthetic + ")");
    }

    /**
     * What the metadata proves about stored {@code _source}. The setting
     * {@code index.mapping.source.mode} is Final: with {@code disabled} no segment ever held stored
     * source, so an index created by ES 8+ is reconstructed from its doc values and stored fields
     * ({@code DISABLED_FROM_CREATION}) unless the kill switch is off, and an older one cannot be
     * ({@code DISABLED}). The mapping's {@code _source.enabled} cannot flip from true to false on
     * ES 7 or older, but ES 8.17 and 9.3 accept that update (and 8.x accepts {@code _source.mode}
     * changes), so on a newer snapshot a disabled mapping does not mean no segment holds stored
     * source ({@code DISABLED_SINCE_UNKNOWN}).
     */
    enum SourceRetention {
        RETAINED, DISABLED, DISABLED_SINCE_UNKNOWN, DISABLED_FROM_CREATION;

        static SourceRetention of(JsonNode metadata, int snapshotMajor) {
            if ("disabled".equalsIgnoreCase(RepositorySources.setting(metadata, "index.mapping.source.mode"))) {
                long created = createdVersion(metadata);
                boolean beforeEs8 = (snapshotMajor >= 0 && snapshotMajor < 8) || (created >= 0 && created < 8_000_099);
                return beforeEs8 || !de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage.reconstructsDisabledSource()
                    ? DISABLED : DISABLED_FROM_CREATION;
            }
            JsonNode source = documentMapping(metadata).path("_source");
            if (isFalse(source.path("enabled")))
                return snapshotMajor >= 0 && snapshotMajor < 8 ? DISABLED : DISABLED_SINCE_UNKNOWN;
            if ("disabled".equalsIgnoreCase(source.path("mode").asText(""))) return DISABLED_SINCE_UNKNOWN;
            return RETAINED;
        }
    }

    private static boolean isFalse(JsonNode node) {
        return node.isBoolean() ? !node.booleanValue() : node.isTextual() && "false".equals(node.textValue());
    }

    static boolean createdTooOld(JsonNode metadata) {
        long created = createdVersion(metadata);
        return created >= 0 && created < MIN_CREATED_VERSION;
    }

    /** {@code index.version.created}, or -1 when missing or unparseable. */
    private static long createdVersion(JsonNode metadata) {
        String created = RepositorySources.setting(metadata, "index.version.created");
        if (created == null) return -1;
        try {
            return Long.parseLong(created.strip());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /** Major of the ES release that wrote the resolved snapshot, or -1 when unknown. */
    private static int snapshotMajor(RepositorySources.Resolved resolved) {
        try {
            return major(resolved.source().inspector().requireSnapshot(resolved.indexName(), resolved.snapshotUuid()).version());
        } catch (IOException e) {
            return -1;
        }
    }

    static int major(String version) {
        if (version == null) return -1;
        int dot = version.indexOf('.');
        try {
            return Integer.parseInt(dot < 0 ? version.strip() : version.substring(0, dot).strip());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * The first signal that decides the source mode: the setting {@code index.mapping.source.mode},
     * else the mapping's {@code _source.mode}, else {@code index.mode} time_series or logsdb
     * (synthetic by default). Returns the deciding setting when it means synthetic, else null.
     */
    static String syntheticSourceSignal(JsonNode metadata) {
        String setting = RepositorySources.setting(metadata, "index.mapping.source.mode");
        if (setting != null)
            return "synthetic".equalsIgnoreCase(setting.strip()) ? "index.mapping.source.mode=synthetic" : null;
        JsonNode mappingMode = documentMapping(metadata).path("_source").path("mode");
        if (mappingMode.isValueNode() && !mappingMode.isNull())
            return "synthetic".equalsIgnoreCase(mappingMode.asText().strip()) ? "_source.mode=synthetic" : null;
        String indexMode = RepositorySources.setting(metadata, "index.mode");
        if (indexMode == null) return null;
        String mode = indexMode.strip().toLowerCase(Locale.ROOT);
        return mode.equals("time_series") || mode.equals("logsdb") ? "index.mode=" + mode : null;
    }

    /** The document mapping, as reconstruction reads it: {@code mappings[0]}, then {@code _doc} or flat. */
    static JsonNode documentMapping(JsonNode metadata) {
        JsonNode mappings = metadata.path("mappings");
        if (mappings.isArray()) mappings = mappings.path(0);
        return mappings.has("_doc") ? mappings.path("_doc") : mappings;
    }

    /**
     * Every field type the mapping declares, recursing into object properties and multi-fields
     * like {@code field-types}; an object field without a type counts as {@code object}.
     */
    public static Set<String> fieldTypes(JsonNode documentMapping) {
        Set<String> types = new TreeSet<>();
        collectTypes(documentMapping.path("properties"), types);
        return types;
    }

    private static void collectTypes(JsonNode properties, Set<String> into) {
        if (!properties.isObject()) return;
        Iterator<Map.Entry<String, JsonNode>> fields = properties.fields();
        while (fields.hasNext()) {
            JsonNode definition = fields.next().getValue();
            String type = definition.path("type").asText("");
            if (!type.isEmpty()) into.add(type);
            else if (definition.has("properties")) into.add("object");
            collectTypes(definition.path("properties"), into);
            collectTypes(definition.path("fields"), into);
        }
    }

    /** Created by Elasticsearch 6.x or older. Fixed message: no metadata content. */
    public static final class IndexCreatedTooOldException extends IOException
            implements de.sloow.gefrierschrank.core.FailureDiagnostics.SafeMessage {
        public IndexCreatedTooOldException() {
            super("index was created by Elasticsearch 6.x or older (index.version.created < 7000000); "
                + "its segments cannot be read, so the index is not exported");
        }
    }

    /** {@code _source} was never stored and cannot be reconstructed. Fixed message: no metadata content. */
    public static final class SourceDisabledException extends IOException
            implements de.sloow.gefrierschrank.core.FailureDiagnostics.SafeMessage {
        public SourceDisabledException() {
            super("index does not store _source and its segments cannot be reconstructed (_source.enabled false in a "
                + "snapshot written by Elasticsearch 7 or older, or index.mapping.source.mode disabled on an index created "
                + "before 8.0), so the index is not exported");
        }
    }
}
