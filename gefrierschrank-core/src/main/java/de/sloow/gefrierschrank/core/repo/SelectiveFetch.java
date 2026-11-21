package de.sloow.gefrierschrank.core.repo;

import de.sloow.gefrierschrank.core.FailureDiagnostics;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Selective fetch of stored-source shards: the switch, and the name-based file plan built from
 * the commit facts ({@link CommitPoint}). A non-compound segment's files of the index structures
 * a stored-source export never opens (postings, points, norms, term vectors, vectors) stay in the
 * repository; everything else is downloaded and verified as before, unknown extensions included.
 */
public final class SelectiveFetch {

    /** {@code true|false}, case-insensitive; set by dump-to-delta's {@code --selective-fetch}. */
    public static final String PROPERTY = "gefrierschrank.download.selective";

    /**
     * Whether every shard reads its commit files first and runs the commit-point preflight even
     * without the flag. Off: without the flag, shards download exactly as before.
     */
    public static final boolean COMMIT_PREFLIGHT = false;

    /**
     * Extensions of the formats the pruned reader replaces by throwing stubs, per Lucene 8-10 and
     * Elasticsearch format: postings and block-tree terms, Elasticsearch's bloom-filter postings,
     * points, norms, term vectors, kNN vectors (flat, HNSW, quantized, Elasticsearch's), and the
     * stored-fields bloom sidecar of Elasticsearch 9.3's TSDB codec, which the ambient codec
     * never opens. Pinned against the Lucene codecs by PrunedCodecTest.
     */
    public static final Set<String> PRUNED_EXTENSIONS = Set.of(
        "doc", "pos", "pay", "psm", "tim", "tip", "tmd",
        "bfi", "bfm",
        "kdd", "kdi", "kdm", "dim", "dii",
        "nvd", "nvm",
        "tvd", "tvx", "tvm",
        "vec", "vex", "vem", "vemf", "veq", "vemq", "veb", "vemb", "cenivf", "clivf", "mivf", "vfi",
        "sfbf", "sfbfm");

    /**
     * The index structures a reader can do without, each a format of the segment's codec. Granularity
     * is the structure per segment: per-field postings and vector formats share their files across
     * fields, so leaving out single fields would save nothing.
     */
    public enum Structure { POSTINGS, POINTS, NORMS, TERM_VECTORS, VECTORS }

    /**
     * The structure each pruned extension belongs to. The stored-fields bloom sidecar
     * ({@code sfbf/sfbfm}) is no structure: Elasticsearch's own TSDB codec opens it with the stored
     * fields, so only the stored-source plan, read by the ambient codec, leaves it out.
     */
    public static final Map<String, Structure> STRUCTURES = structures();

    private static Map<String, Structure> structures() {
        Map<String, Structure> map = new HashMap<>();
        for (String extension : List.of("doc", "pos", "pay", "psm", "tim", "tip", "tmd", "bfi", "bfm"))
            map.put(extension, Structure.POSTINGS);
        for (String extension : List.of("kdd", "kdi", "kdm", "dim", "dii")) map.put(extension, Structure.POINTS);
        for (String extension : List.of("nvd", "nvm")) map.put(extension, Structure.NORMS);
        for (String extension : List.of("tvd", "tvx", "tvm")) map.put(extension, Structure.TERM_VECTORS);
        for (String extension : List.of("vec", "vex", "vem", "vemf", "veq", "vemq", "veb", "vemb", "cenivf", "clivf", "mivf", "vfi"))
            map.put(extension, Structure.VECTORS);
        return Map.copyOf(map);
    }

    /** The structure a file of a non-compound segment belongs to, or null for files every reader opens. */
    public static Structure structureOf(String fileName) {
        String extension = extension(fileName);
        return extension == null ? null : STRUCTURES.get(extension);
    }

    /**
     * Bytes of prunable structures a non-compound segment with live documents must hold before a
     * synthetic-source shard is probed at all ({@value #SYNTHETIC_PROBE_MIN_BYTES_PROPERTY}, default
     * {@value #DEFAULT_SYNTHETIC_PROBE_MIN_BYTES}): the probe builds a MapperService and opens the
     * shard, which a small saving does not pay for.
     */
    public static final String SYNTHETIC_PROBE_MIN_BYTES_PROPERTY = "gefrierschrank.download.selective.syntheticMinBytes";
    public static final long DEFAULT_SYNTHETIC_PROBE_MIN_BYTES = 16L << 20;

    /**
     * Why a synthetic-source candidate is fetched in full; fixed, content-free tokens. Rules F1, F2
     * and F3 are those of {@link #syntheticPlan}; F5 is a probe that fails with nothing left to
     * fetch ({@code probe-failed}) or reaches no fixpoint ({@code probe-unstable}).
     */
    public static final String FULL_VECTORS = "vectors";
    public static final String FULL_BELOW_THRESHOLD = "below-threshold";
    public static final String FULL_LEGACY_NESTED = "legacy-nested";
    public static final String FULL_PROBE_FAILED = "probe-failed";
    public static final String FULL_PROBE_UNSTABLE = "probe-unstable";

    private SelectiveFetch() {}

    /** {@value #SYNTHETIC_PROBE_MIN_BYTES_PROPERTY}; a value that is not a non-negative number is a usage error. */
    public static long syntheticProbeMinBytes() {
        String value = System.getProperty(SYNTHETIC_PROBE_MIN_BYTES_PROPERTY);
        if (value == null) return DEFAULT_SYNTHETIC_PROBE_MIN_BYTES;
        try {
            long bytes = Long.parseLong(value.strip());
            if (bytes >= 0) return bytes;
        } catch (NumberFormatException invalid) {
            // reported below with a fixed message
        }
        throw new InvalidSettingException("-D" + SYNTHETIC_PROBE_MIN_BYTES_PROPERTY + " must be a number of bytes >= 0");
    }

    /** The validated switch; an invalid value is a usage error the CLI reports before any download. */
    public static boolean enabled() {
        String value = System.getProperty(PROPERTY);
        if (value == null) return false;
        return switch (value.strip().toLowerCase(Locale.ROOT)) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new InvalidSettingException();
        };
    }

    /** The commit-point preflight runs on the flag itself, never on per-index eligibility. */
    public static boolean preflightEnabled() {
        return enabled() || COMMIT_PREFLIGHT;
    }

    /** The files the commit facts are read from; each is part of every plan, so never wasted. */
    public static boolean isCommitFile(String name) {
        if (name.startsWith("segments_")) return true;
        String extension = extension(name);
        return extension != null && switch (extension) {
            case "si", "fnm", "cfs", "cfe" -> true;
            default -> false;
        };
    }

    /** Lucene's {@code IndexFileNames.getExtension}: after the last dot, or null. */
    public static String extension(String name) {
        int dot = name.lastIndexOf('.');
        return dot < 0 ? null : name.substring(dot + 1);
    }

    /** Lucene's {@code IndexFileNames.parseSegmentName}: {@code _0} of {@code _0_1_Lucene90_0.dvd}. */
    public static String segmentName(String name) {
        int end = name.indexOf('_', 1);
        if (end < 0) end = name.indexOf('.');
        return end < 0 ? name : name.substring(0, end);
    }

    private static boolean prunable(String name) {
        String extension = extension(name);
        return extension != null && PRUNED_EXTENSIONS.contains(extension);
    }

    /**
     * The bytes a selectively fetched stored-source shard keeps on temp disk, from file names only:
     * every file but those with a pruned extension. Exact for compound segments, whose structures
     * live inside the kept {@code .cfs}; a shard that turns out to need the full fetch exceeds it by
     * its pruned bytes.
     */
    public static long estimateBytes(ShardSnapshot shard) {
        return shard.files().stream().filter(file -> !prunable(file.physicalName())).mapToLong(RepoFileInfo::length).sum();
    }

    /**
     * The plan for one shard. FULL unless the index is {@code eligible} and the commit shows that
     * the pruned reader serves the export: no segment that may hold live documents lacks stored
     * {@code _source} (reconstruction reads other structures), and no segment needs the legacy
     * nested markers, which are read from postings. Otherwise SELECTIVE: a file is skipped only
     * when its extension is pruned and it belongs to a non-compound segment of the commit.
     */
    public static FetchPlan plan(CommitPoint commit, ShardSnapshot shard, boolean eligible) {
        if (!eligible) return FetchPlan.full(shard);
        Set<String> prunableSegments = new HashSet<>();
        for (CommitPoint.Segment segment : commit.segments()) {
            if (!segment.hasSource() && segment.liveDocs() != 0) return FetchPlan.full(shard);
            if (segment.needsLegacyNestedMarkers()) return FetchPlan.full(shard);
            if (!segment.compound()) prunableSegments.add(segment.name());
        }
        Set<String> skipped = new HashSet<>();
        long planned = 0;
        for (RepoFileInfo file : shard.files()) {
            String name = file.physicalName();
            if (prunable(name) && prunableSegments.contains(segmentName(name))) skipped.add(name);
            else planned += file.length();
        }
        return new FetchPlan(FetchPlan.Mode.SELECTIVE, file -> !skipped.contains(file.physicalName()), planned);
    }

    /**
     * The first step of the plan of a shard whose live documents Elasticsearch's loaders reconstruct
     * (synthetic source, or {@code _source} disabled): FULL with its reason when the rules say the
     * probe cannot or need not run, otherwise SYNTHETIC_CANDIDATE, whose files are those every plan
     * keeps ({@link #keeping} with no structure). Rules, in this order: F1 the index's vectors are
     * read from the vector files ({@code vectorsFromVectorFiles}); F2 no non-compound segment with
     * live documents holds at least {@link #syntheticProbeMinBytes()} bytes of prunable structures;
     * F3 a segment with live documents needs the legacy nested markers. A file of no commit segment
     * is always downloaded (F4).
     */
    public static FetchPlan syntheticPlan(CommitPoint commit, ShardSnapshot shard, boolean vectorsFromVectorFiles) {
        if (vectorsFromVectorFiles) return FetchPlan.full(shard, FULL_VECTORS);
        Map<String, Long> lengths = new HashMap<>();
        for (RepoFileInfo file : shard.files()) lengths.put(file.physicalName(), file.length());
        long minBytes = syntheticProbeMinBytes();
        boolean worthProbing = false;
        for (CommitPoint.Segment segment : commit.segments()) {
            if (!segment.compound() && segment.liveDocs() != 0 && prunableBytes(segment, lengths) >= minBytes) worthProbing = true;
        }
        if (!worthProbing) return FetchPlan.full(shard, FULL_BELOW_THRESHOLD);
        for (CommitPoint.Segment segment : commit.segments()) {
            if (segment.liveDocs() != 0 && segment.needsLegacyNestedMarkers()) return FetchPlan.full(shard, FULL_LEGACY_NESTED);
        }
        FetchPlan first = keeping(commit, shard, Set.of());
        return new FetchPlan(FetchPlan.Mode.SYNTHETIC_CANDIDATE, first.include(), first.plannedBytes());
    }

    /** Bytes of the files of {@code segment} that belong to a structure. */
    static long prunableBytes(CommitPoint.Segment segment, Map<String, Long> lengths) {
        long bytes = 0;
        for (String file : segment.files()) {
            if (structureOf(file) != null) bytes += lengths.getOrDefault(file, 0L);
        }
        return bytes;
    }

    /**
     * SELECTIVE: every file except those of non-compound commit segments that belong to a structure
     * outside {@code kept}. Unlike the stored-source plan, the stored-fields bloom sidecar is kept:
     * Elasticsearch's own TSDB codec, which reconstruction reads through, opens it with the stored fields.
     */
    public static FetchPlan keeping(CommitPoint commit, ShardSnapshot shard, Set<Structure> kept) {
        Set<String> prunableSegments = new HashSet<>();
        for (CommitPoint.Segment segment : commit.segments()) {
            if (!segment.compound()) prunableSegments.add(segment.name());
        }
        Set<String> skipped = new HashSet<>();
        long planned = 0;
        for (RepoFileInfo file : shard.files()) {
            String name = file.physicalName();
            Structure structure = structureOf(name);
            if (structure != null && !kept.contains(structure) && prunableSegments.contains(segmentName(name))) skipped.add(name);
            else planned += file.length();
        }
        return new FetchPlan(FetchPlan.Mode.SELECTIVE, file -> !skipped.contains(file.physicalName()), planned);
    }

    /** An invalid selective-fetch setting; the message names only the setting. */
    public static final class InvalidSettingException extends IllegalArgumentException
            implements FailureDiagnostics.SafeMessage {
        InvalidSettingException() {
            this("-D" + PROPERTY + " must be true or false");
        }

        InvalidSettingException(String message) {
            super(message);
        }
    }
}
