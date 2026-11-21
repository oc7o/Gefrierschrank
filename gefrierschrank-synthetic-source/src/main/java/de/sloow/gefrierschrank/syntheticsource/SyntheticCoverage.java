package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Whether reconstruction would refuse, decided before a shard's data files exist: the commit-point
 * preflight ({@link #requireReconstructable}, FieldInfos of the commit only) and the index-level
 * check of indices created with {@code index.mapping.source.mode: disabled}
 * ({@link #requireMappingReconstructable}, mapping only). Both run the worker's own coverage plan in
 * the isolated world and translate failures exactly as the worker's open does, so they throw the
 * class and message a full download's reconstruction would quarantine with. Also holds the switches
 * of reconstructing documents without stored {@code _source} in non-synthetic indices.
 */
public final class SyntheticCoverage {

    /** JVM property set by {@code dump-to-delta --source-disabled-accept}; see {@link #ACCEPT_UNMAPPED_VALUES}. */
    public static final String SOURCE_DISABLED_ACCEPT_PROPERTY = "gefrierschrank.synthetic.sourceDisabledAccept";

    /**
     * The only accepted value (case-insensitive): documents of objects with {@code dynamic: false}
     * or {@code runtime} are reconstructed although values sent there were kept only in the disabled
     * {@code _source} and are lost uncounted. Anything else leaves the refusal on.
     */
    public static final String ACCEPT_UNMAPPED_VALUES = "unmapped-values";

    private static final String CHECK = "de.sloow.gefrierschrank.syntheticsource.isolated.IsolatedCoverageCheck";

    private SyntheticCoverage() {}

    /** The kill switch {@link SyntheticSourceReconstructor#RECONSTRUCT_DISABLED_SOURCE_PROPERTY}: on unless {@code false}. */
    public static boolean reconstructsDisabledSource() {
        return !"false".equalsIgnoreCase(System.getProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, "true").strip());
    }

    /** Whether {@link #SOURCE_DISABLED_ACCEPT_PROPERTY} is {@link #ACCEPT_UNMAPPED_VALUES}. */
    public static boolean acceptsUnmappedValues() {
        String value = System.getProperty(SOURCE_DISABLED_ACCEPT_PROPERTY);
        return value != null && ACCEPT_UNMAPPED_VALUES.equalsIgnoreCase(value.strip());
    }

    /**
     * Throws exactly what reconstruction of live documents in these segments would throw (same
     * class, same message): {@link UnreconstructableSourceException} with the tokens of the
     * worker's open or leaf check, {@code SourceNotSyntheticException},
     * {@link SyntheticSourceReconstructor.UnsupportedSyntheticFieldException}, or the IOException of
     * the worker's MapperService build; returns normally otherwise. Reads only segments_N, .si and
     * .fnm (or .cfs/.cfe) of {@code shardDir}. A no-op for an empty set. Thread-safe.
     *
     * <p>One refusal depends on per-document data the FieldInfos do not hold, so this check lets it
     * pass and the read throws it: a text or match_only_text field with a live value that its
     * keyword multi-field does not hold (the multi-field was added after the document was written)
     * refuses with {@code <type>:no-native-source} at read time only. Returning normally is
     * therefore "nothing the commit shows refuses", never "the read cannot refuse".
     */
    public static void requireReconstructable(Path shardDir, String indexName, JsonNode rawIndexMetadata,
                                              Set<String> sourcelessLiveSegments) throws IOException {
        if (sourcelessLiveSegments.isEmpty()) return;
        Object plan = plan(indexName, rawIndexMetadata);
        invoke(indexName, "requireMapping", new Class<?>[]{Object.class, boolean.class}, plan, acceptsUnmappedValues());
        invoke(indexName, "requireSegments", new Class<?>[]{Object.class, Path.class, Set.class},
            plan, shardDir, Set.copyOf(sourcelessLiveSegments));
    }

    /**
     * Throws exactly what the worker's open throws for this index's mapping (mapping-level tokens,
     * {@code SourceNotSyntheticException}, the unsupported-field or build failure), returns normally
     * otherwise. Reads no shard file. Thread-safe.
     */
    public static void requireMappingReconstructable(String indexName, JsonNode rawIndexMetadata) throws IOException {
        Object plan = plan(indexName, rawIndexMetadata);
        invoke(indexName, "requireMapping", new Class<?>[]{Object.class, boolean.class}, plan, acceptsUnmappedValues());
    }

    /**
     * The per-index warning of {@code --source-disabled-accept unmapped-values}, or null unless the
     * opt-in and the kill switch are on and the mapping has {@code dynamic: false}/{@code runtime}
     * objects whose values a reconstructed document loses. Never throws.
     */
    public static String unmappedValuesNote(String indexName, JsonNode rawIndexMetadata) {
        if (!acceptsUnmappedValues() || !reconstructsDisabledSource()) return null;
        try {
            Object plan = plan(indexName, rawIndexMetadata);
            if (!(Boolean) invoke(indexName, "losesUnmappedValues", new Class<?>[]{Object.class}, plan)) return null;
        } catch (IOException | RuntimeException e) {
            return null;
        }
        return "warning: index " + indexName + ": --source-disabled-accept unmapped-values is set; values sent under"
            + " dynamic:false or dynamic:runtime objects were kept only in the disabled _source and are missing, uncounted,"
            + " from documents reconstructed without stored _source (their shard reports carry unrecoverableUnmappedValues: true)";
    }

    // Plans by metadata identity, held weakly: a run resolves each index's metadata once, so every
    // shard of an index reuses one MapperService plan; a dropped metadata object drops its plan.
    private static final Map<Integer, List<Memo>> PLANS = new HashMap<>();

    private record Memo(WeakReference<JsonNode> metadata, boolean rewrite, ClassLoader loader, Object plan) {}

    private static Object plan(String indexName, JsonNode rawIndexMetadata) throws IOException {
        SyntheticSourceReconstructor.Inputs inputs = SyntheticSourceReconstructor.reconstructionInputs(rawIndexMetadata);
        boolean rewrite = inputs.rewriteMappingJson() != null;
        ClassLoader loader = IsolatedEsClassLoader.shared();
        int key = System.identityHashCode(rawIndexMetadata);
        synchronized (PLANS) {
            List<Memo> memos = PLANS.get(key);
            if (memos != null) {
                memos.removeIf(memo -> memo.metadata().get() == null);
                for (Memo memo : memos) {
                    if (memo.metadata().get() == rawIndexMetadata && memo.rewrite() == rewrite && memo.loader() == loader) {
                        return memo.plan();
                    }
                }
            }
        }
        Object plan = invoke(indexName, "plan", new Class<?>[]{String.class, String.class, Map.class, String.class},
            indexName, inputs.mappingJson(), inputs.flatSettings(), inputs.rewriteMappingJson());
        synchronized (PLANS) {
            PLANS.values().forEach(memos -> memos.removeIf(memo -> memo.metadata().get() == null));
            PLANS.values().removeIf(List::isEmpty);
            PLANS.computeIfAbsent(key, k -> new ArrayList<>()).add(new Memo(new WeakReference<>(rawIndexMetadata), rewrite, loader, plan));
        }
        return plan;
    }

    private static Object invoke(String indexName, String method, Class<?>[] types, Object... arguments) throws IOException {
        IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();
        Thread currentThread = Thread.currentThread();
        ClassLoader previousContextLoader = currentThread.getContextClassLoader();
        try {
            currentThread.setContextClassLoader(loader);
            return Class.forName(CHECK, true, loader).getMethod(method, types).invoke(null, arguments);
        } catch (ReflectiveOperationException e) {
            throw SyntheticSourceReconstructor.buildFailure(e, loader, indexName);
        } catch (LinkageError e) {
            IsolatedEsClassLoader.discard(loader);
            throw e;
        } finally {
            currentThread.setContextClassLoader(previousContextLoader);
        }
    }
}
