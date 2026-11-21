package de.sloow.gefrierschrank.syntheticsource.isolated;

import de.sloow.gefrierschrank.syntheticsource.UnreconstructableSourceException;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.AbstractScriptFieldType;
import org.elasticsearch.index.mapper.ConstantFieldType;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.IgnoredFieldMapper;
import org.elasticsearch.index.mapper.IndexType;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.NestedPathFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.extras.MatchOnlyTextFieldMapper;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.constantkeyword.mapper.ConstantKeywordFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * Whether documents without stored {@code _source} of an index whose mapping is not synthetic can
 * be reconstructed through Elasticsearch's synthetic loaders, decided from Elasticsearch's own
 * mappers: the {@code original} MapperService (the mapping as the index wrote its documents) and
 * the {@code rewrite} (the same mapping without {@code _source}, synthetic mode), whose loaders
 * would read the data. A field whose loader would read a structure the stored-mode index never
 * wrote is a refusal, named by an Elasticsearch type token, never silently a missing value.
 *
 * <p>Mapping-level refusals hold for every document and are known at open; per-segment refusals
 * apply only to fields that hold data in a segment's FieldInfos, so they are evaluated per leaf,
 * on a real leaf at read time and on a {@link FieldInfosLeafReader} in the commit-point preflight.
 */
public final class ReconstructionCoverage {

    /** ES 8.0.0: older indices keep {@code SourceNotSyntheticException} (their segments do not open here anyway). */
    static final int MIN_REWRITE_CREATED = 8_00_00_99;

    /**
     * Types whose rewrite loader reads a stored "diversion" field that stored mode never wrote, and
     * for which ReconstructionCoverageTest proves every diverted value is listed in {@code _ignored}
     * (so the document is counted), or that the field then refuses per segment. Every other type
     * with a diversion refuses ({@code <type>:diverted}).
     */
    static final Set<String> IGNORED_RECORDED_DIVERSIONS = Set.of(
        "keyword", "wildcard", "text", "match_only_text",
        "byte", "short", "integer", "long", "float", "half_float", "double", "scaled_float", "unsigned_long",
        "date", "date_nanos", "ip", "boolean", "geo_point", "histogram", "aggregate_metric_double");

    private static final String TOKEN_UNMAPPED = "unmapped";

    private final MapperService rewrite;
    private final List<TreeField> fields;
    private final Set<String> mappingTokens;
    private final Set<String> unmappedValueTokens;
    private final ConstantNode constants;
    private final Set<String> acceptedDiversions;

    private ReconstructionCoverage(MapperService rewrite, List<TreeField> fields, Set<String> mappingTokens,
                                   Set<String> unmappedValueTokens, ConstantNode constants, Set<String> acceptedDiversions) {
        this.rewrite = rewrite;
        this.fields = fields;
        this.mappingTokens = mappingTokens;
        this.unmappedValueTokens = unmappedValueTokens;
        this.constants = constants;
        this.acceptedDiversions = acceptedDiversions;
    }

    /**
     * One FieldMapper of the reconstruction tree and what the original mapping stores for it;
     * {@code delegate} is the keyword multi-field a text field's loader reads its values from.
     */
    private record TreeField(FieldMapper mapper, boolean storedInOriginal, boolean scripted, KeywordFieldMapper delegate) {}

    /**
     * The worker's (and the preflight's) way from the original MapperService to a reconstructing
     * one: a synthetic mapping needs none (returns null); a non-synthetic mapping of an ES 8+ index
     * with a rewrite mapping gets the rewrite and its plan, whose mapping-level refusals (other than
     * {@code dynamic:*} when {@code acceptUnmappedValues}) throw here; anything else is
     * {@code SourceNotSyntheticException}. On success {@code original} is closed and the plan owns
     * the rewrite ({@link #rewrite()}); on failure {@code original} stays the caller's to close.
     */
    static ReconstructionCoverage open(String indexName, MapperService original, String rewriteMappingJson,
                                       Map<String, String> flatSettings, Function<Query, BitSetProducer> bitsetProducer,
                                       boolean acceptUnmappedValues) throws IOException {
        ReconstructionCoverage coverage = openUnchecked(indexName, original, rewriteMappingJson, flatSettings, bitsetProducer);
        if (coverage != null) {
            try {
                coverage.requireMappingCovered(acceptUnmappedValues);
            } catch (IOException | RuntimeException | Error e) {
                try { coverage.rewrite.close(); } catch (IOException close) { e.addSuppressed(close); }
                throw e;
            }
            original.close();
        }
        return coverage;
    }

    /** {@link #open} without the mapping-level check and leaving {@code original} open; for the preflight's memoized plan. */
    static ReconstructionCoverage openUnchecked(String indexName, MapperService original, String rewriteMappingJson,
                                                Map<String, String> flatSettings, Function<Query, BitSetProducer> bitsetProducer)
            throws IOException {
        if (original.mappingLookup().isSourceSynthetic()) {
            return null;
        }
        if (rewriteMappingJson == null || original.getIndexSettings().getIndexVersionCreated().id() < MIN_REWRITE_CREATED) {
            throw new IsolatedReconstructionWorkerImpl.SourceNotSyntheticException();
        }
        MapperService rewrite;
        try {
            rewrite = IsolatedReconstructionWorkerImpl.buildMapperService(indexName, rewriteMappingJson,
                rewriteSettings(flatSettings), bitsetProducer);
        } catch (RuntimeException | IOException rejected) {
            throw new UnreconstructableSourceException(List.of("mapping-rejected:" + rejected.getClass().getSimpleName()));
        }
        try {
            if (!rewrite.mappingLookup().isSourceSynthetic()) {
                throw new UnreconstructableSourceException(List.of("mapping-rejected:" + "SourceFieldMapper"));
            }
            return plan(original, rewrite);
        } catch (IOException | RuntimeException | Error e) {
            try { rewrite.close(); } catch (IOException close) { e.addSuppressed(close); }
            throw e;
        }
    }

    /** The flat settings with the source mode forced to synthetic (a recorded {@code stored} or {@code disabled} is replaced). */
    static Map<String, String> rewriteSettings(Map<String, String> flatSettings) {
        Map<String, String> settings = new LinkedHashMap<>(flatSettings);
        settings.put("index.mapping.source.mode", "synthetic");
        return settings;
    }

    MapperService rewrite() {
        return rewrite;
    }

    static ReconstructionCoverage plan(MapperService original, MapperService rewrite) {
        return plan(original, rewrite, IGNORED_RECORDED_DIVERSIONS);
    }

    /** {@link #plan(MapperService, MapperService)} with another accepted diversion set; tests prove {@code <type>:diverted} with it. */
    static ReconstructionCoverage plan(MapperService original, MapperService rewrite, Set<String> ignoredRecordedDiversions) {
        Set<String> tokens = new TreeSet<>();
        Set<String> unmappedValueTokens = new TreeSet<>();
        List<TreeField> fields = new ArrayList<>();
        Map<String, ObjectMapper.Dynamic> dynamics = new LinkedHashMap<>();
        ConstantNode constants = new ConstantNode(false);
        Set<String> acceptedDiversions = new TreeSet<>();
        MappingLookup lookup = rewrite.mappingLookup();
        walk(lookup.getMapping().getRoot(), ObjectMapper.Dynamic.TRUE, original, rewrite, tokens, unmappedValueTokens,
            fields, dynamics, constants, acceptedDiversions, ignoredRecordedDiversions);
        // A runtime field without a concrete field at its path shadowed every value sent for it:
        // no FieldInfo and no _ignored entry remain. Under a dynamic:runtime object those are the
        // dynamic:runtime values themselves.
        for (Map.Entry<String, MappedFieldType> entry : lookup.getFullNameToFieldType().entrySet()) {
            if (!(entry.getValue() instanceof AbstractScriptFieldType<?>)) continue;
            if (lookup.getMapper(entry.getKey()) instanceof FieldMapper) continue;
            if (effectiveDynamic(entry.getKey(), dynamics) == ObjectMapper.Dynamic.RUNTIME) continue;
            tokens.add("runtime:shadowing");
        }
        return new ReconstructionCoverage(rewrite, List.copyOf(fields), tokens, unmappedValueTokens, constants, acceptedDiversions);
    }

    private static ObjectMapper.Dynamic effectiveDynamic(String path, Map<String, ObjectMapper.Dynamic> dynamics) {
        for (int dot = path.lastIndexOf('.'); dot > 0; dot = path.lastIndexOf('.', dot - 1)) {
            ObjectMapper.Dynamic dynamic = dynamics.get(path.substring(0, dot));
            if (dynamic != null) return dynamic;
        }
        return dynamics.get("");
    }

    private static void walk(ObjectMapper object, ObjectMapper.Dynamic inherited, MapperService original, MapperService rewrite,
                             Set<String> tokens, Set<String> unmappedValueTokens, List<TreeField> fields,
                             Map<String, ObjectMapper.Dynamic> dynamics, ConstantNode constants, Set<String> acceptedDiversions,
                             Set<String> ignoredRecordedDiversions) {
        ObjectMapper.Dynamic dynamic = object.dynamic() == null ? inherited : object.dynamic();
        dynamics.put(object instanceof org.elasticsearch.index.mapper.RootObjectMapper ? "" : object.fullPath(), dynamic);
        if (!object.isEnabled()) tokens.add("object:enabled-false");
        if (dynamic == ObjectMapper.Dynamic.FALSE) unmappedValueTokens.add("dynamic:false");
        if (dynamic == ObjectMapper.Dynamic.RUNTIME) unmappedValueTokens.add("dynamic:runtime");
        if (object instanceof NestedObjectMapper
            && object.sourceKeepMode().orElse(Mapper.SourceKeepMode.NONE) == Mapper.SourceKeepMode.ALL) {
            tokens.add("nested:keep-all");
        }
        for (Mapper child : object) {
            if (child instanceof ObjectMapper childObject) {
                walk(childObject, dynamic, original, rewrite, tokens, unmappedValueTokens, fields, dynamics,
                    constants.child(childObject.leafName(), childObject instanceof NestedObjectMapper), acceptedDiversions,
                    ignoredRecordedDiversions);
            } else if (child instanceof FieldMapper field && !rewrite.isMetadataField(field.fullPath())) {
                Mapper originalMapper = original.mappingLookup().getMapper(field.fullPath());
                MappedFieldType originalType = originalMapper instanceof FieldMapper o ? o.fieldType() : null;
                if (originalType != null && writesNothing(originalType)) {
                    tokens.add(typeName(field) + ":not-indexed");
                }
                // Flattened values over ignore_above are dropped without an _ignored entry, so the
                // diversion that would hold them is no further concern.
                boolean flattenedIgnoreAbove = field instanceof FlattenedFieldMapper flattened
                    && flattened.fieldType().ignoreAbove().get() < Integer.MAX_VALUE;
                if (flattenedIgnoreAbove) {
                    tokens.add("flattened:ignore_above");
                }
                SourceLoader.SyntheticFieldLoader loader = field.syntheticFieldLoader();
                boolean storedInOriginal = false;
                boolean diverted = false;
                if (loader != SourceLoader.SyntheticFieldLoader.NOTHING) {
                    for (String stored : loader.storedFieldLoaders().map(Map.Entry::getKey).toList()) {
                        if (storedInOriginal(original, stored)) storedInOriginal = true;
                        else diverted = true;
                    }
                }
                if (diverted && !flattenedIgnoreAbove) {
                    if (ignoredRecordedDiversions.contains(typeName(field))) acceptedDiversions.add(typeName(field));
                    else tokens.add(typeName(field) + ":diverted");
                }
                if (field.fieldType() instanceof ConstantKeywordFieldMapper.ConstantKeywordFieldType constant
                    && constant.value() != null) {
                    constants.constant(field.leafName(), constant.value());
                }
                // A constant has no per-document data to check; its value is added (withConstants).
                if (!(field.fieldType() instanceof ConstantFieldType)) {
                    fields.add(new TreeField(field, storedInOriginal, field.hasScript(), keywordDelegate(field)));
                }
            }
        }
    }

    /**
     * The keyword multi-field whose values a text-like field's synthetic loader renders as its own,
     * as Elasticsearch picks it; null for a stored text field (its loader reads its own stored field)
     * and for every other type.
     */
    private static KeywordFieldMapper keywordDelegate(FieldMapper field) {
        boolean delegates = field instanceof TextFieldMapper text ? !text.fieldType().isStored()
            : field instanceof MatchOnlyTextFieldMapper;
        return delegates ? TextFieldMapper.SyntheticSourceHelper.getKeywordFieldMapperForSyntheticSource(field) : null;
    }

    /**
     * The mapper's type name as the mapping declares it ({@code contentType()}): {@code typeName()}
     * is the field type's, which some mappers share (token_count reports {@code integer}).
     */
    static String typeName(FieldMapper mapper) {
        for (Class<?> type = mapper.getClass(); type != null && type != FieldMapper.class; type = type.getSuperclass()) {
            try {
                java.lang.reflect.Method contentType = type.getDeclaredMethod("contentType");
                contentType.setAccessible(true);
                return (String) contentType.invoke(mapper);
            } catch (NoSuchMethodException next) {
                // declared further up
            } catch (ReflectiveOperationException | RuntimeException e) {
                break;
            }
        }
        return mapper.typeName();
    }

    private static boolean storedInOriginal(MapperService original, String storedFieldName) {
        MappedFieldType type = original.mappingLookup().getFieldType(storedFieldName);
        return type != null && !(type instanceof AbstractScriptFieldType<?>) && type.isStored();
    }

    /** Neither indexed, nor doc values, nor stored, nor constant or computed: a value left no trace at all. */
    private static boolean writesNothing(MappedFieldType type) {
        IndexType index = type.indexType();
        return !index.hasTerms() && !index.hasPoints() && !index.hasVectors() && !index.hasDocValues()
            && !type.hasDocValues() && !type.isStored() && !(type instanceof ConstantFieldType) && !type.hasScriptValues();
    }

    /** Types of IGNORED_RECORDED_DIVERSIONS this mapping relies on; for ReconstructionCoverageTest. */
    Set<String> acceptedDiversions() {
        return acceptedDiversions;
    }

    /** Mapping-level tokens, sorted; {@code dynamic:*} only without the opt-in. */
    Set<String> mappingRefusals(boolean acceptUnmappedValues) {
        Set<String> refusals = new TreeSet<>(mappingTokens);
        if (!acceptUnmappedValues) refusals.addAll(unmappedValueTokens);
        return refusals;
    }

    /** Whether the mapping has values that only the disabled {@code _source} held, uncounted ({@code dynamic:false}/{@code runtime}). */
    boolean losesUnmappedValues() {
        return !unmappedValueTokens.isEmpty();
    }

    void requireMappingCovered(boolean acceptUnmappedValues) throws UnreconstructableSourceException {
        Set<String> refusals = mappingRefusals(acceptUnmappedValues);
        if (!refusals.isEmpty()) throw new UnreconstructableSourceException(refusals);
    }

    /**
     * Per-segment refusals of {@code leaf}: for every tree field holding data in it, ES's
     * rewrite loader must find doc values on the leaf or a stored field the original mapping
     * stores; every FieldInfo must belong to a mapper.
     *
     * <p>On a real leaf two answers depend on the data, which the preflight's
     * {@link FieldInfosLeafReader} cannot see. A loader that is null although the FieldInfos declare
     * what it reads means an empty dictionary (every document holding the field was deleted and
     * merged away), so nothing is lost and the check passes, as on the FieldInfos leaf. And a text
     * field whose values come from a keyword multi-field refuses with {@code <type>:no-native-source}
     * when a live document holds a text value its multi-field does not hold and {@code _ignored} does
     * not list: Elasticsearch accepts adding the multi-field later, and documents written before
     * that have none. The preflight cannot count, so it lets such a segment pass and the read
     * refuses it (the one way the preflight is more permissive than reconstruction).
     */
    Set<String> refusals(LeafReader leaf) throws IOException {
        FieldInfos infos = leaf.getFieldInfos();
        boolean presenceOnly = leaf instanceof FieldInfosLeafReader;
        LeafReader declared = null;
        Set<String> tokens = new TreeSet<>();
        Set<String> attributed = new HashSet<>();
        for (TreeField field : fields) {
            FieldMapper mapper = field.mapper();
            boolean holdsData = attribute(mapper, infos, attributed) | mapper.fieldType().fieldHasValue(infos);
            if (!holdsData || field.scripted()) continue;
            SourceLoader.SyntheticFieldLoader loader = mapper.syntheticFieldLoader();
            if (loader == SourceLoader.SyntheticFieldLoader.NOTHING) {
                tokens.add(typeName(mapper) + (mapper.copyTo().copyToFields().isEmpty() ? ":fallback" : ":copy_to"));
                continue;
            }
            if (!field.storedInOriginal() && loader.docValuesLoader(leaf, null) == null) {
                if (!presenceOnly && declared == null) declared = new FieldInfosLeafReader(infos, leaf.maxDoc(), leaf.getMetaData());
                if (presenceOnly || mapper.syntheticFieldLoader().docValuesLoader(declared, null) == null) {
                    tokens.add(typeName(mapper) + ":no-native-source");
                    continue;
                }
            }
            if (!presenceOnly && field.delegate() != null && delegateMissesValues(leaf, mapper.fullPath(), field.delegate())) {
                tokens.add(typeName(mapper) + ":no-native-source");
            }
        }
        if (!unattributed(infos, attributed).isEmpty()) tokens.add(TOKEN_UNMAPPED);
        return tokens;
    }

    /**
     * Whether a live document of {@code leaf} holds a value of the text field {@code path} that its
     * keyword {@code delegate} holds no value for and whose root's {@code _ignored} lists neither the
     * field nor one below it. A document holds a text value where the field has a norm (every
     * document that sent it, tokens or not) or, without norms, a posting or a {@code _field_names}
     * entry (Elasticsearch indexes the path there for every value of a text field without norms, so
     * a value of no tokens is seen too; postings without that entry mean a leaf that cannot show
     * it, which counts as missing values). A delegate that is neither doc values nor indexed (stored
     * only) cannot be counted per document and is trusted.
     */
    private static boolean delegateMissesValues(LeafReader leaf, String path, KeywordFieldMapper delegate) throws IOException {
        FieldInfo info = leaf.getFieldInfos().fieldInfo(path);
        if (info == null) return false;
        FixedBitSet held = documentsWithValues(leaf, delegate);
        if (held == null) return false;
        Bits dead = deadDocuments(leaf);
        FixedBitSet ignored = null;
        FixedBitSet roots = null;
        boolean ignoredRead = false;
        DocIdSetIterator values;
        if (info.hasNorms()) {
            values = leaf.getNormValues(path);
            // Declared but absent (a leaf opened without norms): coverage cannot be shown.
            if (values == null) return true;
        } else if (info.getIndexOptions() != IndexOptions.NONE) {
            FixedBitSet sent = new FixedBitSet(leaf.maxDoc());
            Terms terms = leaf.terms(path);
            if (terms != null) sent.or(union(terms.iterator(), leaf.maxDoc()));
            // A value that analyzes to no token ("", "-") writes no posting; _field_names lists it anyway.
            Terms fieldNames = leaf.terms(FieldNamesFieldMapper.NAME);
            TermsEnum names = fieldNames == null ? null : fieldNames.iterator();
            if (names != null && names.seekExact(new BytesRef(path))) {
                sent.or(names.postings(null, PostingsEnum.NONE));
            } else if (terms != null) {
                // Postings without the _field_names entry every such value writes (a pruned leaf): coverage cannot be shown.
                return true;
            }
            values = new BitSetIterator(sent, sent.cardinality());
        } else {
            return false;
        }
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            if (held.get(doc) || (dead != null && dead.get(doc))) continue;
            if (!ignoredRead) {
                ignored = ignoring(leaf, path);
                roots = ignored == null ? null : rootDocuments(leaf);
                ignoredRead = true;
            }
            if (ignored == null) return true;
            int root = roots == null ? doc : roots.nextSetBit(doc);
            if (root == DocIdSetIterator.NO_MORE_DOCS || !ignored.get(root)) return true;
        }
        return false;
    }

    /** Documents with a value of a keyword field: doc values, else postings; null when it has neither. */
    private static FixedBitSet documentsWithValues(LeafReader leaf, KeywordFieldMapper keyword) throws IOException {
        String name = keyword.fullPath();
        FixedBitSet documents = new FixedBitSet(leaf.maxDoc());
        FieldInfo info = leaf.getFieldInfos().fieldInfo(name);
        if (keyword.fieldType().hasDocValues()) {
            if (info == null) return documents;
            DocIdSetIterator values = info.getDocValuesType() == DocValuesType.SORTED
                ? leaf.getSortedDocValues(name) : leaf.getSortedSetDocValues(name);
            if (values != null) documents.or(values);
            return documents;
        }
        if (keyword.fieldType().indexType().hasTerms()) {
            if (info == null) return documents;
            Terms terms = leaf.terms(name);
            if (terms != null) documents.or(union(terms.iterator(), leaf.maxDoc()));
            return documents;
        }
        return null;
    }

    /** Hard-deleted and soft-deleted documents of {@code leaf}, or null when it has neither. */
    private static Bits deadDocuments(LeafReader leaf) throws IOException {
        Bits live = leaf.getLiveDocs();
        NumericDocValues softDeletes = leaf.getNumericDocValues(Lucene.SOFT_DELETES_FIELD);
        if (live == null && softDeletes == null) return null;
        FixedBitSet dead = new FixedBitSet(leaf.maxDoc());
        if (live != null) {
            for (int doc = 0; doc < leaf.maxDoc(); doc++) if (!live.get(doc)) dead.set(doc);
        }
        if (softDeletes != null) dead.or(softDeletes);
        return dead;
    }

    /** Root documents ({@code _primary_term}) when the leaf holds nested documents, whose {@code _ignored} sits on their root. */
    private static FixedBitSet rootDocuments(LeafReader leaf) throws IOException {
        if (leaf.getFieldInfos().fieldInfo(NestedPathFieldMapper.NAME) == null) return null;
        NumericDocValues primaryTerms = leaf.getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        FixedBitSet roots = new FixedBitSet(leaf.maxDoc());
        if (primaryTerms != null) roots.or(primaryTerms);
        return roots;
    }

    /** Documents whose {@code _ignored} lists {@code path} or a field below it, or null when none does. */
    private static FixedBitSet ignoring(LeafReader leaf, String path) throws IOException {
        FieldInfo info = leaf.getFieldInfos().fieldInfo(IgnoredFieldMapper.NAME);
        if (info == null) return null;
        FixedBitSet documents = new FixedBitSet(leaf.maxDoc());
        if (info.getDocValuesType() != DocValuesType.NONE) {
            SortedSetDocValues values = leaf.getSortedSetDocValues(IgnoredFieldMapper.NAME);
            if (values == null) return null;
            Set<Long> ords = new HashSet<>();
            TermsEnum terms = values.termsEnum();
            if (terms.seekCeil(new BytesRef(path)) != TermsEnum.SeekStatus.END) {
                do {
                    String name = terms.term().utf8ToString();
                    if (!name.startsWith(path)) break;
                    if (listsField(name, path)) ords.add(terms.ord());
                } while (terms.next() != null);
            }
            if (ords.isEmpty()) return null;
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                for (int i = 0; i < values.docValueCount(); i++) {
                    if (ords.contains(values.nextOrd())) {
                        documents.set(doc);
                        break;
                    }
                }
            }
        } else {
            Terms terms = leaf.terms(IgnoredFieldMapper.NAME);
            if (terms == null) return null;
            TermsEnum termsEnum = terms.iterator();
            PostingsEnum postings = null;
            if (termsEnum.seekCeil(new BytesRef(path)) != TermsEnum.SeekStatus.END) {
                do {
                    String name = termsEnum.term().utf8ToString();
                    if (!name.startsWith(path)) break;
                    if (!listsField(name, path)) continue;
                    postings = termsEnum.postings(postings, PostingsEnum.NONE);
                    documents.or(postings);
                } while (termsEnum.next() != null);
            }
        }
        return documents.cardinality() == 0 ? null : documents;
    }

    private static boolean listsField(String ignored, String path) {
        return ignored.equals(path) || ignored.startsWith(path + ".");
    }

    /** The documents holding any term of {@code terms}. */
    private static DocIdSetIterator union(TermsEnum terms, int maxDoc) throws IOException {
        FixedBitSet documents = new FixedBitSet(maxDoc);
        PostingsEnum postings = null;
        while (terms.next() != null) {
            postings = terms.postings(postings, PostingsEnum.NONE);
            documents.or(postings);
        }
        return new BitSetIterator(documents, documents.cardinality());
    }

    /** Adds the FieldInfo names of {@code mapper} (its own, its multi-fields', internal suffixes) to {@code into}. */
    private boolean attribute(FieldMapper mapper, FieldInfos infos, Set<String> into) {
        MappingLookup lookup = rewrite.mappingLookup();
        String path = mapper.fullPath();
        boolean found = false;
        for (FieldInfo info : infos) {
            if (info.name.equals(path) || (info.name.startsWith(path + ".")
                && (lookup.isMultiField(info.name) || lookup.getMapper(info.name) == null))) {
                found = true;
                into.add(info.name);
            }
        }
        return found;
    }

    /** FieldInfo names of {@code leaf} no mapper owns (the evidence for {@code unmapped}); for tests. */
    List<String> unattributed(LeafReader leaf) {
        Set<String> attributed = new HashSet<>();
        for (TreeField field : fields) attribute(field.mapper(), leaf.getFieldInfos(), attributed);
        return unattributed(leaf.getFieldInfos(), attributed);
    }

    /**
     * FieldInfo names no mapper owns, past the fields Elasticsearch writes beside its
     * mappers: metadata fields, {@code _primary_term} (every segment), {@code _tombstone} (deletes),
     * the recovery source, soft deletes, {@code _ignored_source}, and Lucene's block parent field
     * ({@code __root_doc_for_nested} on every segment of an index-sorted index).
     */
    private List<String> unattributed(FieldInfos infos, Set<String> attributed) {
        MappingLookup lookup = rewrite.mappingLookup();
        List<String> names = new ArrayList<>();
        for (FieldInfo info : infos) {
            String name = info.name;
            if (attributed.contains(name) || rewrite.isMetadataField(name) || lookup.getMapper(name) != null
                || name.equals(SeqNoFieldMapper.PRIMARY_TERM_NAME) || name.equals(SeqNoFieldMapper.TOMBSTONE_NAME)
                || name.startsWith("_recovery_source") || name.equals(Lucene.SOFT_DELETES_FIELD)
                || name.equals("_ignored_source") || name.equals(infos.getParentField())) {
                continue;
            }
            names.add(name);
        }
        return names;
    }

    void requireCovered(LeafReader leaf) throws IOException {
        Set<String> refusals = refusals(leaf);
        if (!refusals.isEmpty()) throw new UnreconstructableSourceException(refusals);
    }

    /**
     * Documents of {@code leaf} that list a field in {@code _ignored} (values Elasticsearch ignored at
     * index time, kept only in the disabled {@code _source}), or null when none does. Doc values from
     * 8.15 on, postings before.
     */
    static FixedBitSet ignoredDocuments(LeafReader leaf) throws IOException {
        FieldInfo info = leaf.getFieldInfos().fieldInfo(IgnoredFieldMapper.NAME);
        if (info == null) return null;
        FixedBitSet documents = new FixedBitSet(leaf.maxDoc());
        if (info.getDocValuesType() != DocValuesType.NONE) {
            SortedSetDocValues values = leaf.getSortedSetDocValues(IgnoredFieldMapper.NAME);
            if (values != null) {
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    documents.set(doc);
                }
            }
        } else {
            Terms terms = leaf.terms(IgnoredFieldMapper.NAME);
            if (terms != null) {
                TermsEnum termsEnum = terms.iterator();
                PostingsEnum postings = null;
                while (termsEnum.next() != null) {
                    postings = termsEnum.postings(postings, PostingsEnum.NONE);
                    for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                        documents.set(doc);
                    }
                }
            }
        }
        return documents.cardinality() == 0 ? null : documents;
    }

    boolean hasConstants() {
        return !constants.isEmpty();
    }

    /**
     * Adds every {@code constant_keyword} value of the mapping to a reconstructed document that lacks
     * it: its synthetic loader renders the value only for documents carrying a marker
     * that ES writes in synthetic mode only. The field goes where native synthetic rendering puts a
     * sent value (fields ordered by leaf name), creating plain objects on the way; below a nested
     * mapper only existing nested objects receive it.
     */
    byte[] withConstants(byte[] esJson) throws IOException {
        Map<String, Object> document = new LinkedHashMap<>(
            XContentHelper.convertToMap(new BytesArray(esJson), true, XContentType.JSON).v2());
        Map<String, Object> completed = constants.complete(document);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(completed);
            return BytesReference.toBytes(BytesReference.bytes(builder));
        }
    }

    /** The constant values below one object of the mapping. */
    private static final class ConstantNode {
        private final boolean nested;
        private final Map<String, String> values = new LinkedHashMap<>();
        private final Map<String, ConstantNode> children = new LinkedHashMap<>();

        ConstantNode(boolean nested) {
            this.nested = nested;
        }

        ConstantNode child(String leafName, boolean nestedChild) {
            return children.computeIfAbsent(leafName, name -> new ConstantNode(nestedChild));
        }

        void constant(String leafName, String value) {
            values.put(leafName, value);
        }

        boolean isEmpty() {
            if (!values.isEmpty()) return false;
            for (ConstantNode child : children.values()) if (!child.isEmpty()) return false;
            return true;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> complete(Map<String, Object> object) {
            Map<String, Object> additions = new LinkedHashMap<>();
            Map<String, Object> result = new LinkedHashMap<>(object);
            for (Map.Entry<String, String> constant : values.entrySet()) {
                if (!result.containsKey(constant.getKey())) additions.put(constant.getKey(), constant.getValue());
            }
            for (Map.Entry<String, ConstantNode> entry : children.entrySet()) {
                ConstantNode child = entry.getValue();
                if (child.isEmpty()) continue;
                Object value = result.get(entry.getKey());
                if (value instanceof Map<?, ?> map) {
                    result.put(entry.getKey(), child.complete((Map<String, Object>) map));
                } else if (value instanceof Collection<?> list) {
                    List<Object> completed = new ArrayList<>();
                    for (Object element : list) {
                        completed.add(element instanceof Map<?, ?> map ? child.complete((Map<String, Object>) map) : element);
                    }
                    result.put(entry.getKey(), completed);
                } else if (value == null && !result.containsKey(entry.getKey()) && !child.nested) {
                    additions.put(entry.getKey(), child.complete(new LinkedHashMap<>()));
                }
            }
            if (additions.isEmpty()) return result;
            // Each addition goes before the first rendered key ordered after it; rendered keys keep their order.
            List<Map.Entry<String, Object>> pending = new ArrayList<>(additions.entrySet());
            pending.sort(Map.Entry.comparingByKey());
            Map<String, Object> merged = new LinkedHashMap<>();
            int next = 0;
            for (Map.Entry<String, Object> entry : result.entrySet()) {
                while (next < pending.size() && pending.get(next).getKey().compareTo(entry.getKey()) < 0) {
                    merged.put(pending.get(next).getKey(), pending.get(next++).getValue());
                }
                merged.put(entry.getKey(), entry.getValue());
            }
            for (; next < pending.size(); next++) merged.put(pending.get(next).getKey(), pending.get(next).getValue());
            return merged;
        }
    }
}
