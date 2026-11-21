package de.sloow.gefrierschrank.syntheticsource.isolated;

import de.sloow.gefrierschrank.syntheticsource.UnreconstructableSourceException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Test helper of ReconstructionCoverageTest, defined by the isolated loader (hence this package):
 * one case of the end-to-end coverage matrix. The documents are indexed through Elasticsearch's own
 * DocumentParser twice, into a stored-mode index whose mapping disables {@code _source} (what an
 * index whose source was disabled later writes) and into a native synthetic twin; the stored-mode
 * index is then reconstructed through the reconstruction worker's rewrite, the twin through its
 * native path. JDK types in and out: the result is one JSON object per case.
 */
public final class CoverageMatrixProbe {

    private CoverageMatrixProbe() {}

    /** A {@code documents} entry with this prefix is a mapping update applied between documents, not a document. */
    public static final String UPDATE = "@update ";

    /**
     * @param inject {@code tombstone} adds a delete tombstone to the stored-mode index, {@code stray}
     *               a Lucene field no mapper knows to its first document, {@code delete <i>} commits
     *               before the last document, deletes document {@code i}, and force-merges the index
     *               into one segment after the last; null for none
     */
    public static String run(String mappingJson, Map<String, String> settings, List<String> documents, String tmp,
                             String inject) throws IOException {
        IsolatedEsBootstrap.ensure();
        Path root = Files.createDirectories(Path.of(tmp));
        Map<String, Object> result = new LinkedHashMap<>();
        Indexed stored = index(mappingJson, settings, documents, root.resolve("stored"), inject);
        String rewriteMapping = withoutSource(stored.finalMapping());
        Indexed twin = index(rewriteMapping, ReconstructionCoverage.rewriteSettings(settings), documents, root.resolve("twin"), null);
        result.put("indexFailures", stored.failures());

        List<Integer> counted = new ArrayList<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        try (var worker = new IsolatedReconstructionWorkerImpl(root.resolve("stored"), "probe", stored.finalMapping(),
                 settings, rewriteMapping, counted::add);
             var native_ = new IsolatedReconstructionWorkerImpl(root.resolve("twin"), "probe", twin.finalMapping(),
                 ReconstructionCoverage.rewriteSettings(settings))) {
            result.put("losesUnmappedValues", worker.losesUnmappedValues());
            result.put("acceptedDiversions", List.copyOf(acceptedDiversions(worker)));
            for (int i = 0; i < documents.size(); i++) {
                Integer storedRoot = stored.roots().get(i);
                Integer twinRoot = twin.roots().get(i);
                if (storedRoot == null || twinRoot == null) continue;
                Map<String, Object> doc = new LinkedHashMap<>();
                doc.put("doc", i);
                doc.put("storedIgnored", stored.ignored().get(i));
                doc.put("twinIgnored", twin.ignored().get(i));
                doc.put("native", new String(native_.readSource(twinRoot), java.nio.charset.StandardCharsets.UTF_8));
                try {
                    doc.put("rewrite", new String(worker.readSource(storedRoot), java.nio.charset.StandardCharsets.UTF_8));
                    doc.put("counted", counted.contains(storedRoot));
                } catch (UnreconstructableSourceException refused) {
                    doc.put("refused", refused.tokens());
                }
                docs.add(doc);
            }
        } catch (UnreconstructableSourceException refused) {
            result.put("refused", refused.tokens());
        }
        result.put("docs", docs);
        result.put("leaves", leafRefusals(root.resolve("stored"), stored.finalMapping(), settings, rewriteMapping));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(result);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    private static java.util.Set<String> acceptedDiversions(IsolatedReconstructionWorkerImpl worker) {
        try {
            var field = IsolatedReconstructionWorkerImpl.class.getDeclaredField("setup");
            field.setAccessible(true);
            ReconstructionCoverage coverage = ((IsolatedReconstructionWorkerImpl.Setup) field.get(worker)).coverage();
            return coverage == null ? java.util.Set.of() : coverage.acceptedDiversions();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Per segment of the stored-mode index, the per-segment refusals on the real leaf, on the
     * FieldInfos leaf the commit-point preflight uses, and on the real leaf with its
     * {@code _field_names} postings hidden; empty when the plan cannot be built.
     */
    private static List<Map<String, Object>> leafRefusals(Path dir, String mapping, Map<String, String> settings,
                                                          String rewriteMapping) throws IOException {
        List<Map<String, Object>> leaves = new ArrayList<>();
        MapperService original;
        MapperService rewrite;
        try {
            original = IsolatedReconstructionWorkerImpl.buildMapperService("probe", mapping, settings);
        } catch (IOException | RuntimeException unbuildable) {
            return leaves;
        }
        try (original) {
            try {
                rewrite = IsolatedReconstructionWorkerImpl.buildMapperService("probe", rewriteMapping,
                    ReconstructionCoverage.rewriteSettings(settings));
            } catch (IOException | RuntimeException unbuildable) {
                return leaves;
            }
            try (rewrite; var directory = FSDirectory.open(dir); var reader = DirectoryReader.open(directory)) {
                ReconstructionCoverage coverage = ReconstructionCoverage.plan(original, rewrite);
                SegmentInfos commit = SegmentInfos.readLatestCommit(directory);
                Map<String, SegmentCommitInfo> byName = new LinkedHashMap<>();
                for (SegmentCommitInfo info : commit) byName.put(info.info.name, info);
                for (LeafReaderContext leaf : reader.leaves()) {
                    SegmentCommitInfo info = byName.get(((SegmentReader) leaf.reader()).getSegmentName());
                    LeafMetaData metaData = new LeafMetaData(commit.getIndexCreatedVersionMajor(), info.info.getMinVersion(),
                        info.info.getIndexSort(), info.info.getHasBlocks());
                    Map<String, Object> segment = new LinkedHashMap<>();
                    segment.put("real", String.join(" ", coverage.refusals(leaf.reader())));
                    segment.put("stub", String.join(" ", coverage.refusals(
                        new FieldInfosLeafReader(IsolatedCoverageCheck.fieldInfos(info), info.info.maxDoc(), metaData))));
                    segment.put("pruned", String.join(" ", coverage.refusals(new WithoutFieldNames(leaf.reader()))));
                    leaves.add(segment);
                }
            }
        }
        return leaves;
    }

    /** A leaf opened without the {@code _field_names} postings, as a pruned (selective) read could open it. */
    private static final class WithoutFieldNames extends FilterLeafReader {
        WithoutFieldNames(LeafReader in) {
            super(in);
        }

        @Override
        public Terms terms(String field) throws IOException {
            return "_field_names".equals(field) ? null : super.terms(field);
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return null;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return null;
        }
    }

    private record Indexed(String finalMapping, Map<Integer, Integer> roots, Map<Integer, List<String>> ignored,
                           List<String> failures) {}

    private static Indexed index(String mappingJson, Map<String, String> settings, List<String> documents, Path dir,
                                 String inject) throws IOException {
        Map<Integer, Integer> roots = new LinkedHashMap<>();
        Map<Integer, List<String>> ignored = new LinkedHashMap<>();
        List<String> failures = new ArrayList<>();
        String finalMapping;
        try (MapperService mapper = IsolatedReconstructionWorkerImpl.buildMapperService("probe", mappingJson, settings);
             var directory = FSDirectory.open(dir);
             var writer = new IndexWriter(directory, new IndexWriterConfig())) {
            int next = 0;
            Integer deleted = inject != null && inject.startsWith("delete ") ? Integer.valueOf(inject.substring(7).strip()) : null;
            Map<Integer, BytesRef> ids = new LinkedHashMap<>();
            for (int i = 0; i < documents.size(); i++) {
                if (documents.get(i).startsWith(UPDATE)) {
                    mapper.merge("_doc", new CompressedXContent(documents.get(i).substring(UPDATE.length())),
                        MapperService.MergeReason.MAPPING_UPDATE);
                    continue;
                }
                if (deleted != null && i == documents.size() - 1) {
                    writer.commit();
                    writer.deleteDocuments(new Term("_id", ids.get(deleted)));
                }
                ParsedDocument parsed;
                try {
                    parsed = parse(mapper, i, documents.get(i));
                } catch (Exception e) {
                    failures.add(i + ":" + e.getClass().getSimpleName());
                    continue;
                }
                parsed.updateSeqID(i, 1);
                parsed.version().setLongValue(1);
                if ("stray".equals(inject) && roots.isEmpty()) {
                    parsed.rootDoc().add(new org.apache.lucene.document.StringField("stray", "x",
                        org.apache.lucene.document.Field.Store.NO));
                }
                writer.addDocuments(parsed.docs());
                next += parsed.docs().size();
                roots.put(i, next - 1);
                ids.put(i, BytesRef.deepCopyOf(parsed.rootDoc().getField("_id").binaryValue()));
            }
            if (deleted != null) {
                writer.commit();
                writer.forceMerge(1);
                roots.remove(deleted);
            }
            if ("tombstone".equals(inject)) {
                ParsedDocument tombstone = ParsedDocument.deleteTombstone(
                    org.elasticsearch.index.mapper.SeqNoFieldMapper.SeqNoIndexOptions.POINTS_AND_DOC_VALUES, "gone");
                tombstone.updateSeqID(documents.size(), 1);
                tombstone.version().setLongValue(2);
                writer.addDocuments(tombstone.docs());
            }
            writer.commit();
            finalMapping = withoutTypeWrapper(mapper.documentMapper().mappingSource().string());
            if (deleted != null) {
                // The merge renumbered the documents: find each root again by its _id.
                try (var reader = DirectoryReader.open(directory)) {
                    for (Map.Entry<Integer, Integer> root : roots.entrySet()) root.setValue(rootById(reader, ids.get(root.getKey())));
                }
            }
        }
        try (var directory = FSDirectory.open(dir); var reader = DirectoryReader.open(directory)) {
            for (Map.Entry<Integer, Integer> root : roots.entrySet()) {
                ignored.put(root.getKey(), ignoredValues(reader, root.getValue()));
            }
        }
        return new Indexed(finalMapping, roots, ignored, failures);
    }

    /** The last (root) live document carrying {@code id}; nested documents carry their root's _id too. */
    private static int rootById(DirectoryReader reader, BytesRef id) throws IOException {
        int root = -1;
        for (LeafReaderContext leaf : reader.leaves()) {
            PostingsEnum postings = leaf.reader().postings(new Term("_id", id), PostingsEnum.NONE);
            if (postings == null) continue;
            var live = leaf.reader().getLiveDocs();
            for (int doc = postings.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = postings.nextDoc()) {
                if (live == null || live.get(doc)) root = Math.max(root, leaf.docBase + doc);
            }
        }
        if (root < 0) throw new IllegalStateException("document not found after the merge");
        return root;
    }

    private static ParsedDocument parse(MapperService mapper, int id, String document) throws IOException {
        ParsedDocument parsed = mapper.documentMapper().parse(new SourceToParse("d" + id, new BytesArray(document), XContentType.JSON));
        if (parsed.dynamicMappingsUpdate() != null) {
            mapper.merge("_doc", new CompressedXContent(parsed.dynamicMappingsUpdate().toString()),
                MapperService.MergeReason.MAPPING_UPDATE);
            parsed = mapper.documentMapper().parse(new SourceToParse("d" + id, new BytesArray(document), XContentType.JSON));
        }
        return parsed;
    }

    /** {@code _ignored} of one document: doc values from 8.15 on, a stored field before. */
    private static List<String> ignoredValues(DirectoryReader reader, int docId) throws IOException {
        Set<String> values = new TreeSet<>();
        for (LeafReaderContext leaf : reader.leaves()) {
            int leafDoc = docId - leaf.docBase;
            if (leafDoc < 0 || leafDoc >= leaf.reader().maxDoc()) continue;
            SortedSetDocValues docValues = leaf.reader().getSortedSetDocValues("_ignored");
            if (docValues != null && docValues.advanceExact(leafDoc)) {
                for (int i = 0; i < docValues.docValueCount(); i++) values.add(docValues.lookupOrd(docValues.nextOrd()).utf8ToString());
            }
            StoredFields storedFields = leaf.reader().storedFields();
            for (var field : storedFields.document(leafDoc, Set.of("_ignored")).getFields("_ignored")) {
                values.add(field.stringValue());
            }
        }
        return new ArrayList<>(values);
    }

    @SuppressWarnings("unchecked")
    private static String withoutTypeWrapper(String mapping) throws IOException {
        Map<String, Object> map = XContentHelper.convertToMap(new BytesArray(mapping), true, XContentType.JSON).v2();
        if (map.size() == 1 && map.get("_doc") instanceof Map<?, ?> inner) map = (Map<String, Object>) inner;
        return render(map);
    }

    private static String withoutSource(String mapping) throws IOException {
        Map<String, Object> map = new LinkedHashMap<>(XContentHelper.convertToMap(new BytesArray(mapping), true, XContentType.JSON).v2());
        map.remove("_source");
        return render(map);
    }

    private static String render(Map<String, Object> map) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(map);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    /** Mapping-level refusals of a plan whose accepted diversion set is {@code ignoredRecordedDiversions}. */
    public static String mappingRefusals(String mappingJson, Map<String, String> settings, java.util.Set<String> ignoredRecordedDiversions)
            throws IOException {
        IsolatedEsBootstrap.ensure();
        try (MapperService original = IsolatedReconstructionWorkerImpl.buildMapperService("probe", mappingJson, settings);
             MapperService rewrite = IsolatedReconstructionWorkerImpl.buildMapperService("probe", withoutSource(mappingJson),
                 ReconstructionCoverage.rewriteSettings(settings))) {
            return String.join(" ", ReconstructionCoverage.plan(original, rewrite, ignoredRecordedDiversions).mappingRefusals(false));
        }
    }

    /** For callers that only need the coverage plan's tokens of a mapping (mapping-level refusals). */
    public static String mappingRefusals(String mappingJson, Map<String, String> settings, boolean acceptUnmappedValues)
            throws IOException {
        IsolatedEsBootstrap.ensure();
        MapperService original = IsolatedReconstructionWorkerImpl.buildMapperService("probe", mappingJson, settings);
        try {
            ReconstructionCoverage coverage = ReconstructionCoverage.open("probe", original, withoutSource(mappingJson), settings,
                query -> null, acceptUnmappedValues);
            if (coverage == null) {
                original.close();
                return "native";
            }
            coverage.rewrite().close();
            return "";
        } catch (UnreconstructableSourceException refused) {
            original.close();
            return refused.tokens();
        }
    }
}
