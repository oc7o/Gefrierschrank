package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Test helper of FieldInfosLeafReaderTest and CoverageNoFalseRefusalTest, defined by the isolated
 * loader: per segment of a shard, the per-segment refusals of a coverage plan evaluated on the real
 * leaf and on the FieldInfos-only leaf the preflight uses. A mapping whose source is synthetic is
 * planned as if its source were stored and disabled (the rewrite of the same mapping), so every
 * fixture segment exercises the rule. JDK types across the boundary: one JSON array.
 */
public final class CoverageSegmentsProbe {

    private CoverageSegmentsProbe() {}

    public static String refusalsPerSegment(String shardDir, String mappingJson, Map<String, String> flatSettings)
            throws IOException {
        IsolatedEsBootstrap.ensure();
        Map<String, String> stored = new LinkedHashMap<>(flatSettings);
        stored.put("index.mapping.source.mode", "stored");
        String original = withoutSourceMode(mappingJson);
        List<Map<String, Object>> segments = new ArrayList<>();
        try (MapperService originalService = IsolatedReconstructionWorkerImpl.buildMapperService("probe", original, stored);
             MapperService rewrite = IsolatedReconstructionWorkerImpl.buildMapperService("probe", withoutSource(mappingJson),
                 ReconstructionCoverage.rewriteSettings(flatSettings));
             Directory directory = new NIOFSDirectory(Path.of(shardDir));
             DirectoryReader reader = DirectoryReader.open(directory)) {
            ReconstructionCoverage coverage = ReconstructionCoverage.plan(originalService, rewrite);
            SegmentInfos commit = SegmentInfos.readLatestCommit(directory);
            Map<String, SegmentCommitInfo> byName = new LinkedHashMap<>();
            for (SegmentCommitInfo info : commit) byName.put(info.info.name, info);
            for (LeafReaderContext leaf : reader.leaves()) {
                SegmentReader segmentReader = (SegmentReader) leaf.reader();
                SegmentCommitInfo info = byName.get(segmentReader.getSegmentName());
                LeafMetaData metaData = new LeafMetaData(commit.getIndexCreatedVersionMajor(), info.info.getMinVersion(),
                    info.info.getIndexSort(), info.info.getHasBlocks());
                Map<String, Object> segment = new LinkedHashMap<>();
                segment.put("segment", info.info.name);
                segment.put("hasSource", segmentReader.getFieldInfos().fieldInfo("_source") != null);
                segment.put("real", String.join(" ", coverage.refusals(segmentReader)));
                segment.put("unattributed", String.join(" ", coverage.unattributed(segmentReader)));
                segment.put("stub", String.join(" ", coverage.refusals(
                    new FieldInfosLeafReader(IsolatedCoverageCheck.fieldInfos(info), info.info.maxDoc(), metaData))));
                segments.add(segment);
            }
        }
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startArray();
            for (Map<String, Object> segment : segments) builder.map(segment);
            builder.endArray();
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    private static String withoutSource(String mapping) throws IOException {
        Map<String, Object> map = new LinkedHashMap<>(XContentHelper.convertToMap(new BytesArray(mapping), true, XContentType.JSON).v2());
        map.remove("_source");
        return render(map);
    }

    /** The mapping with its {@code _source.mode} removed (the stored-mode original of a synthetic fixture). */
    @SuppressWarnings("unchecked")
    private static String withoutSourceMode(String mapping) throws IOException {
        Map<String, Object> map = new LinkedHashMap<>(XContentHelper.convertToMap(new BytesArray(mapping), true, XContentType.JSON).v2());
        if (map.get("_source") instanceof Map<?, ?> source) {
            Map<String, Object> copy = new LinkedHashMap<>((Map<String, Object>) source);
            copy.remove("mode");
            if (copy.isEmpty()) map.remove("_source");
            else map.put("_source", copy);
        }
        return render(map);
    }

    private static String render(Map<String, Object> map) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(map);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }
}
