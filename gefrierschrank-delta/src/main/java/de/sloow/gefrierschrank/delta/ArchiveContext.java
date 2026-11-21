package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.repo.IndexMapping;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Fixed archive columns keep original source JSON independent of ES schema evolution. */
public record ArchiveContext(String indexName, String repositoryIndexId, String snapshotUuid,
                             String snapshotName, String sourceIndex, String sourceSnapshotUuid) {
    private static final ObjectMapper JSON = de.sloow.gefrierschrank.core.DocumentJson.newMapper();

    public static IndexMapping mapping() {
        ObjectNode fields = JSON.createObjectNode();
        for (String name : List.of("_index", "_repository_index_id", "_snapshot_uuid", "_snapshot",
                "_source_index", "_source_snapshot_uuid", "_source", "_id", "_routing", "_source_kind"))
            fields.putObject(name).put("type", "keyword");
        fields.putObject("_shard").put("type", "long");
        fields.putObject("_doc").put("type", "long");
        return new IndexMapping(fields);
    }

    private static final String KEY_PREFIX = "archive-v1:";

    String key(int shard) throws IOException {
        return KEY_PREFIX + JSON.writeValueAsString(List.of(repositoryIndexId, snapshotUuid, indexName, shard,
            sourceIndex, sourceSnapshotUuid));
    }

    /** The identity components of one {@link #key} value. */
    record KeyParts(String repositoryIndexId, String snapshotUuid, String indexName, int shard,
                    String sourceIndex, String sourceSnapshotUuid) {}

    /**
     * Inverse of {@link #key}, kept beside it so the two cannot drift apart. Accepts only a value
     * {@code key} itself would produce: the parsed parts are rendered again and must reproduce the
     * input exactly. Legacy {@link ShardKey} values and anything malformed give empty, never a guess.
     */
    static Optional<KeyParts> parseKey(String key) {
        if (key == null || !key.startsWith(KEY_PREFIX)) return Optional.empty();
        try {
            var array = JSON.readTree(key.substring(KEY_PREFIX.length()));
            if (array == null || !array.isArray() || array.size() != 6) return Optional.empty();
            for (int i : new int[]{0, 1, 2, 4, 5}) if (!array.get(i).isTextual()) return Optional.empty();
            if (!array.get(3).isInt()) return Optional.empty();
            var parts = new KeyParts(array.get(0).asText(), array.get(1).asText(), array.get(2).asText(),
                array.get(3).intValue(), array.get(4).asText(), array.get(5).asText());
            var context = new ArchiveContext(parts.indexName(), parts.repositoryIndexId(), parts.snapshotUuid(),
                null, parts.sourceIndex(), parts.sourceSnapshotUuid());
            return context.key(parts.shard()).equals(key) ? Optional.of(parts) : Optional.empty();
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    String document(String source, int shard, de.sloow.gefrierschrank.core.lucene.DocumentMetadata metadata) throws IOException {
        validateSource(source);
        ObjectNode row = JSON.createObjectNode();
        row.put("_index", indexName);
        row.put("_repository_index_id", repositoryIndexId);
        row.put("_snapshot_uuid", snapshotUuid);
        row.put("_snapshot", snapshotName);
        row.put("_source_index", sourceIndex);
        row.put("_source_snapshot_uuid", sourceSnapshotUuid);
        row.put("_shard", shard);
        row.put("_doc", metadata.slot());
        row.put("_id", metadata.id());
        row.put("_routing", metadata.routing());
        row.put("_source_kind", metadata.sourceKind());
        row.put("_source", source);
        return JSON.writeValueAsString(row);
    }

    /**
     * The acceptance rule for a stored source: one JSON object and nothing after it. Validates
     * without reserializing its content: numeric precision, arrays, names, and whitespace in stored
     * source survive exactly as returned by the reader.
     */
    static void validateSource(String source) throws IOException {
        if (source == null || source.isBlank()) throw new IOException("Live document has no source payload");
        try (var parser = JSON.createParser(source)) {
            if (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.START_OBJECT)
                throw new IOException("Source is not a JSON object");
            parser.skipChildren();
            if (parser.nextToken() != null) throw new IOException("Source has trailing JSON content");
        }
    }
}
