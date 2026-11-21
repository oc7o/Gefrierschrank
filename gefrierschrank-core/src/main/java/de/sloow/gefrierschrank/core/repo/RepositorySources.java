package de.sloow.gefrierschrank.core.repo;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Resolves searchable-snapshot references by immutable IDs, never by stripping a name prefix. */
public final class RepositorySources {
    public record Source(String name, BlobStore blobs, RepositoryInspector inspector) {
        public Source(String name, BlobStore blobs) { this(name, blobs, new RepositoryInspector(blobs)); }
    }
    public record Resolved(Source source, String indexName, String snapshotUuid, JsonNode metadata,
                           List<String> referenceChain) {
        public int shardCount() throws IOException {
            String count = setting(metadata, "index.number_of_shards");
            try {
                int n = Integer.parseInt(count);
                if (n <= 0) throw new NumberFormatException();
                return n;
            } catch (NumberFormatException e) {
                throw new IOException("Snapshot metadata has no valid shard count");
            }
        }
    }

    public static final class UnresolvedSnapshotException extends IOException {
        public UnresolvedSnapshotException(String message) { super(message); }
    }

    /**
     * Two names with equal identity export the same shard data. Only this exact tuple groups:
     * never name prefixes or nearby snapshots.
     */
    public record Identity(String source, String repositoryIndexId, String index, String snapshotUuid, int shardCount) {
        public String label() {
            return source + "/" + snapshotUuid + "/" + index;
        }
    }

    /** The identity of the backing data {@code resolved} ends at. */
    public static Identity identity(Resolved resolved) throws IOException {
        return new Identity(resolved.source().name(), resolved.source().inspector().index(resolved.indexName()).repoUuid(),
            resolved.indexName(), resolved.snapshotUuid(), resolved.shardCount());
    }

    private final Source primary;
    private final Map<String, Source> sources = new LinkedHashMap<>();

    public RepositorySources(Source primary) {
        this.primary = primary;
    }
    public Source primary() { return primary; }
    public void add(Source source) {
        if (sources.putIfAbsent(source.name(), source) != null)
            throw new IllegalArgumentException("Duplicate repository alias");
    }

    public Resolved resolve(String indexName, String snapshotUuid) throws IOException {
        Source current = primary;
        var visited = new HashSet<String>();
        var chain = new ArrayList<String>();
        for (int depth = 0; depth < 32; depth++) {
            String identity = current.name() + "/" + snapshotUuid + "/" + indexName;
            if (!visited.add(identity)) throw new UnresolvedSnapshotException("Mounted snapshot reference cycle");
            chain.add(identity);
            current.inspector().requireSnapshot(indexName, snapshotUuid);
            JsonNode metadata = current.inspector().indexMetadata(indexName, snapshotUuid);
            String targetSnapshot = setting(metadata, "index.store.snapshot.snapshot_uuid");
            if (targetSnapshot == null) {
                if ("snapshot".equals(setting(metadata, "index.store.type")))
                    throw new UnresolvedSnapshotException("Mounted index has no backing snapshot UUID");
                return new Resolved(current, indexName, snapshotUuid, metadata, List.copyOf(chain));
            }
            String repository = setting(metadata, "index.store.snapshot.repository_name");
            String targetIndex = setting(metadata, "index.store.snapshot.index_name");
            String targetId = setting(metadata, "index.store.snapshot.index_uuid");
            String repositoryId = setting(metadata, "index.store.snapshot.repository_uuid");
            if (targetSnapshot.isBlank() || targetIndex == null || targetId == null)
                throw new UnresolvedSnapshotException("Mounted index has an incomplete backing snapshot reference");
            Source match = null;
            // A configured alias is authoritative. Otherwise search registered repositories by
            // snapshot UUID + repository index ID; names alone can select unrelated data.
            var available = new ArrayList<Source>();
            available.add(primary);
            available.addAll(sources.values());
            List<Source> candidates = sources.containsKey(repository)
                ? List.of(sources.get(repository)) : available;
            for (Source candidate : candidates) {
                boolean found = candidate.inspector().containsSnapshot(targetIndex, targetId, targetSnapshot);
                String actualRepositoryId = candidate.inspector().repositoryUuid();
                if (found && repositoryId != null && !repositoryId.equals("_na_") && actualRepositoryId != null
                    && !repositoryId.equals(actualRepositoryId)) continue;
                if (found) {
                    if (match != null) throw new UnresolvedSnapshotException("Backing snapshot matches multiple repositories; configure its repository alias");
                    match = candidate;
                }
            }
            if (match == null) throw new UnresolvedSnapshotException(
                "Backing snapshot unavailable: repository=" + repository + ", snapshot_uuid=" + targetSnapshot
                    + ", index=" + targetIndex + ". Register its location with --repository NAME=URI");
            current = match;
            indexName = targetIndex;
            snapshotUuid = targetSnapshot;
        }
        throw new UnresolvedSnapshotException("Mounted snapshot reference chain exceeds 32 entries");
    }

    /** Supports flat, nested, and mixed settings objects used by different ES versions. */
    public static String setting(JsonNode metadata, String key) {
        return lookup(metadata.path("settings"), key);
    }
    private static String lookup(JsonNode node, String key) {
        JsonNode direct = node.get(key);
        if (direct != null && direct.isValueNode() && !direct.isNull()) return direct.asText();
        for (int dot = key.lastIndexOf('.'); dot > 0; dot = key.lastIndexOf('.', dot - 1)) {
            JsonNode child = node.get(key.substring(0, dot));
            if (child != null) {
                String value = lookup(child, key.substring(dot + 1));
                if (value != null) return value;
            }
        }
        return null;
    }
}
