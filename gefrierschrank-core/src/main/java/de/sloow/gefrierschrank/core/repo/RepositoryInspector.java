package de.sloow.gefrierschrank.core.repo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import de.sloow.gefrierschrank.core.smile.EnvelopeDecoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * High-level read access to a snapshot repository: resolves the generation chain
 * ({@code index.latest} → {@code index-N}) and the per-shard file mappings
 * ({@code indices/<repo-uuid>/<shard>/snap-<snapshot-uuid>.dat}).
 *
 * <p>Safe for concurrent callers. The first lookup pins one repository generation for the
 * lifetime of the inspector, including mounted-source lookups; returned collections are
 * unmodifiable and shared between callers.
 */
public final class RepositoryInspector {

    private final BlobStore blobs;
    private final ObjectMapper json = new ObjectMapper();
    private final ObjectMapper smile = new ObjectMapper(new SmileFactory());

    // index-N lists every retained snapshot, index and shard generation, so on a long-lived
    // repository it is large; it is fetched and indexed once rather than per lookup. Safe to keep
    // for the inspector's lifetime because this tool only reads, and the snapshots it exports are
    // complete and immutable.
    private volatile Catalog catalog;
    private final Object catalogLock = new Object();

    /**
     * One pinned index-N generation and its lookup maps. Immutable; published through the
     * volatile field, so every reader sees a fully built instance.
     */
    private record Catalog(long generation, JsonNode repoData, RepositoryOverview overview,
                           Map<String, IndexOverview> indicesByName,
                           Map<String, Set<String>> snapshotUuidsByIndex,
                           Map<String, SnapshotOverview> snapshotsByUuid,
                           Map<String, SnapshotOverview> snapshotsByName,
                           Map<String, List<JsonNode>> snapshotNodesByUuid) {

        IndexOverview index(String indexName) throws IOException {
            IndexOverview index = indicesByName.get(indexName);
            if (index == null) throw new IOException("Index not in repository: " + indexName);
            return index;
        }
    }

    public RepositoryInspector(BlobStore blobs) {
        this.blobs = blobs;
    }

    public String repositoryUuid() throws IOException {
        String uuid = catalog().repoData().path("uuid").asText(null);
        return uuid == null || uuid.equals("_na_") || uuid.isBlank() ? null : uuid;
    }

    public RepositoryOverview overview() throws IOException {
        return catalog().overview();
    }

    private Catalog catalog() throws IOException {
        Catalog loaded = catalog;
        if (loaded != null) return loaded;
        synchronized (catalogLock) {
            if (catalog == null) catalog = loadCatalog();
            return catalog;
        }
    }

    /** Reads index.latest exactly once per inspector: a failed load leaves nothing pinned. */
    private Catalog loadCatalog() throws IOException {
        long generation = readLatestGeneration();
        JsonNode repoData = json.readTree(readBlob("index-" + generation));

        List<SnapshotOverview> snapshots = new ArrayList<>();
        Map<String, SnapshotOverview> snapshotsByUuid = new HashMap<>();
        Map<String, SnapshotOverview> snapshotsByName = new HashMap<>();
        Map<String, List<JsonNode>> snapshotNodesByUuid = new HashMap<>();
        for (JsonNode snap : repoData.path("snapshots")) {
            var snapshot = new SnapshotOverview(
                snap.path("name").asText(),
                snap.path("uuid").asText(),
                snap.path("state").asInt(),
                snap.path("version").asText(),
                snap.path("start_time_millis").asLong(-1),
                snap.path("index_version").asLong(-1)
            );
            snapshots.add(snapshot);
            // First occurrence wins, as the scans this replaces returned their first match.
            snapshotsByUuid.putIfAbsent(snapshot.uuid(), snapshot);
            snapshotsByName.putIfAbsent(snapshot.name(), snapshot);
            snapshotNodesByUuid.computeIfAbsent(snapshot.uuid(), uuid -> new ArrayList<>()).add(snap);
        }

        List<IndexOverview> indices = new ArrayList<>();
        Map<String, IndexOverview> indicesByName = new HashMap<>();
        Map<String, Set<String>> snapshotUuidsByIndex = new HashMap<>();
        JsonNode indicesNode = repoData.path("indices");
        Iterator<Map.Entry<String, JsonNode>> fields = indicesNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            var index = new IndexOverview(
                field.getKey(),
                field.getValue().path("id").asText(),
                Collections.unmodifiableList(stringList(field.getValue().path("shard_generations"))),
                Collections.unmodifiableList(stringList(field.getValue().path("snapshots")))
            );
            indices.add(index);
            indicesByName.putIfAbsent(index.name(), index);
            snapshotUuidsByIndex.putIfAbsent(index.name(), Collections.unmodifiableSet(new HashSet<>(index.snapshotUuids())));
        }

        var overview = new RepositoryOverview(
            generation,
            repoData.path("min_version").asText(),
            unmodifiable(snapshots),
            unmodifiable(indices)
        );
        Map<String, List<JsonNode>> nodes = new HashMap<>();
        snapshotNodesByUuid.forEach((uuid, list) -> nodes.put(uuid, unmodifiable(list)));
        return new Catalog(generation, repoData, overview, Collections.unmodifiableMap(indicesByName),
            Collections.unmodifiableMap(snapshotUuidsByIndex), Collections.unmodifiableMap(snapshotsByUuid),
            Collections.unmodifiableMap(snapshotsByName), Collections.unmodifiableMap(nodes));
    }

    /**
     * An unmodifiable view of a private copy. Unlike List.copyOf, {@code contains(null)} and
     * {@code indexOf(null)} keep answering false and -1 as the ArrayLists callers used to get.
     */
    private static <T> List<T> unmodifiable(List<T> list) {
        return Collections.unmodifiableList(new ArrayList<>(list));
    }

    /**
     * Reads the ES mapping of an index from its repository metadata blob.
     * Path: index-N snapshots[].index_metadata_lookup[repoUuid] →
     * index_metadata_identifiers → meta-&lt;uuid&gt;.dat → {"&lt;index&gt;": {"mappings": ...}}.
     */
    public IndexMapping indexMapping(String indexName) throws IOException {
        JsonNode meta = indexMetadata(indexName);
        JsonNode mappings = meta.path("mappings");
        // Jackson serializes the singleton "mappings" field as a single-element array
        if (mappings.isArray()) {
            mappings = mappings.path(0);
        }
        // ES 7+ wraps properties in the type name (_doc); older indices sit flat
        JsonNode properties = mappings.has("_doc")
            ? mappings.path("_doc").path("properties")
            : mappings.path("properties");
        return new IndexMapping(properties);
    }

    /**
     * The full per-index metadata document from {@code meta-<uuid>.dat}: both {@code mappings} and
     * {@code settings}. {@link #indexMapping} narrows this to the field properties; callers that
     * need index settings (e.g. to detect {@code index.mode: time_series}) use this directly.
     */
    public JsonNode indexMetadata(String indexName) throws IOException {
        return indexMetadata(indexName, latestSnapshotUuid(indexName));
    }

    /** Metadata and segment files must come from the SAME snapshot. */
    public JsonNode indexMetadata(String indexName, String snapshotUuid) throws IOException {
        Catalog catalog = catalog();
        JsonNode repoData = catalog.repoData();

        JsonNode indexEntry = repoData.path("indices").path(indexName);
        if (indexEntry.isMissingNode()) {
            throw new IOException("Index not in repository: " + indexName);
        }
        String repoUuid = indexEntry.path("id").asText();

        String lookupId = null;
        for (JsonNode snap : catalog.snapshotNodesByUuid().getOrDefault(snapshotUuid, List.of())) {
            JsonNode lookup = snap.path("index_metadata_lookup");
            if (lookup.has(repoUuid)) {
                lookupId = lookup.get(repoUuid).asText();
                break;
            }
        }
        requireSnapshot(indexName, snapshotUuid);
        // Older repositories predate metadata deduplication and use the snapshot UUID.
        String metaUuid = lookupId == null ? snapshotUuid
            : repoData.path("index_metadata_identifiers").path(lookupId).asText();
        if (metaUuid.isEmpty()) {
            throw new IOException("No meta blob registered for lookup id " + lookupId);
        }

        JsonNode metadata = decodeEnvelope("indices/" + repoUuid + "/meta-" + metaUuid + ".dat").path(indexName);
        if (!metadata.isObject()) throw new IOException("Index metadata is missing its index object");
        return metadata;
    }

    public ShardSnapshot shardSnapshot(String indexName, int shardId) throws IOException {
        return shardSnapshot(indexName, shardId, latestSnapshotUuid(indexName));
    }

    public String latestSnapshotUuid(String indexName) throws IOException {
        Catalog catalog = catalog();
        IndexOverview index = catalog.index(indexName);
        if (index.snapshotUuids().isEmpty()) throw new IOException("No snapshot references index " + indexName);
        // Timestamp order when available; retain repository order for legacy metadata.
        String selected = null;
        long newest = Long.MIN_VALUE;
        for (String uuid : index.snapshotUuids()) {
            SnapshotOverview snapshot = catalog.snapshotsByUuid().get(uuid);
            if (snapshot == null) throw new IOException("Snapshot reference is missing from repository catalog");
            if (selected == null || snapshot.startTimeMillis() >= newest) {
                selected = uuid;
                newest = snapshot.startTimeMillis();
            }
        }
        return selected;
    }

    public IndexOverview index(String indexName) throws IOException {
        return catalog().index(indexName);
    }

    public SnapshotOverview requireSnapshot(String indexName, String uuid) throws IOException {
        Catalog catalog = catalog();
        catalog.index(indexName);
        if (!catalog.snapshotUuidsByIndex().get(indexName).contains(uuid))
            throw new IOException("Snapshot does not contain the requested index");
        SnapshotOverview snapshot = catalog.snapshotsByUuid().get(uuid);
        if (snapshot == null) throw new IOException("Snapshot is absent from repository catalog");
        return snapshot;
    }

    /**
     * Whether this repository holds {@code indexName} under the repository index ID
     * {@code repoUuid}, and snapshot {@code snapshotUuid} references it. Mounted-index resolution
     * uses it to find the repository that holds a backing snapshot.
     */
    public boolean containsSnapshot(String indexName, String repoUuid, String snapshotUuid) throws IOException {
        Catalog catalog = catalog();
        IndexOverview index = catalog.indicesByName().get(indexName);
        return index != null && index.repoUuid().equals(repoUuid)
            && catalog.snapshotUuidsByIndex().get(indexName).contains(snapshotUuid);
    }

    /**
     * The snapshot of {@code indexName} whose UUID, or else whose name, equals {@code nameOrUuid}.
     * Fails like {@link #requireSnapshot} when no retained snapshot matches or the match does not
     * contain the index.
     */
    public SnapshotOverview snapshotOfIndex(String indexName, String nameOrUuid) throws IOException {
        Catalog catalog = catalog();
        SnapshotOverview match = catalog.snapshotsByUuid().get(nameOrUuid);
        if (match == null) match = catalog.snapshotsByName().get(nameOrUuid);
        if (match == null) throw new IOException("Snapshot is absent from repository catalog");
        return requireSnapshot(indexName, match.uuid());
    }

    public ShardSnapshot shardSnapshot(String indexName, int shardId, String snapshotUuid) throws IOException {
        IndexOverview index = catalog().index(indexName);
        if (shardId < 0) {
            throw new IOException("Shard %d not present (index has %d shard generations)"
                .formatted(shardId, index.shardGenerations().size()));
        }

        String shardDir = "indices/" + index.repoUuid() + "/" + shardId;
        // Materialize one exact immutable snapshot. Mounted indices can intentionally refer
        // to an older snapshot than latestSnapshotUuid(indexName).
        requireSnapshot(indexName, snapshotUuid);

        JsonNode shardData = decodeEnvelope(shardDir + "/snap-" + snapshotUuid + ".dat");
        JsonNode filesNode = shardData.path("files");
        if (!filesNode.isArray()) throw new IOException("Shard metadata has no file list");
        List<RepoFileInfo> files = new ArrayList<>();
        for (JsonNode file : filesNode) {
            files.add(new RepoFileInfo(
                file.path("name").asText(),
                file.path("physical_name").asText(),
                file.path("length").asLong(),
                file.path("checksum").asText(),
                file.path("written_by").asText(null),
                file.path("meta_hash").asText(null),
                file.path("part_size").asLong(0)
            ));
        }

        return new ShardSnapshot(
            index.repoUuid(),
            shardDir + "/",
            shardData.path("name").asText(),
            shardData.path("index_version").asInt(),
            files.stream().mapToLong(RepoFileInfo::length).sum(),
            files
        );
    }

    /**
     * Every blob key (repository-relative) that the CURRENT index-N view says is still needed:
     * root generation pointers, every retained snapshot's top-level SnapshotInfo/global-state
     * blobs, every index's per-index metadata blob, and - critically - every shard's
     * <em>consolidated</em> {@code index-<shardGen>} listing plus every data blob any of that
     * shard's retained snapshots still reference (the same {@code BlobStoreIndexShardSnapshots}
     * union Elasticsearch's own {@code _snapshot/<repo>/_cleanup} API consults, i.e. the union
     * across ALL retained snapshots referencing that shard - not just the one snapshot
     * {@link #shardSnapshot} happens to resolve to). Anything present in the blob store but
     * absent from this set is unreferenced by any current, restorable snapshot: safe to consider
     * for garbage collection the way {@code _cleanup} would, and exactly what a diff against a
     * bulk repository listing should treat as "orphaned".
     *
     * <p>Deliberately does NOT reuse {@link #shardSnapshot}: that method picks one snapshot's
     * file list (the current view of a single Lucene commit, needed so a consistent set of
     * physical names can be materialized to disk for reading). Reachability for orphan detection
     * must instead be the union across every retained snapshot of a shard, since a still-retained
     * older snapshot's blobs are legitimately restorable and must never be reported as orphaned.
     */
    public Set<String> reachableBlobKeys() throws IOException {
        Catalog catalog = catalog();
        JsonNode repoData = catalog.repoData();
        var reachable = new HashSet<String>();

        reachable.add("index.latest");
        reachable.add("index-" + catalog.generation());

        // Every lookup id any snapshot records per repository index ID, in snapshot order: one
        // pass instead of a scan of every snapshot per index.
        Map<String, List<String>> lookupIdsByRepoUuid = new HashMap<>();
        for (JsonNode snap : repoData.path("snapshots")) {
            Iterator<Map.Entry<String, JsonNode>> lookups = snap.path("index_metadata_lookup").fields();
            while (lookups.hasNext()) {
                Map.Entry<String, JsonNode> lookup = lookups.next();
                lookupIdsByRepoUuid.computeIfAbsent(lookup.getKey(), id -> new ArrayList<>()).add(lookup.getValue().asText());
            }
        }

        for (JsonNode snap : repoData.path("snapshots")) {
            String uuid = snap.path("uuid").asText();
            if (uuid.isEmpty()) continue;
            // Top-level per-snapshot blobs: SnapshotInfo (snap-<uuid>.dat) and, when the
            // snapshot captured cluster state, global metadata (meta-<uuid>.dat). Marked
            // reachable unconditionally - a key that doesn't actually exist simply never shows
            // up in the bulk listing, so this never hides a real orphan.
            reachable.add("snap-" + uuid + ".dat");
            reachable.add("meta-" + uuid + ".dat");
        }

        JsonNode indicesNode = repoData.path("indices");
        Iterator<Map.Entry<String, JsonNode>> fields = indicesNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String repoUuid = field.getValue().path("id").asText();
            List<String> shardGens = stringList(field.getValue().path("shard_generations"));
            List<String> snapshotUuids = stringList(field.getValue().path("snapshots"));

            // Per-index metadata blob(s): one meta-<metaUuid>.dat per distinct mapping/settings
            // revision any retained snapshot of this index used, reached via each snapshot's
            // index_metadata_lookup -> index_metadata_identifiers indirection.
            for (String lookupId : lookupIdsByRepoUuid.getOrDefault(repoUuid, List.of())) {
                String metaUuid = repoData.path("index_metadata_identifiers").path(lookupId).asText();
                if (!metaUuid.isEmpty()) {
                    reachable.add("indices/" + repoUuid + "/meta-" + metaUuid + ".dat");
                }
            }

            for (int shardId = 0; shardId < shardGens.size(); shardId++) {
                String shardDir = "indices/" + repoUuid + "/" + shardId + "/";
                String shardGen = shardGens.get(shardId);
                if (shardGen == null || shardGen.isEmpty()) continue;
                reachable.add(shardDir + "index-" + shardGen);

                // Every per-shard snap-<uuid>.dat for every snapshot still referencing this
                // shard (not just the one shardSnapshot() resolves to) - each is independently
                // restorable and must not be flagged as orphaned.
                for (String uuid : snapshotUuids) {
                    reachable.add(shardDir + "snap-" + uuid + ".dat");
                }

                // The consolidated shard listing: the union of every data blob any retained
                // snapshot of this shard still needs. Files embedded via meta_hash have no
                // separate blob (their content is inline in the consolidated/snap-*.dat JSON
                // itself), so only non-embedded entries name a real object in the blob store.
                try {
                    JsonNode consolidated = decodeEnvelope(shardDir + "index-" + shardGen);
                    for (JsonNode file : consolidated.path("files")) {
                        String blobName = file.path("name").asText();
                        if (blobName.startsWith("v__")) continue;
                        if (!blobName.isEmpty()) {
                            var info = new RepoFileInfo(blobName, file.path("physical_name").asText(),
                                file.path("length").asLong(), null, null, null, file.path("part_size").asLong(0));
                            for (int part = 0; part < info.partCount(); part++) reachable.add(shardDir + info.partName(part));
                        }
                    }
                } catch (IOException e) {
                    // A missing/corrupt consolidated listing means this method can't establish
                    // what's reachable under this shard with confidence - surfacing that loudly
                    // (skip silently would make every blob under this shard look orphaned) is
                    // the caller's call; log this shard was skipped, no partial credit assumed.
                    throw new IOException("Cannot read consolidated shard listing for "
                        + shardDir + "index-" + shardGen + ": " + e.getMessage(), e);
                }
            }
        }

        return reachable;
    }

    private JsonNode decodeEnvelope(String key) throws IOException {
        byte[] raw = readBlob(key);
        return smile.readTree(EnvelopeDecoder.decode(raw).content());
    }

    private long readLatestGeneration() throws IOException {
        byte[] raw = readBlob("index.latest");
        if (raw.length != 8) {
            throw new IOException("index.latest has unexpected length %d".formatted(raw.length));
        }
        long generation = 0;
        for (byte b : raw) {
            generation = (generation << 8) | (b & 0xFFL);
        }
        return generation;
    }

    private byte[] readBlob(String key) throws IOException {
        try (InputStream in = blobs.read(key)) {
            return in.readAllBytes();
        }
    }

    private List<String> stringList(JsonNode arrayNode) {
        List<String> result = new ArrayList<>();
        if (arrayNode.isArray()) {
            for (JsonNode item : arrayNode) {
                result.add(item.isObject() ? item.path("uuid").asText() : item.asText());
            }
        }
        return result;
    }
}
