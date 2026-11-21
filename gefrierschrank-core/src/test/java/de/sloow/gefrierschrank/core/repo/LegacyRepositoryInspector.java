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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Test oracle: {@link RepositoryInspector} as it was before the memoized
 * catalog, with only the class name changed. {@code CatalogEquivalenceTest} checks that the
 * memoized lookups give exactly these answers, tie-breaks, fallbacks and exception messages.
 * Do not edit.
 */
final class LegacyRepositoryInspector {

    private final BlobStore blobs;
    private final ObjectMapper json = new ObjectMapper();
    private final ObjectMapper smile = new ObjectMapper(new SmileFactory());

    // Memoized: index-N lists every snapshot/index/shard-generation ever taken, so on a
    // repository with years of history it can be a genuinely large document - and every one of
    // overview()/shardSnapshot()/indexMetadata() used to re-fetch and re-parse it from scratch.
    // In an --all-indices/dump-to-delta run that means re-downloading it once per shard of
    // every index, which can dominate wall-clock time on a large repository. Safe to
    // cache for the lifetime of one inspector: this tool only ever reads a repository, and the
    // snapshot it is dumping is, by definition, already complete and immutable.
    private JsonNode cachedRepoData;
    private long cachedGeneration = -1;

    LegacyRepositoryInspector(BlobStore blobs) {
        this.blobs = blobs;
    }

    public String repositoryUuid() throws IOException {
        String uuid = repoData(readLatestGeneration()).path("uuid").asText(null);
        return uuid == null || uuid.equals("_na_") || uuid.isBlank() ? null : uuid;
    }

    public RepositoryOverview overview() throws IOException {
        long generation = readLatestGeneration();
        JsonNode repoData = repoData(generation);

        List<SnapshotOverview> snapshots = new ArrayList<>();
        for (JsonNode snap : repoData.path("snapshots")) {
            snapshots.add(new SnapshotOverview(
                snap.path("name").asText(),
                snap.path("uuid").asText(),
                snap.path("state").asInt(),
                snap.path("version").asText(),
                snap.path("start_time_millis").asLong(-1)
            ));
        }

        List<IndexOverview> indices = new ArrayList<>();
        JsonNode indicesNode = repoData.path("indices");
        Iterator<Map.Entry<String, JsonNode>> fields = indicesNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            indices.add(new IndexOverview(
                field.getKey(),
                field.getValue().path("id").asText(),
                stringList(field.getValue().path("shard_generations")),
                stringList(field.getValue().path("snapshots"))
            ));
        }

        return new RepositoryOverview(
            generation,
            repoData.path("min_version").asText(),
            snapshots,
            indices
        );
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
        long generation = readLatestGeneration();
        JsonNode repoData = repoData(generation);

        JsonNode indexEntry = repoData.path("indices").path(indexName);
        if (indexEntry.isMissingNode()) {
            throw new IOException("Index not in repository: " + indexName);
        }
        String repoUuid = indexEntry.path("id").asText();

        String lookupId = null;
        for (JsonNode snap : repoData.path("snapshots")) {
            JsonNode lookup = snap.path("index_metadata_lookup");
            if (snap.path("uuid").asText().equals(snapshotUuid) && lookup.has(repoUuid)) {
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
        IndexOverview index = index(indexName);
        if (index.snapshotUuids().isEmpty()) throw new IOException("No snapshot references index " + indexName);
        var snapshots = overview().snapshots();
        // Timestamp order when available; retain repository order for legacy metadata.
        String selected = null;
        long newest = Long.MIN_VALUE;
        for (String uuid : index.snapshotUuids()) {
            SnapshotOverview snapshot = snapshots.stream().filter(s -> s.uuid().equals(uuid)).findFirst()
                .orElseThrow(() -> new IOException("Snapshot reference is missing from repository catalog"));
            if (selected == null || snapshot.startTimeMillis() >= newest) {
                selected = uuid;
                newest = snapshot.startTimeMillis();
            }
        }
        return selected;
    }

    public IndexOverview index(String indexName) throws IOException {
        return overview().indices().stream().filter(i -> i.name().equals(indexName)).findFirst()
            .orElseThrow(() -> new IOException("Index not in repository: " + indexName));
    }

    public SnapshotOverview requireSnapshot(String indexName, String uuid) throws IOException {
        if (!index(indexName).snapshotUuids().contains(uuid))
            throw new IOException("Snapshot does not contain the requested index");
        return overview().snapshots().stream().filter(s -> s.uuid().equals(uuid)).findFirst()
            .orElseThrow(() -> new IOException("Snapshot is absent from repository catalog"));
    }

    /**
     * The snapshot of {@code indexName} whose UUID, or else whose name, equals {@code nameOrUuid}.
     * Fails like {@link #requireSnapshot} when no retained snapshot matches or the match does not
     * contain the index.
     */
    public SnapshotOverview snapshotOfIndex(String indexName, String nameOrUuid) throws IOException {
        var snapshots = overview().snapshots();
        SnapshotOverview match = snapshots.stream().filter(s -> s.uuid().equals(nameOrUuid)).findFirst()
            .or(() -> snapshots.stream().filter(s -> s.name().equals(nameOrUuid)).findFirst())
            .orElseThrow(() -> new IOException("Snapshot is absent from repository catalog"));
        return requireSnapshot(indexName, match.uuid());
    }

    public ShardSnapshot shardSnapshot(String indexName, int shardId, String snapshotUuid) throws IOException {
        RepositoryOverview overview = overview();
        IndexOverview index = overview.indices().stream()
            .filter(i -> i.name().equals(indexName))
            .findFirst()
            .orElseThrow(() -> new IOException("Index not in repository: " + indexName));
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
    public java.util.Set<String> reachableBlobKeys() throws IOException {
        long generation = readLatestGeneration();
        JsonNode repoData = repoData(generation);
        var reachable = new java.util.HashSet<String>();

        reachable.add("index.latest");
        reachable.add("index-" + generation);

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
            java.util.List<String> shardGens = stringList(field.getValue().path("shard_generations"));
            java.util.List<String> snapshotUuids = stringList(field.getValue().path("snapshots"));

            // Per-index metadata blob(s): one meta-<metaUuid>.dat per distinct mapping/settings
            // revision any retained snapshot of this index used, reached via each snapshot's
            // index_metadata_lookup -> index_metadata_identifiers indirection.
            for (JsonNode snap : repoData.path("snapshots")) {
                JsonNode lookup = snap.path("index_metadata_lookup");
                if (!lookup.has(repoUuid)) continue;
                String lookupId = lookup.get(repoUuid).asText();
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

    private JsonNode repoData(long generation) throws IOException {
        if (cachedRepoData == null || cachedGeneration != generation) {
            cachedRepoData = json.readTree(readBlob("index-" + generation));
            cachedGeneration = generation;
        }
        return cachedRepoData;
    }

    private JsonNode decodeEnvelope(String key) throws IOException {
        byte[] raw = readBlob(key);
        return smile.readTree(EnvelopeDecoder.decode(raw).content());
    }

    private long readLatestGeneration() throws IOException {
        // Pin one generation for the entire run, including mounted-source lookups.
        if (cachedGeneration >= 0) return cachedGeneration;
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
