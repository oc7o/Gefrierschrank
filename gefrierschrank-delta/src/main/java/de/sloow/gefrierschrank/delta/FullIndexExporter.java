package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.IndexMapping;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Exports one logical index, including mounted backing data, from one retained snapshot: by
 * default the snapshot a partially written table started from, else the latest (see
 * {@link SnapshotSelection}).
 */
public final class FullIndexExporter {
    /**
     * Under {@link SnapshotSelection.Mode#AUTO}, a mounted index's table whose shell snapshot
     * retention has deleted stays resumable while its backing snapshot is unchanged: it is
     * completed, and rows written later keep naming the retired shell snapshot in
     * {@code _snapshot_uuid}/{@code _snapshot}. false restores the refusal, i.e. re-exporting the
     * backing data into a new table. See {@link #retiredShell}.
     */
    static final boolean RESUME_RETIRED_SHELLS = true;

    private static final ObjectMapper JSON = new ObjectMapper();
    private final RepositorySources sources;
    private final Configuration conf;
    private final DumpToDeltaPipeline.ShardReaderOpener opener;
    private ExportSettings settings = ExportSettings.DEFAULT;
    private ShardWorkPool pool;
    private final java.util.concurrent.atomic.AtomicBoolean unlockedTargetWarned = new java.util.concurrent.atomic.AtomicBoolean();

    public FullIndexExporter(RepositorySources sources, Configuration conf,
                             DumpToDeltaPipeline.ShardReaderOpener opener) {
        this.sources = sources;
        this.conf = conf;
        this.opener = opener;
    }

    /**
     * Applies to every later {@link #run}; one configuration per CLI run. All indices share one
     * {@link ShardWorkPool}, so the worker and temp-disk limits hold across the whole run, also
     * while several indices run at once. Once configured, {@link #run} may be called concurrently
     * for different tables, provided the repository inspectors' catalogs are already loaded.
     */
    public void configure(ExportSettings settings) {
        configure(settings, null);
    }

    /**
     * {@link #configure(ExportSettings)} for a run whose tables go under {@code outputUri}; warnings
     * that concern only {@code s3a://} targets are left out for other outputs. Null, as for
     * {@link #configure(ExportSettings)}, prints them all.
     */
    public void configure(ExportSettings settings, String outputUri) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.pool = settings.parallelism() > 1
            ? ShardWorkPool.create(settings.parallelism(), settings.tempBudgetMiB()) : null;
        if (pool != null) {
            System.out.println("Processing up to " + pool.workers() + " shards concurrently across the run, temp-disk budget "
                + (pool.budgetMiB() > 0 ? pool.budgetMiB() + " MiB" : "unbounded"));
        }
        boolean s3aTarget = outputUri == null
            || "s3a".equalsIgnoreCase(new org.apache.hadoop.fs.Path(outputUri).toUri().getScheme());
        for (String warning : settings.resourceWarnings(Runtime.getRuntime().maxMemory(),
                Integer.getInteger("gefrierschrank.download.concurrency", 8),
                Integer.getInteger("gefrierschrank.s3.maxConnections", 128), s3aTarget))
            System.err.println("warning: " + warning);
    }

    /**
     * Exports {@code indexName} into {@code tableUri} while holding the table's {@link TableWriteLock},
     * taken before the table or the repository is read: a second writer on a local table fails
     * here, before any download.
     */
    public RunReport run(String indexName, String tableUri, RunReportListener listener) throws Exception {
        return runWithIdentity(indexName, tableUri, listener).report();
    }

    /** What {@link #runWithIdentity} exported: the report and the identity of the data it read. */
    public record Exported(RunReport report, RepositorySources.Identity identity) {}

    /** {@link #run}, also naming the identity of the backing data the export read, under the table's lock. */
    public Exported runWithIdentity(String indexName, String tableUri, RunReportListener listener) throws Exception {
        if (TableWriteLock.localDirectory(tableUri) == null && unlockedTargetWarned.compareAndSet(false, true)) {
            System.err.println("warning: concurrent writers on one " + new org.apache.hadoop.fs.Path(tableUri).toUri().getScheme()
                + ":// table are not detected; run one export per table");
        }
        try (var lock = TableWriteLock.acquire(tableUri)) {
            return runLocked(indexName, tableUri, listener);
        }
    }

    /**
     * What {@link #run} would export into {@code tableUri}: the identity of the backing data of the
     * snapshot it would select, whether the name is mounted, and whether the table (or its
     * {@code _export_metadata}) already holds a commit or its directory exists at all.
     */
    public record PlannedExport(RepositorySources.Identity identity, boolean mounted, boolean tableStarted) {}

    /**
     * Plans {@link #run} without the table lock and without writing anything: repository metadata
     * and Delta-log reads only, the same snapshot selection, no preflight. An absent table stays
     * absent. Its writers only read keys; every commit still goes through the one writer
     * {@link #runLocked} vets.
     */
    public PlannedExport plan(String indexName, String plannedTableUri) throws IOException {
        var inspector = sources.primary().inspector();
        var documents = new DeltaTableWriter(plannedTableUri);
        var metadata = new DeltaTableWriter(plannedTableUri + "/_export_metadata");
        String latestSnapshotUuid = inspector.latestSnapshotUuid(indexName);
        var selected = selectSnapshot(inspector, indexName, latestSnapshotUuid, documents, metadata,
            plannedTableUri + "/_export_metadata");
        var resolved = sources.resolve(indexName, selected.snapshotUuid());
        Set<String> documentKeys = selected.committedShardKeys() != null
            ? selected.committedShardKeys() : documents.committedShardKeys(conf);
        // A run killed before its first commit leaves the directory (lock file, staged Parquet)
        // without a commit; a shell aliased past it would keep that directory as debris forever.
        boolean tableStarted = !documentKeys.isEmpty() || !metadata.committedShardKeys(conf).isEmpty()
            || exists(plannedTableUri);
        return new PlannedExport(RepositorySources.identity(resolved), resolved.referenceChain().size() > 1, tableStarted);
    }

    private boolean exists(String tableUri) throws IOException {
        var path = new org.apache.hadoop.fs.Path(tableUri);
        return path.getFileSystem(conf).exists(path);
    }

    private Exported runLocked(String indexName, String tableUri, RunReportListener listener) throws Exception {
        var inspector = sources.primary().inspector();
        // One writer per table serves the snapshot selection, the conflict check and every shard
        // commit, so whatever requireArchiveSnapshot establishes about the table holds for all of them.
        var writer = new DeltaTableWriter(tableUri);
        var metadataWriter = new DeltaTableWriter(tableUri + "/_export_metadata");
        String latestSnapshotUuid = inspector.latestSnapshotUuid(indexName);
        var selected = selectSnapshot(inspector, indexName, latestSnapshotUuid, writer, metadataWriter,
            tableUri + "/_export_metadata");
        String snapshotUuid = selected.snapshotUuid();
        var snapshot = inspector.requireSnapshot(indexName, snapshotUuid);
        var resolved = sources.resolve(indexName, snapshotUuid);
        ExportPreflight.require(indexName, resolved);
        int shardCount = resolved.shardCount();
        var identity = RepositorySources.identity(resolved);
        // What the table is as of: the data snapshot, or the retired shell snapshot it was started from.
        var label = selected.retiredShell() != null ? selected.retiredShell() : new Label(snapshotUuid, snapshot.name());
        var context = new ArchiveContext(indexName, inspector.index(indexName).repoUuid(), label.snapshotUuid(),
            label.snapshotName(), resolved.indexName(), resolved.snapshotUuid());

        // Old projected exports cannot establish complete-source coverage. A different source
        // snapshot also cannot be appended to a table without mixing versions.
        writer.requireArchiveSnapshot(conf, context, shardCount);

        // Preserve exact mappings/settings and reference lineage once per index, in a companion
        // Delta table. Never repeat potentially large mappings on every document row. Rows only
        // for other snapshots mean this table belongs to one of those, even while the document
        // table itself is still empty. A row for this snapshot next to another one is 1.0.0's
        // shape after it exported an empty table from a newer snapshot; its document table is
        // consistent with this snapshot (requireArchiveSnapshot above), so it stays usable.
        String metadataKey = context.key(-1);
        Set<String> metadataKeys = metadataWriter.committedShardKeys(conf);
        if (!metadataKeys.isEmpty() && !metadataKeys.contains(metadataKey))
            throw new DeltaTableWriter.ArchiveTargetConflictException();
        if (!metadataKeys.contains(metadataKey)) {
            var metadata = JSON.createObjectNode();
            metadata.put("index_name", indexName);
            metadata.put("snapshot_uuid", snapshotUuid);
            metadata.put("snapshot_name", snapshot.name());
            metadata.put("index_metadata", inspector.indexMetadata(indexName, snapshotUuid).toString());
            metadata.put("source_index_metadata", resolved.metadata().toString());
            metadata.put("reference_chain", JSON.writeValueAsString(resolved.referenceChain()));
            metadata.put("source_snapshot_uuid", resolved.snapshotUuid());
            var properties = JSON.createObjectNode();
            metadata.fieldNames().forEachRemaining(name -> properties.putObject(name).put("type", "keyword"));
            metadataWriter.write(conf, List.of(metadata.toString()).iterator(), new IndexMapping(properties), metadataKey);
        }

        var info = new RunReport.SnapshotInfo(settings.snapshotSelection().label(), label.snapshotUuid(),
            label.snapshotName(), latestSnapshotUuid, !label.snapshotUuid().equals(latestSnapshotUuid));
        System.out.println("Exporting " + indexName + " from snapshot " + snapshot.name()
            + (resolved.referenceChain().size() > 1 ? " via " + String.join(" -> ", resolved.referenceChain()) : ""));
        if (selected.retiredShell() != null) {
            System.out.println("Completing " + indexName + " as of snapshot " + label.snapshotName()
                + ", which retention has deleted; its backing snapshot " + resolved.snapshotUuid() + " of "
                + resolved.indexName() + " is unchanged");
        } else if (info.newerSnapshotAvailable()) {
            System.out.println("Completing " + indexName + " from snapshot " + snapshot.name() + "; newer snapshot "
                + inspector.requireSnapshot(indexName, latestSnapshotUuid).name()
                + " available; this table is as of " + snapshot.name());
        }
        var pipeline = new DumpToDeltaPipeline(resolved.source().inspector(), resolved.source().blobs(),
            writer, conf, opener).withWorkPool(pool);
        RunReportListener withSnapshot = reportSoFar -> listener.onShardProcessed(reportSoFar.withSnapshot(info));
        return new Exported(pipeline.runSnapshot(context, resolved.metadata(), shardCount, withSnapshot,
            selected.committedShardKeys()).withSnapshot(info), identity);
    }

    /** A snapshot a table is as of. */
    record Label(String snapshotUuid, String snapshotName) {}

    /**
     * {@code snapshotUuid}: the snapshot whose data is exported. {@code committedShardKeys}: the
     * document table's keys if selection had to read them, else null. {@code retiredShell}: the
     * table's own snapshot when retention deleted it (see {@link #retiredShell}), else null.
     */
    private record Selected(String snapshotUuid, Set<String> committedShardKeys, Label retiredShell) {
        Selected(String snapshotUuid, Set<String> committedShardKeys) {
            this(snapshotUuid, committedShardKeys, null);
        }
    }

    private Selected selectSnapshot(RepositoryInspector inspector, String indexName, String latestSnapshotUuid,
                                    DeltaTableWriter writer, DeltaTableWriter metadataWriter,
                                    String metadataTableUri) throws IOException {
        var selection = settings.snapshotSelection();
        return switch (selection.mode()) {
            case LATEST -> new Selected(latestSnapshotUuid, null);
            case EXPLICIT -> new Selected(inspector.snapshotOfIndex(indexName, selection.token()).uuid(), null);
            case AUTO -> {
                Set<String> committed = writer.committedShardKeys(conf);
                Set<String> keys = committed.isEmpty() ? metadataWriter.committedShardKeys(conf) : committed;
                var index = inspector.index(indexName);
                var resumable = resumableSnapshot(keys, indexName, index.repoUuid(), index.snapshotUuids());
                if (resumable.isEmpty() && RESUME_RETIRED_SHELLS) {
                    Set<String> metadataKeys = committed.isEmpty() ? keys : metadataWriter.committedShardKeys(conf);
                    var retired = retiredShell(indexName, index, latestSnapshotUuid, committed, metadataKeys, metadataTableUri);
                    if (retired.isPresent()) yield new Selected(latestSnapshotUuid, committed, retired.get());
                }
                yield new Selected(resumable.orElse(latestSnapshotUuid), committed);
            }
        };
    }

    /** One {@code _export_metadata} row's identity columns; null where a column is absent. */
    record MetadataRow(String indexName, String snapshotUuid, String snapshotName, String sourceSnapshotUuid) {}

    /**
     * Reads what {@link #retiredShell(Collection, Collection, List, String, String, Collection, String, String, int)}
     * decides on. Anything unreadable means no: the caller then refuses the table as before.
     */
    private Optional<Label> retiredShell(String indexName, de.sloow.gefrierschrank.core.repo.IndexOverview index,
                                         String latestSnapshotUuid, Set<String> documentKeys, Set<String> metadataKeys,
                                         String metadataTableUri) {
        if (metadataKeys.size() != 1) return Optional.empty();
        try {
            var latest = sources.resolve(indexName, latestSnapshotUuid);
            return retiredShell(documentKeys, metadataKeys, metadataRows(metadataTableUri), indexName, index.repoUuid(),
                index.snapshotUuids(), latest.indexName(), latest.snapshotUuid(), latest.referenceChain().size());
        } catch (IOException | RuntimeException unreadable) {
            return Optional.empty();
        }
    }

    private List<MetadataRow> metadataRows(String metadataTableUri) throws IOException {
        var rows = new java.util.ArrayList<MetadataRow>();
        var snapshot = io.delta.standalone.DeltaLog.forTable(conf, metadataTableUri).update();
        if (snapshot.getVersion() < 0) return rows;
        var columns = List.of(snapshot.getMetadata().getSchema().getFieldNames());
        try (var it = snapshot.open()) {
            while (it.hasNext()) {
                var row = it.next();
                java.util.function.Function<String, String> value =
                    column -> columns.contains(column) && !row.isNullAt(column) ? row.getString(column) : null;
                rows.add(new MetadataRow(value.apply("index_name"), value.apply("snapshot_uuid"),
                    value.apply("snapshot_name"), value.apply("source_snapshot_uuid")));
            }
        }
        return rows;
    }

    /**
     * The retired shell snapshot a table of a mounted index was started from, when completing it
     * from the latest snapshot's data provably adds the same documents. A mounted (searchable
     * snapshot) index holds no data of its own: every snapshot of it only references one immutable
     * backing snapshot. So a shell snapshot deleted by retention changes nothing about the rows,
     * as long as the latest snapshot still resolves to the same backing index and snapshot. Every
     * condition must hold, else empty and the table is refused as before:
     * <ol type="a">
     *   <li>every document key and the one metadata key parse as archive keys, shard -1 exactly
     *       for the metadata key;</li>
     *   <li>they name this index, this repository index ID and one shell snapshot P;</li>
     *   <li>P is no longer retained (otherwise {@link #resumableSnapshot} applies);</li>
     *   <li>they name one backing (source index, source snapshot) pair whose snapshot is not P, so
     *       the table was written from a mounted index, never from an index's own data;</li>
     *   <li>the latest snapshot resolves through a mounted reference to that same pair;</li>
     *   <li>{@code _export_metadata} holds exactly one row, for this index, snapshot P and that
     *       backing snapshot; it supplies P's name.</li>
     * </ol>
     * The keys, the metadata key and the commit-time checks stay those of snapshot P, so
     * exactly-once holds and no second key can enter the table.
     */
    static Optional<Label> retiredShell(Collection<String> documentKeys, Collection<String> metadataKeys,
                                        List<MetadataRow> metadataRows, String indexName, String repositoryIndexId,
                                        Collection<String> retainedSnapshotUuids, String latestSourceIndex,
                                        String latestSourceSnapshotUuid, int latestReferenceChainLength) {
        if (metadataKeys.size() != 1 || metadataRows.size() != 1) return Optional.empty();
        var parsed = new java.util.ArrayList<ArchiveContext.KeyParts>();
        for (String key : metadataKeys) {
            var parts = ArchiveContext.parseKey(key).orElse(null);
            if (parts == null || parts.shard() != -1) return Optional.empty();
            parsed.add(parts);
        }
        for (String key : documentKeys) {
            var parts = ArchiveContext.parseKey(key).orElse(null);
            if (parts == null || parts.shard() < 0) return Optional.empty();
            parsed.add(parts);
        }
        var shell = parsed.get(0);
        for (var parts : parsed) {
            if (!parts.indexName().equals(indexName) || !parts.repositoryIndexId().equals(repositoryIndexId)
                || !parts.snapshotUuid().equals(shell.snapshotUuid()) || !parts.sourceIndex().equals(shell.sourceIndex())
                || !parts.sourceSnapshotUuid().equals(shell.sourceSnapshotUuid())) return Optional.empty();
        }
        String retired = shell.snapshotUuid();
        if (retainedSnapshotUuids.contains(retired) || shell.sourceSnapshotUuid().equals(retired)) return Optional.empty();
        if (latestReferenceChainLength <= 1 || !shell.sourceIndex().equals(latestSourceIndex)
            || !shell.sourceSnapshotUuid().equals(latestSourceSnapshotUuid)) return Optional.empty();
        var row = metadataRows.get(0);
        if (!indexName.equals(row.indexName()) || !retired.equals(row.snapshotUuid())
            || !shell.sourceSnapshotUuid().equals(row.sourceSnapshotUuid())
            || row.snapshotName() == null || row.snapshotName().isBlank()) return Optional.empty();
        return Optional.of(new Label(retired, row.snapshotName()));
    }

    /**
     * The snapshot a table was started from, if resuming it is safe: every key parses as an
     * archive key of this very index (same repository index ID) and names one snapshot that the
     * pinned repository generation still retains. Mixed, legacy or foreign keys give empty, so the
     * caller falls back to the latest snapshot and requireArchiveSnapshot refuses the table as before.
     */
    static Optional<String> resumableSnapshot(Collection<String> keys, String indexName, String repositoryIndexId,
                                              Collection<String> retainedSnapshotUuids) {
        String pinned = null;
        for (String key : keys) {
            var parts = ArchiveContext.parseKey(key).orElse(null);
            if (parts == null || !parts.indexName().equals(indexName)
                || !parts.repositoryIndexId().equals(repositoryIndexId)) return Optional.empty();
            if (pinned == null) pinned = parts.snapshotUuid();
            else if (!pinned.equals(parts.snapshotUuid())) return Optional.empty();
        }
        return pinned != null && retainedSnapshotUuids.contains(pinned) ? Optional.of(pinned) : Optional.empty();
    }
}
