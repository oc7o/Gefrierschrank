package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@code bytesFetched} and {@code fetch} are in every shard report the pipeline writes, with or without selective fetch. */
class ReportFetchFieldsTest {

    static final Path CORE = Path.of("../gefrierschrank-core/src/test/resources/fixtures");

    @Test
    void fullFetchesReportTheirDownloadedBytes(@TempDir Path tmp) throws Exception {
        for (String[] export : List.of(new String[] {"frozen-repo", "frozen-docs-f5ffca09"},
                new String[] {"demo-repo", "test_data_9_3_0"}, new String[] {"es7-features-repo", "es7_hard_deletes"})) {
            var store = new SelectiveShardFixture.CountingStore(CORE.resolve(export[0]));
            var exporter = new FullIndexExporter(new RepositorySources(new RepositorySources.Source("primary", store)),
                new Configuration(), (shard, dir) -> new Lucene10ShardReader(dir));
            RunReport report = exporter.run(export[1], tmp.resolve(export[1]).toString(), RunReportListener.NO_OP);
            var inspector = new RepositoryInspector(new FileSystemBlobStore(CORE.resolve(export[0])));
            assertFalse(report.shards().isEmpty());
            for (RunReport.ShardReport shard : report.shards()) {
                assertEquals(RunReport.Outcome.COMMITTED, shard.outcome());
                assertEquals("full", shard.fetch());
                ShardSnapshot snapshot = inspector.shardSnapshot(export[1], shard.shardId());
                long downloaded = snapshot.files().stream().filter(f -> !f.blobName().startsWith("v__"))
                    .mapToLong(RepoFileInfo::length).sum();
                assertEquals(downloaded, shard.bytesFetched(), export[1] + " shard " + shard.shardId());
                assertNull(shard.convertedSources());
                assertNull(shard.ignoredFieldDocuments());
                assertNull(shard.unrecoverableUnmappedValues());
            }

            RunReport resumed = exporter.run(export[1], tmp.resolve(export[1]).toString(), RunReportListener.NO_OP);
            for (RunReport.ShardReport shard : resumed.shards()) {
                assertEquals(RunReport.Outcome.SKIPPED_ALREADY_COMMITTED, shard.outcome());
                assertEquals("none", shard.fetch());
                assertEquals(0L, shard.bytesFetched());
            }
        }
    }

    @Test
    void shardsThatNeverReachTheirDownloadReportNone(@TempDir Path tmp) throws Exception {
        var shard = SelectiveShardFixture.storedShard(tmp.resolve("shard"));
        var store = new SelectiveShardFixture.CountingStore(shard.repo());
        var catalog = new DumpToDeltaPipeline.ShardCatalog() {
            @Override public int shardCount() { return 3; }
            @Override public ShardSnapshot shardSnapshot(int shardId) throws IOException {
                if (shardId == 0) throw new IOException("snap file missing");
                if (shardId == 1) {
                    return new ShardSnapshot("repo", SelectiveShardFixture.PREFIX, "snap", 1, 0,
                        shard.snapshot().files().stream().filter(f -> !f.physicalName().startsWith("segments_")).toList());
                }
                return shard.snapshot();
            }
        };
        RunReport report = new DumpToDeltaPipeline(new RepositoryInspector(store), store, tmp.resolve("t").toString(),
            new Configuration(), new SelectiveShardFixture.Opener())
            .runSnapshot(SelectiveShardFixture.context(), SelectiveShardFixture.STORED_METADATA, catalog, RunReportListener.NO_OP);
        assertEquals(List.of(RunReport.Outcome.QUARANTINED, RunReport.Outcome.QUARANTINED, RunReport.Outcome.COMMITTED),
            report.shards().stream().map(RunReport.ShardReport::outcome).toList());
        assertEquals("none", report.shards().get(0).fetch());
        assertEquals(0L, report.shards().get(0).bytesFetched());
        assertEquals("MissingLuceneCommitException", report.shards().get(1).errorClass());
        assertEquals("none", report.shards().get(1).fetch());
        assertEquals(0L, report.shards().get(1).bytesFetched());
        assertEquals("full", report.shards().get(2).fetch());
        assertEquals(shard.downloadBytes(), report.shards().get(2).bytesFetched());
    }

    @Test
    void theNewKeysAreAbsentUnlessSetAndRoundTrip() throws Exception {
        var mapper = new ObjectMapper();
        var legacy = new RunReport.ShardReport(0, "snap", RunReport.Outcome.COMMITTED, 1, 2, "t", 3, null, null);
        String legacyJson = mapper.writeValueAsString(legacy);
        for (String key : List.of("bytesFetched", "fetch", "convertedSources", "ignoredFieldDocuments", "unrecoverableUnmappedValues"))
            assertFalse(legacyJson.contains(key), legacyJson);
        assertTrue(legacyJson.contains("\"errorClass\":null"), "the 1.1.0 keys keep their shape: " + legacyJson);
        assertEquals(legacy, mapper.readValue(legacyJson, RunReport.ShardReport.class));

        var full = new RunReport.ShardReport(0, "snap", RunReport.Outcome.COMMITTED, 1, 2, "t", 3, null, null,
            123L, "selective", 4L, 5L, Boolean.TRUE);
        String json = mapper.writeValueAsString(full);
        assertTrue(json.endsWith("\"bytesFetched\":123,\"fetch\":\"selective\",\"convertedSources\":4,"
            + "\"ignoredFieldDocuments\":5,\"unrecoverableUnmappedValues\":true}"), json);
        assertEquals(full, mapper.readValue(json, RunReport.ShardReport.class));
        var report = new RunReport("idx", "a", "b", 1, 1, 1, 0, 0, 1, 2, List.of(full));
        assertEquals(report, mapper.readValue(mapper.writeValueAsString(report), RunReport.class));
    }
}
