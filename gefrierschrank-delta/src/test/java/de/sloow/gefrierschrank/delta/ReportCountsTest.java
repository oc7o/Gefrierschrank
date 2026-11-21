package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.IndexOverview;
import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-shard count fields of the run report: {@code convertedSources} of the
 * source-formats shards, {@code ignoredFieldDocuments} with its stderr note, which is printed once
 * per shard only after the shard committed, and {@code unrecoverableUnmappedValues} under the
 * {@code --source-disabled-accept unmapped-values} opt-in. Each key is absent from the report JSON
 * unless set, and {@code fetch}/{@code bytesFetched} are present on every shard.
 */
class ReportCountsTest {

    static final Path SOURCE_FORMATS = Path.of("../gefrierschrank-core/src/test/resources/fixtures/source-formats-repo");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Pattern CONVERTED_NOTE =
        Pattern.compile("note: shard (\\d+) of (\\S+): (\\d+) SMILE, (\\d+) CBOR, (\\d+) YAML documents converted to JSON.*");
    private static final List<String> COUNT_KEYS = List.of("convertedSources", "ignoredFieldDocuments", "unrecoverableUnmappedValues");

    private String savedSelective;
    private String savedAccept;

    @BeforeEach
    void save() {
        savedSelective = System.getProperty(SelectiveFetch.PROPERTY);
        savedAccept = System.getProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY);
        System.clearProperty(SelectiveFetch.PROPERTY);
        System.clearProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY);
    }

    @AfterEach
    void restore() {
        restore(SelectiveFetch.PROPERTY, savedSelective);
        restore(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY, savedAccept);
    }

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }

    /** One export: its report, its stderr, and each {@code _ignored} note with the shards whose rows the table held when it was printed. */
    record Run(RunReport report, String stderr, List<Map.Entry<String, Set<Integer>>> notes) {
        RunReport.ShardReport shard(int id) {
            return report.shards().stream().filter(s -> s.shardId() == id).findFirst().orElseThrow();
        }

        boolean notedAfterCommit(String note, int shard) {
            return notes.stream().anyMatch(entry -> entry.getKey().equals(note) && entry.getValue().contains(shard));
        }
    }

    private static Run export(Path repository, String index, Path table, DumpToDeltaPipeline.ShardReaderOpener opener)
            throws Exception {
        var sources = new RepositorySources(new RepositorySources.Source("primary", new FileSystemBlobStore(repository)));
        List<Map.Entry<String, Set<Integer>>> notes = new ArrayList<>();
        var captured = new ByteArrayOutputStream();
        PrintStream previous = System.err;
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8) {
            @Override
            public void println(String line) {
                if (line != null && line.contains(" list fields in _ignored")) {
                    try {
                        Set<Integer> committed = new TreeSet<>();
                        SynthCovFixtures.rows(table).values().forEach(row -> committed.add(row.shard()));
                        notes.add(Map.entry(line, committed));
                    } catch (IOException unreadable) {
                        throw new IllegalStateException(unreadable);
                    }
                }
                super.println(line);
            }
        });
        RunReport report;
        try {
            report = new FullIndexExporter(sources, new Configuration(), opener).run(index, table.toString(), RunReportListener.NO_OP);
        } finally {
            System.setErr(previous);
        }
        String stderr = captured.toString(StandardCharsets.UTF_8);
        previous.print(stderr);
        return new Run(report, stderr, notes);
    }

    private static Run export(Path repository, String index, Path table) throws Exception {
        return export(repository, index, table, (shard, dir) -> new Lucene10ShardReader(dir));
    }

    private static JsonNode json(RunReport.ShardReport shard) {
        return JSON.valueToTree(shard);
    }

    private static long downloadableBytes(Path repository, String index, int shard) throws IOException {
        return new RepositoryInspector(new FileSystemBlobStore(repository)).shardSnapshot(index, shard).files().stream()
            .filter(f -> !f.blobName().startsWith("v__")).mapToLong(RepoFileInfo::length).sum();
    }

    private static List<String> ignoredNotes(String stderr) {
        return stderr.lines().filter(line -> line.contains(" list fields in _ignored")).toList();
    }

    @Test
    void convertedSourcesIsTheSumOfTheShardsConversionNote(@TempDir Path tmp) throws Exception {
        var inspector = new RepositoryInspector(new FileSystemBlobStore(SOURCE_FORMATS));
        int counted = 0;
        for (IndexOverview index : inspector.overview().indices()) {
            Run run = export(SOURCE_FORMATS, index.name(), tmp.resolve(index.name()));
            for (RunReport.ShardReport shard : run.report().shards()) {
                long noted = 0;
                for (String line : run.stderr().lines().toList()) {
                    Matcher note = CONVERTED_NOTE.matcher(line);
                    if (note.matches() && note.group(2).equals(index.name()) && Integer.parseInt(note.group(1)) == shard.shardId())
                        noted += Long.parseLong(note.group(3)) + Long.parseLong(note.group(4)) + Long.parseLong(note.group(5));
                }
                JsonNode report = json(shard);
                String name = index.name() + "[" + shard.shardId() + "]";
                if (shard.outcome() == RunReport.Outcome.COMMITTED && noted > 0) {
                    assertEquals(noted, shard.convertedSources(), name);
                    assertEquals(noted, report.path("convertedSources").asLong(), name);
                    counted++;
                } else {
                    assertNull(shard.convertedSources(), name);
                    assertFalse(report.has("convertedSources"), name);
                }
                assertFalse(report.has("ignoredFieldDocuments") || report.has("unrecoverableUnmappedValues"), name);
                assertEquals("full", report.path("fetch").asText(), name);
                assertEquals(downloadableBytes(SOURCE_FORMATS, index.name(), shard.shardId()), report.path("bytesFetched").asLong(), name);
            }
        }
        assertTrue(counted >= 5, "sf_smile, sf_cbor, sf_yaml, sf_nonfinite, sf_yaml_text, sf_yaml_nonfinite convert: " + counted);
        assertEquals(39L, export(SOURCE_FORMATS, "sf_smile", tmp.resolve("again-sf_smile")).shard(0).convertedSources());
    }

    @ParameterizedTest
    @ValueSource(strings = {"cov_srcoff_native", "cov_srcoff_merged", "cov8_srcoff_native"})
    void ignoredFieldDocumentsAreReportedAndNotedOnceAfterTheCommit(String index, @TempDir Path tmp) throws Exception {
        JsonNode expected = SynthCovFixtures.expected(index);
        Set<String> ignored = new HashSet<>();
        expected.path("ignored").forEach(id -> ignored.add(id.asText()));
        Path repository = SynthCovFixtures.repository(index);
        for (boolean selective : new boolean[]{false, true}) {
            if (selective) System.setProperty(SelectiveFetch.PROPERTY, "true");
            Path table = tmp.resolve(selective ? "on" : "off");
            Run run = export(repository, index, table);
            int noted = 0;
            for (int id = 0; id < 2; id++) {
                long count = SynthCovFixtures.idsOnShard(expected, id).stream().filter(ignored::contains).count();
                RunReport.ShardReport shard = run.shard(id);
                String name = index + "[" + id + "]" + (selective ? " flag on" : "");
                assertEquals(RunReport.Outcome.COMMITTED, shard.outcome(), name);
                assertEquals(count == 0 ? null : count, shard.ignoredFieldDocuments(), name);
                assertEquals(count > 0, json(shard).has("ignoredFieldDocuments"), name);
                assertFalse(json(shard).has("convertedSources") || json(shard).has("unrecoverableUnmappedValues"), name);
                assertEquals("full", shard.fetch(), name + ": an index whose _source was disabled later takes the full plan");
                assertEquals(downloadableBytes(repository, index, id), shard.bytesFetched(), name);
                if (count > 0) {
                    String note = SyntheticSourceFallbackShardReader.ignoredFieldNote(index, count);
                    assertEquals(1, ignoredNotes(run.stderr()).stream().filter(note::equals).count(), name + run.stderr());
                    assertTrue(run.notedAfterCommit(note, id), name + ": printed after the shard's commit");
                    noted++;
                }
            }
            assertTrue(noted > 0, index + " has a shard with counted documents");
            assertEquals(noted, ignoredNotes(run.stderr()).size(), "one note per counting shard, none from a reader's close");

            Run again = export(repository, index, table);
            for (RunReport.ShardReport shard : again.report().shards()) {
                assertEquals(RunReport.Outcome.SKIPPED_ALREADY_COMMITTED, shard.outcome());
                assertEquals("none", shard.fetch());
                assertEquals(0L, shard.bytesFetched());
                for (String key : COUNT_KEYS) assertFalse(json(shard).has(key), key);
            }
            assertEquals(List.of(), ignoredNotes(again.stderr()), "a skipped shard reads nothing");
        }
    }

    /**
     * An attempt that quarantines never prints the note. The injected failure is the shard's last
     * live document's metadata, read after its source, so every document was reconstructed (and
     * counted) before the write fails.
     */
    @Test
    void anAttemptThatQuarantinesNeverPrintsTheNote(@TempDir Path tmp) throws Exception {
        String index = "cov_srcoff_native";
        String failing = new RepositoryInspector(new FileSystemBlobStore(SynthCovFixtures.repository(index)))
            .shardSnapshot(index, 1).shardPrefix();
        Run run = export(SynthCovFixtures.repository(index), index, tmp.resolve("table"), (shard, dir) ->
            shard.shardPrefix().equals(failing) ? new LastDocumentFails(new Lucene10ShardReader(dir)) : new Lucene10ShardReader(dir));
        assertEquals(RunReport.Outcome.COMMITTED, run.shard(0).outcome());
        RunReport.ShardReport failed = run.shard(1);
        assertEquals(RunReport.Outcome.QUARANTINED, failed.outcome());
        assertEquals("InjectedFailure", failed.errorClass());
        assertNull(failed.ignoredFieldDocuments());
        assertEquals("full", failed.fetch());
        assertEquals(List.of(), ignoredNotes(run.stderr()), run.stderr());

        Run retried = export(SynthCovFixtures.repository(index), index, tmp.resolve("table"));
        assertEquals(RunReport.Outcome.SKIPPED_ALREADY_COMMITTED, retried.shard(0).outcome());
        assertEquals(RunReport.Outcome.COMMITTED, retried.shard(1).outcome());
        assertTrue(retried.shard(1).ignoredFieldDocuments() > 0);
        String note = SyntheticSourceFallbackShardReader.ignoredFieldNote(index, retried.shard(1).ignoredFieldDocuments());
        assertEquals(List.of(note), ignoredNotes(retried.stderr()), "the next run notes the shard once");
        assertTrue(retried.notedAfterCommit(note, 1), "after its commit");
    }

    @Test
    void unrecoverableUnmappedValuesOnlyOnTheSourcelessShardWithTheOptIn(@TempDir Path tmp) throws Exception {
        String index = "cov_srcoff_dynamic";
        Path repository = SynthCovFixtures.repository(index);
        Run without = export(repository, index, tmp.resolve("without"));
        assertEquals(RunReport.Outcome.COMMITTED, without.shard(0).outcome());
        assertEquals(RunReport.Outcome.QUARANTINED, without.shard(1).outcome());
        for (RunReport.ShardReport shard : without.report().shards()) {
            for (String key : COUNT_KEYS) assertFalse(json(shard).has(key), shard.shardId() + " " + key);
        }

        System.setProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY, SyntheticCoverage.ACCEPT_UNMAPPED_VALUES);
        Run with = export(repository, index, tmp.resolve("with"));
        assertEquals(2, with.report().committedCount(), with.report().shards().toString());
        assertNull(with.shard(0).unrecoverableUnmappedValues(), "shard 0 holds stored sources only");
        assertFalse(json(with.shard(0)).has("unrecoverableUnmappedValues"));
        assertEquals(Boolean.TRUE, with.shard(1).unrecoverableUnmappedValues());
        assertTrue(json(with.shard(1)).path("unrecoverableUnmappedValues").asBoolean());

        // Every other fixture shard committed with the opt-in carries no such flag.
        for (String other : List.of("cov_srcoff_native", "cov_nested_synth", "cov_srcmode_native")) {
            Run run = export(SynthCovFixtures.repository(other), other, tmp.resolve(other));
            for (RunReport.ShardReport shard : run.report().shards()) {
                assertEquals(RunReport.Outcome.COMMITTED, shard.outcome(), other);
                assertFalse(json(shard).has("unrecoverableUnmappedValues"), other + "[" + shard.shardId() + "]");
            }
        }
    }

    /** Fails {@code documentMetadata} of the last live document, in every fork too. */
    private static final class LastDocumentFails implements ShardReader {
        private final ShardReader delegate;

        LastDocumentFails(ShardReader delegate) {
            this.delegate = delegate;
        }

        @Override public String runtimeName() { return delegate.runtimeName(); }
        @Override public int docCount() throws IOException { return delegate.docCount(); }
        @Override public boolean isLive(int docId) throws IOException { return delegate.isLive(docId); }
        @Override public boolean storedSourceAbsent(int docId) throws IOException { return delegate.storedSourceAbsent(docId); }
        @Override public void readSource(int docId, SourceConsumer consumer) throws IOException { delegate.readSource(docId, consumer); }
        @Override public void close() throws IOException { delegate.close(); }

        @Override
        public DocumentMetadata documentMetadata(int docId) throws IOException {
            int last = delegate.docCount() - 1;
            while (last > 0 && !delegate.isLive(last)) last--;
            if (docId == last) throw new InjectedFailure();
            return delegate.documentMetadata(docId);
        }

        @Override
        public ShardReader fork() throws IOException {
            ShardReader fork = delegate.fork();
            return fork == null ? null : new LastDocumentFails(fork);
        }
    }

    static final class InjectedFailure extends IOException implements de.sloow.gefrierschrank.core.FailureDiagnostics.SafeMessage {
        InjectedFailure() {
            super("injected failure");
        }
    }
}
