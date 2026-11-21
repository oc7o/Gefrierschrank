package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.IndexOverview;
import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shipped commit-point preflight is reconstruction's own coverage predicate: with
 * {@code --selective-fetch} (and {@link SelectiveFetch#COMMIT_PREFLIGHT} off), every shard of the
 * synthetic-coverage fixtures that the full download quarantines is quarantined after its commit
 * files only ({@code fetch: preflight}), with the same error class and message, and every other
 * shard commits the same rows. An index refused by its mapping fails before any shard download with
 * and without the flag. Without the flag, the {@code preflight=true} overload reproduces each
 * refusal. Runs in a child JVM laid out like the shaded CLI jar (synth-deletes-repo's ZSTD index
 * needs Elasticsearch's native access, see {@link Es93BinaryDocValuesPipelineTest}).
 */
class CoveragePreflightWiringTest {

    static final List<String> REPOSITORIES = List.of("synth-deletes-repo", "synthcov-repo", "synthcov8-repo");

    @TempDir
    static Path work;
    private static final List<String> LINES = new ArrayList<>();

    @BeforeAll
    static void runInChildJvm() throws Exception {
        List<String> command = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-ea", "-Djdk.util.jar.enableMultiRelease=false",
            "-cp", String.join(File.pathSeparator, Es93BinaryDocValuesPipelineTest.productionOrderedClassPath()),
            ChildMain.class.getName(), work.toString()));
        Path out = work.resolve("child.out");
        Path err = work.resolve("child.err");
        Process process = new ProcessBuilder(command).redirectOutput(out.toFile()).redirectError(err.toFile()).start();
        assertTrue(process.waitFor(10, TimeUnit.MINUTES));
        assertEquals(0, process.exitValue(), Files.readString(out) + Files.readString(err));
        for (String line : Files.readAllLines(out)) {
            if (line.startsWith("SHARD\t") || line.startsWith("INDEX\t") || line.startsWith("OVERLOAD\t") || line.startsWith("MISMATCH\t")) {
                LINES.add(line);
                System.out.println(line);
            }
        }
    }

    private static List<String[]> lines(String kind) {
        return LINES.stream().filter(line -> line.startsWith(kind + "\t")).map(line -> line.split("\t", -1)).toList();
    }

    @Test
    void withTheFlagEveryRefusedShardFailsAfterItsCommitFilesAsTheFullDownloadDoes() {
        assertEquals(List.of(), lines("MISMATCH").stream().map(l -> l[1]).toList());
        List<String[]> shards = lines("SHARD");
        long preflighted = shards.stream().filter(l -> l[2].equals("QUARANTINED") && l[3].equals("preflight")).count();
        long committed = shards.stream().filter(l -> l[2].equals("COMMITTED")).count();
        assertEquals(shards.size(), preflighted + committed, "every shard either commits or is refused at the preflight");
        assertTrue(preflighted >= 5, "refused shards: " + preflighted);
        assertTrue(committed >= 10, "committed shards: " + committed);
        assertTrue(shards.stream().anyMatch(l -> l[1].equals("synthcov-repo/cov_srcmode_text[1]") && l[3].equals("preflight")),
            "an index created with index.mapping.source.mode: disabled is refused per segment at the preflight");
        assertEquals(List.of("synthcov-repo/cov_srcmode_mapping"), lines("INDEX").stream().map(l -> l[1]).toList(),
            "refused by its mapping before any download, with and without the flag");
    }

    @Test
    void thePreflightOverloadRefusesTheSameShardsWithoutTheFlag() {
        List<String[]> overload = lines("OVERLOAD");
        assertEquals(lines("SHARD").stream().filter(l -> l[3].equals("preflight")).map(l -> l[1]).toList(),
            overload.stream().map(l -> l[1]).toList());
        for (String[] line : overload) assertEquals("same", line[2], line[1]);
    }

    /**
     * Prints one SHARD line per shard exported with and without the flag (name, flag-on outcome, flag-on
     * fetch), one INDEX line per index that failed before its shards, one OVERLOAD line per refused
     * shard, and a MISMATCH line for each difference to the full download.
     */
    public static final class ChildMain {

        record Run(RunReport report, String indexFailure, Map<String, SynthCovFixtures.Row> rows, SynthCovFixtures.RecordingStore store) {}

        public static void main(String[] args) throws Exception {
            Path tmp = Path.of(args[0]);
            if (SelectiveFetch.COMMIT_PREFLIGHT) mismatch("COMMIT_PREFLIGHT is on; 1.2.0 ties the preflight to the flag");
            for (String repository : REPOSITORIES) {
                Path repo = SynthCovFixtures.FIXTURES.resolve(repository);
                var store = new FileSystemBlobStore(repo);
                var inspector = new RepositoryInspector(store);
                for (IndexOverview index : inspector.overview().indices()) {
                    compare(repository, repo, store, inspector, index.name(), tmp.resolve(repository).resolve(index.name()));
                }
            }
        }

        private static void compare(String repository, Path repo, FileSystemBlobStore store, RepositoryInspector inspector, String index,
                                    Path work) throws Exception {
            String name = repository + "/" + index;
            Run off = export(repo, index, work.resolve("off"), false);
            Run on = export(repo, index, work.resolve("on"), true);
            if (off.indexFailure() != null || on.indexFailure() != null) {
                if (!String.valueOf(off.indexFailure()).equals(String.valueOf(on.indexFailure())))
                    mismatch(name + ": index failure " + off.indexFailure() + " != " + on.indexFailure());
                if (readsShardData(on.store()) || readsShardData(off.store())) mismatch(name + ": refused by its mapping after a download");
                System.out.println("INDEX\t" + name);
                return;
            }
            if (!off.rows().equals(on.rows())) mismatch(name + ": committed rows differ with the flag");
            JsonNode metadata = inspector.indexMetadata(index);
            for (RunReport.ShardReport full : off.report().shards()) {
                String shard = name + "[" + full.shardId() + "]";
                RunReport.ShardReport flagged = on.report().shards().stream()
                    .filter(s -> s.shardId() == full.shardId()).findFirst().orElseThrow();
                System.out.println(String.join("\t", "SHARD", shard, flagged.outcome().name(), flagged.fetch()));
                if (!"full".equals(full.fetch())) mismatch(shard + ": flag off fetched " + full.fetch());
                if (full.outcome() != flagged.outcome()) {
                    mismatch(shard + ": " + full.outcome() + " != " + flagged.outcome());
                    continue;
                }
                if (full.outcome() == RunReport.Outcome.COMMITTED) continue;
                if (!full.errorClass().equals(flagged.errorClass()) || !SynthCovFixtures.message(full).equals(SynthCovFixtures.message(flagged)))
                    mismatch(shard + ": " + full.errorClass() + " " + SynthCovFixtures.message(full) + " != "
                        + flagged.errorClass() + " " + SynthCovFixtures.message(flagged));
                ShardSnapshot snapshot = inspector.shardSnapshot(index, full.shardId());
                if (!"preflight".equals(flagged.fetch())) {
                    mismatch(shard + ": refused after fetch=" + flagged.fetch());
                    continue;
                }
                // Every fixture shard is one compound segment, so its commit files are the whole shard;
                // CommitPointPreflightTest shows the commit-files-only download on a multi-segment shard.
                if (flagged.bytesFetched() != commitFileBytes(snapshot) || flagged.bytesFetched() > full.bytesFetched())
                    mismatch(shard + ": preflight fetched " + flagged.bytesFetched() + " bytes, commit files " + commitFileBytes(snapshot)
                        + ", full " + full.bytesFetched());
                System.out.println(String.join("\t", "OVERLOAD", shard, overload(store, snapshot, index, metadata, full,
                    work.resolve("overload" + full.shardId()))));
            }
        }

        /** The {@code preflight=true} overload without the flag: "same" when it refuses as the full download does. */
        private static String overload(FileSystemBlobStore store, ShardSnapshot snapshot, String index, JsonNode metadata,
                                       RunReport.ShardReport full, Path dir) {
            System.clearProperty(SelectiveFetch.PROPERTY);
            var tracker = new ShardOpening.Tracker();
            try {
                ShardOpening.open(new ShardMaterializer(store), new SelectiveShardFixture.Opener(), snapshot, dir, index, metadata,
                    true, tracker).rawReader().close();
                return "opened";
            } catch (Exception refused) {
                boolean same = refused.getClass().getSimpleName().equals(full.errorClass())
                    && SynthCovFixtures.message(full).equals(refused.getMessage())
                    && "preflight".equals(tracker.fetchMode()) && tracker.bytesFetched() == commitFileBytes(snapshot);
                return same ? "same" : refused.getClass().getSimpleName() + " " + tracker.fetchMode() + " " + tracker.bytesFetched();
            }
        }

        private static Run export(Path repository, String index, Path table, boolean selective) throws Exception {
            if (selective) System.setProperty(SelectiveFetch.PROPERTY, "true");
            else System.clearProperty(SelectiveFetch.PROPERTY);
            var store = new SynthCovFixtures.RecordingStore(new FileSystemBlobStore(repository));
            var sources = new RepositorySources(new RepositorySources.Source("primary", store));
            try {
                RunReport report = new FullIndexExporter(sources, new Configuration(), new SelectiveShardFixture.Opener())
                    .run(index, table.toString(), RunReportListener.NO_OP);
                return new Run(report, null, SynthCovFixtures.rows(table), store);
            } catch (IOException failure) {
                return new Run(null, failure.getClass().getSimpleName() + ": " + failure.getMessage(), Map.of(), store);
            } finally {
                System.clearProperty(SelectiveFetch.PROPERTY);
            }
        }

        private static boolean readsShardData(SynthCovFixtures.RecordingStore store) {
            return store.read.stream().anyMatch(key -> key.substring(key.lastIndexOf('/') + 1).startsWith("__"));
        }

        private static long commitFileBytes(ShardSnapshot shard) {
            return shard.files().stream().filter(f -> SelectiveFetch.isCommitFile(f.physicalName()))
                .filter(f -> !f.blobName().startsWith("v__")).mapToLong(RepoFileInfo::length).sum();
        }

        private static void mismatch(String text) {
            System.out.println("MISMATCH\t" + text);
        }
    }
}
