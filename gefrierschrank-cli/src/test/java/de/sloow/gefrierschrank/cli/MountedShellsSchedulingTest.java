package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.RepositorySources.Identity;
import de.sloow.gefrierschrank.delta.RunReport;
import de.sloow.gefrierschrank.delta.TargetStorageException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * --mounted-shells skip-if-original through the --all-indices seam: a held shell starts only after
 * its original's entry is recorded, and then becomes ALIAS_OF or is exported.
 */
@Tag("concurrency")
@Timeout(60)
class MountedShellsSchedulingTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Identity X = new Identity("primary", "id-x", "x", "snap-1", 1);
    // The ilm-shell-repo order: the shell is listed before its original.
    private static final List<String> NAMES = List.of("partial-x", "x", "a", "b");

    private static MountedShellPlan shells() {
        return MountedShellPlan.of(List.of(new MountedShellPlan.Planned("partial-x", X, true, false),
            new MountedShellPlan.Planned("x", X, false, false)));
    }

    private static RunReport completed(String name) {
        return new RunReport(name, "t0", "t1", 5, 1, 1, 0, 0, 10, 100, List.of());
    }

    private static RunReport quarantined(String name) {
        return new RunReport(name, "t0", "t1", 5, 1, 0, 0, 1, 0, 0, List.of());
    }

    /** What each name does when processed; records the order and the manifest each one saw at its start. */
    private static final class Processor implements DumpToDeltaCommand.IndexProcessor {
        interface Body {
            DumpToDeltaCommand.IndexOutcome run(String name) throws Exception;
        }

        final List<String> processed = Collections.synchronizedList(new ArrayList<>());
        final Map<String, JsonNode> manifestAtStart = new ConcurrentHashMap<>();
        private final Path reports;
        private final Body body;

        Processor(Path reports, Body body) {
            this.reports = reports;
            this.body = body;
        }

        @Override
        public RunReport process(String name) {
            throw new AssertionError("the run must ask for the identity");
        }

        @Override
        public DumpToDeltaCommand.IndexOutcome processWithIdentity(String name) throws Exception {
            processed.add(name);
            manifestAtStart.put(name, JSON.readTree(reports.resolve("manifest.json").toFile()));
            return body.run(name);
        }
    }

    private static DumpToDeltaCommand.IndexOutcome exported(String name) {
        return new DumpToDeltaCommand.IndexOutcome(completed(name), name.equals("x") ? X : null);
    }

    private static int run(Processor processor, Path reports, int concurrency) throws InterruptedException {
        return new DumpToDeltaCommand().runAllIndices(NAMES, processor, reports, concurrency, (name, report) -> {}, shells());
    }

    private static JsonNode manifest(Path reports) throws IOException {
        return JSON.readTree(reports.resolve("manifest.json").toFile());
    }

    private static JsonNode entry(JsonNode manifest, String name) {
        for (var entry : manifest.path("indices")) if (entry.path("indexName").asText().equals(name)) return entry;
        return null;
    }

    @ParameterizedTest(name = "concurrency {0}")
    @ValueSource(ints = {1, 2, 3})
    void aShellIsHeldUntilItsOriginalCompletesAndThenRecordedAsItsAlias(int concurrency, @TempDir Path reports)
            throws Exception {
        var processor = new Processor(reports, MountedShellsSchedulingTest::exported);
        var out = new ByteArrayOutputStream();
        int exit = captured(out, () -> run(processor, reports, concurrency));

        assertEquals(0, exit);
        assertFalse(processor.processed.contains("partial-x"), "an alias is never processed");
        assertEquals(List.of("a", "b", "x"), processor.processed.stream().sorted().toList());
        if (concurrency == 1) assertEquals(List.of("x", "a", "b"), processor.processed, "listing order without the shell");
        assertTrue(IndexConcurrencyTest.pending(processor.manifestAtStart.get("x")).contains("partial-x"));

        var manifest = manifest(reports);
        assertEquals(NAMES, IndexConcurrencyTest.indexNames(manifest), "listing order");
        var alias = entry(manifest, "partial-x");
        assertEquals("ALIAS_OF", alias.path("outcome").asText());
        assertEquals("x", alias.path("aliasOf").asText());
        assertTrue(alias.path("reportFile").isNull());
        assertTrue(alias.path("snapshotUuid").isNull());
        assertFalse(entry(manifest, "x").has("aliasOf"), "aliasOf only on alias entries");
        assertEquals(1, manifest.path("aliasCount").asInt());
        assertEquals(4, manifest.path("totalIndices").asInt());
        assertEquals(3, manifest.path("completedCount").asInt());
        assertTrue(manifest.path("complete").asBoolean());
        assertEquals(List.of(), IndexConcurrencyTest.pending(manifest));
        assertTrue(out.toString().contains("Index partial-x: alias of x (same backing index, snapshot and shard count); "
            + "no table written"), out.toString());
        assertTrue(out.toString().contains("All-indices run: 4 indices - 3 completed, 0 completed with quarantined shards, "
            + "0 failed outright, 1 aliases of exported tables"), out.toString());
    }

    @Test
    void anAliasHeldBeforeItsOriginalNeverWaitsForOtherIndices(@TempDir Path reports) throws Exception {
        var release = new java.util.concurrent.CountDownLatch(1);
        var processor = new Processor(reports, name -> {
            if (name.equals("a")) assertTrue(release.await(30, java.util.concurrent.TimeUnit.SECONDS));
            if (name.equals("b")) {
                assertEquals("ALIAS_OF", entry(manifest(reports), "partial-x").path("outcome").asText(),
                    "decided as soon as x is recorded, while a is still in flight");
                release.countDown();
            }
            return exported(name);
        });
        assertEquals(0, run(processor, reports, 2));
    }

    @ParameterizedTest(name = "concurrency {0}")
    @ValueSource(ints = {1, 2})
    void aQuarantinedOriginalStillAliasesItsShellAndTheRunExitsThree(int concurrency, @TempDir Path reports)
            throws Exception {
        var processor = new Processor(reports, name -> name.equals("x")
            ? new DumpToDeltaCommand.IndexOutcome(quarantined(name), X) : exported(name));
        assertEquals(3, run(processor, reports, concurrency));
        var manifest = manifest(reports);
        assertEquals("COMPLETED_WITH_QUARANTINE", entry(manifest, "x").path("outcome").asText());
        assertEquals("ALIAS_OF", entry(manifest, "partial-x").path("outcome").asText());
        assertFalse(manifest.path("complete").asBoolean());
        assertFalse(processor.processed.contains("partial-x"));
    }

    @ParameterizedTest(name = "concurrency {0}")
    @ValueSource(ints = {1, 2})
    void aFailedOriginalExportsItsShellAndTheRunExitsFour(int concurrency, @TempDir Path reports) throws Exception {
        var processor = new Processor(reports, name -> {
            if (name.equals("x")) throw new IOException("table locked");
            return exported(name);
        });
        assertEquals(4, run(processor, reports, concurrency));
        assertTrue(processor.processed.contains("partial-x"));
        var manifest = manifest(reports);
        assertEquals("FAILED", entry(manifest, "x").path("outcome").asText());
        assertEquals("COMPLETED", entry(manifest, "partial-x").path("outcome").asText());
        assertEquals(0, manifest.path("aliasCount").asInt());
        assertEquals(NAMES, IndexConcurrencyTest.indexNames(manifest));
        if (concurrency == 1) assertEquals(List.of("x", "partial-x", "a", "b"), processor.processed,
            "the shell starts right after its original's result");
    }

    @ParameterizedTest(name = "concurrency {0}")
    @ValueSource(ints = {1, 2})
    void anOriginalThatExportedAnotherIdentityExportsItsShell(int concurrency, @TempDir Path reports) throws Exception {
        var drifted = new Identity("primary", "id-x", "x", "snap-2", 1);
        var processor = new Processor(reports, name -> name.equals("x")
            ? new DumpToDeltaCommand.IndexOutcome(completed(name), drifted) : exported(name));
        assertEquals(0, run(processor, reports, concurrency));
        assertTrue(processor.processed.contains("partial-x"));
        var manifest = manifest(reports);
        assertEquals("COMPLETED", entry(manifest, "partial-x").path("outcome").asText());
        assertTrue(manifest.path("complete").asBoolean());
    }

    @ParameterizedTest(name = "concurrency {0}")
    @ValueSource(ints = {1, 3})
    void aTargetStorageFailureOfTheOriginalLeavesItsShellPendingAndStartsNothing(int concurrency, @TempDir Path reports)
            throws Exception {
        var processor = new Processor(reports, name -> {
            if (name.equals("x")) throw new TargetStorageException("no space left: FSError", new IOException("disk"));
            if (concurrency > 1) awaitFailedInManifest(reports, "x");
            return exported(name);
        });
        var err = new ByteArrayOutputStream();
        int exit = capturedErr(err, () -> run(processor, reports, concurrency));
        assertEquals(1, exit);
        assertFalse(processor.processed.contains("partial-x"));
        var manifest = manifest(reports);
        assertTrue(IndexConcurrencyTest.pending(manifest).contains("partial-x"));
        assertEquals("FAILED", entry(manifest, "x").path("outcome").asText());
        if (concurrency == 1) {
            assertEquals(List.of("x"), processor.processed, "nothing starts after the failure");
            assertEquals(List.of("partial-x", "a", "b"), IndexConcurrencyTest.pending(manifest));
        }
    }

    @Test
    void anErrorInTheOriginalIsRethrownWithItsShellPending(@TempDir Path reports) throws Exception {
        var crash = new AssertionError("simulated VM-level failure");
        for (int concurrency : List.of(1, 2)) {
            var processor = new Processor(reports, name -> {
                if (name.equals("x")) throw crash;
                return exported(name);
            });
            assertSame(crash, assertThrows(AssertionError.class, () -> run(processor, reports, concurrency)));
            assertFalse(processor.processed.contains("partial-x"));
            assertTrue(IndexConcurrencyTest.pending(manifest(reports)).contains("partial-x"));
        }
    }

    @Test
    void theDefaultPlanKeepsTheManifestFreeOfAliasKeys(@TempDir Path reports) throws Exception {
        int exit = new DumpToDeltaCommand().runAllIndices(NAMES, MountedShellsSchedulingTest::completed, reports, 2,
            (name, report) -> {}, MountedShellPlan.NONE);
        assertEquals(0, exit);
        var manifest = manifest(reports);
        assertFalse(manifest.has("aliasCount"));
        manifest.path("indices").forEach(entry -> assertFalse(entry.has("aliasOf")));
        assertEquals(NAMES, IndexConcurrencyTest.indexNames(manifest));
    }

    private static void awaitFailedInManifest(Path reports, String name) throws Exception {
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (System.nanoTime() < deadline) {
            var entry = entry(manifest(reports), name);
            if (entry != null && entry.path("outcome").asText().equals("FAILED")) return;
            Thread.sleep(10);
        }
        fail(name + " never recorded as FAILED");
    }

    private interface Body {
        int call() throws Exception;
    }

    private static int captured(ByteArrayOutputStream out, Body body) throws Exception {
        PrintStream previous = System.out;
        System.setOut(new PrintStream(out, true));
        try {
            return body.call();
        } finally {
            System.setOut(previous);
        }
    }

    private static int capturedErr(ByteArrayOutputStream err, Body body) throws Exception {
        PrintStream previous = System.err;
        System.setErr(new PrintStream(err, true));
        try {
            return body.call();
        } finally {
            System.setErr(previous);
        }
    }
}
