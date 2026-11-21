package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.delta.RunReport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** --all-indices with --parallelism above 1: several indices in flight, one thread reporting. */
@Tag("concurrency")
@Timeout(60)
class IndexConcurrencyTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Processors that block until the test releases their index. */
    private static final class Latches {
        final Map<String, CountDownLatch> started = new ConcurrentHashMap<>();
        final Map<String, CountDownLatch> release = new ConcurrentHashMap<>();

        Latches(List<String> names) {
            for (String name : names) {
                started.put(name, new CountDownLatch(1));
                release.put(name, new CountDownLatch(1));
            }
        }

        void awaitRelease(String name) throws InterruptedException {
            started.get(name).countDown();
            assertTrue(release.get(name).await(30, TimeUnit.SECONDS), name + " never released");
        }

        boolean awaitStart(String name) throws InterruptedException {
            return started.get(name).await(30, TimeUnit.SECONDS);
        }

        boolean hasStarted(String name) {
            return started.get(name).getCount() == 0;
        }
    }

    private static RunReport completed(String name) {
        return new RunReport(name, "t0", "t1", 5, 1, 1, 0, 0, 10, 100, List.of());
    }

    @Test
    void twoIndicesRunAtOnceWhileTheManifestStaysInPlannedOrderWithInFlightIndicesPending(@TempDir Path reports)
            throws Exception {
        var names = List.of("a", "b", "c", "d");
        var latches = new Latches(names);
        var manifestSeenAtStart = new ConcurrentHashMap<String, JsonNode>();
        var summaries = Collections.synchronizedList(new ArrayList<String>());
        var threads = Collections.synchronizedList(new ArrayList<String>());
        DumpToDeltaCommand.IndexProcessor processor = name -> {
            threads.add(Thread.currentThread().getName());
            manifestSeenAtStart.put(name, JSON.readTree(reports.resolve("manifest.json").toFile()));
            latches.awaitRelease(name);
            return completed(name);
        };
        var result = new Object[1];
        Thread main = new Thread(() -> {
            try {
                result[0] = new DumpToDeltaCommand().runAllIndices(names, processor, reports, 2,
                    (name, report) -> summaries.add(name + ":" + Thread.currentThread().getName()));
            } catch (Throwable e) {
                result[0] = e;
            }
        }, "reporting-thread");
        main.start();

        assertTrue(latches.awaitStart("a"));
        assertTrue(latches.awaitStart("b"), "both indices in flight at once");
        Thread.sleep(100);
        assertFalse(latches.hasStarted("c"), "never more than two indices in flight");

        latches.release.get("b").countDown();
        assertTrue(latches.awaitStart("c"));
        var afterB = manifestSeenAtStart.get("c");
        assertEquals(List.of("b"), indexNames(afterB));
        assertEquals(List.of("a", "c", "d"), pending(afterB), "a is still in flight, so still pending");
        assertFalse(afterB.path("complete").asBoolean());

        latches.release.get("a").countDown();
        assertTrue(latches.awaitStart("d"));
        var afterA = manifestSeenAtStart.get("d");
        assertEquals(List.of("a", "b"), indexNames(afterA), "planned order, not completion order");
        assertEquals(List.of("c", "d"), pending(afterA));

        latches.release.get("d").countDown();
        latches.release.get("c").countDown();
        main.join(30_000);
        assertEquals(0, result[0]);
        var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        assertEquals(List.of("a", "b", "c", "d"), indexNames(manifest));
        assertEquals(List.of(), pending(manifest));
        assertTrue(manifest.path("complete").asBoolean());
        assertEquals(4, manifest.path("completedCount").asInt());
        assertEquals(4, summaries.size());
        assertTrue(summaries.stream().allMatch(s -> s.endsWith(":reporting-thread")),
            "summaries are printed by the reporting thread only: " + summaries);
        assertTrue(threads.stream().allMatch(t -> t.startsWith("gefrierschrank-index-")), threads.toString());
    }

    @Test
    void indexThreadsCarryTheCallersContextClassLoader() throws Exception {
        var marker = new URLClassLoader(new URL[0], getClass().getClassLoader());
        var observed = Collections.synchronizedList(new ArrayList<Object[]>());
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(marker);
        try {
            new DumpToDeltaCommand().runAllIndices(List.of("a", "b", "c"), name -> {
                observed.add(new Object[]{Thread.currentThread().getContextClassLoader(), Thread.currentThread().isDaemon()});
                return completed(name);
            }, null, 3, (name, report) -> {});
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
        assertEquals(3, observed.size());
        for (Object[] thread : observed) {
            assertSame(marker, thread[0]);
            assertEquals(true, thread[1]);
        }
    }

    @Test
    void oneIndexFailingOutrightIsFailedWhileTheOthersComplete(@TempDir Path reports) throws Exception {
        var processed = Collections.synchronizedList(new ArrayList<String>());
        int exit = new DumpToDeltaCommand().runAllIndices(List.of("a", "b", "c", "d"), name -> {
            processed.add(name);
            if (name.equals("b")) throw new IOException("shard catalog unreachable");
            return completed(name);
        }, reports, 3, (name, report) -> {});

        assertEquals(4, exit);
        assertEquals(List.of("a", "b", "c", "d"), processed.stream().sorted().toList());
        var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        assertEquals(List.of("a", "b", "c", "d"), indexNames(manifest));
        assertEquals("FAILED", manifest.path("indices").path(1).path("outcome").asText());
        assertEquals("IOException", manifest.path("indices").path(1).path("errorClass").asText());
        assertFalse(manifest.toString().contains("shard catalog unreachable"), "no raw exception message");
        assertEquals(3, manifest.path("completedCount").asInt());
        assertEquals(1, manifest.path("failedCount").asInt());
    }

    @Test
    void anErrorFromAnIndexPropagatesWithoutWaitingForTheOthers(@TempDir Path reports) throws Exception {
        var crash = new AssertionError("simulated VM-level failure");
        var releaseA = new CountDownLatch(1);
        try {
            var thrown = assertThrows(AssertionError.class, () -> new DumpToDeltaCommand().runAllIndices(
                List.of("a", "b", "c"), name -> {
                    if (name.equals("b")) throw crash;
                    if (name.equals("a")) releaseA.await();
                    return completed(name);
                }, reports, 2, (name, report) -> {}));
            assertSame(crash, thrown);
        } finally {
            releaseA.countDown();
        }
        var manifest = JSON.readTree(reports.resolve("manifest.json").toFile());
        assertEquals(List.of(), indexNames(manifest));
        assertEquals(List.of("a", "b", "c"), pending(manifest), "nothing recorded as done after the crash");
        assertFalse(manifest.path("complete").asBoolean());
    }

    @Test
    void theSequentialSeamIsUnchangedAtConcurrencyOne(@TempDir Path reports) throws Exception {
        var order = Collections.synchronizedList(new ArrayList<String>());
        int exit = new DumpToDeltaCommand().runAllIndices(List.of("a", "b", "c"), name -> {
            order.add("process " + name + " on " + Thread.currentThread().getName());
            if (name.equals("b")) throw new IOException("boom");
            return completed(name);
        }, reports, 1, (name, report) -> order.add("summary " + name));
        String caller = Thread.currentThread().getName();
        assertEquals(List.of("process a on " + caller, "summary a", "process b on " + caller,
            "process c on " + caller, "summary c"), order);
        assertEquals(4, exit);
    }

    static List<String> indexNames(JsonNode manifest) {
        var names = new ArrayList<String>();
        manifest.path("indices").forEach(entry -> names.add(entry.path("indexName").asText()));
        return names;
    }

    static List<String> pending(JsonNode manifest) {
        var names = new ArrayList<String>();
        manifest.path("pendingIndices").forEach(name -> names.add(name.asText()));
        return names;
    }
}
