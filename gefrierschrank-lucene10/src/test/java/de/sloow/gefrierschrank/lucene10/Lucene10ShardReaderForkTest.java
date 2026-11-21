package de.sloow.gefrierschrank.lucene10;

import de.sloow.gefrierschrank.core.lucene.ShardReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static de.sloow.gefrierschrank.lucene10.Lucene10ShardReaderEquivalenceTest.Outcome;
import static de.sloow.gefrierschrank.lucene10.Lucene10ShardReaderEquivalenceTest.metadata;
import static de.sloow.gefrierschrank.lucene10.Lucene10ShardReaderEquivalenceTest.source;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Forks: a fork reads exactly what its parent reads, closing a fork releases nothing shared,
 * and forks read one segment concurrently without disturbing each other, over LZ4, DEFLATE and
 * both ES ZSTD presets (blocks of up to 2048 documents) and over ES 7 (Lucene 8) shards.
 */
class Lucene10ShardReaderForkTest {

    /** Every document's source and metadata, in production order, as the given reader returns them. */
    private static List<Outcome> readAll(Lucene10ShardReader reader, int from, int to) throws Exception {
        var outcomes = new ArrayList<Outcome>();
        for (int doc = from; doc < to; doc++) {
            if (!reader.isLive(doc)) continue;
            outcomes.add(source(reader, doc));
            outcomes.add(metadata(reader, doc));
        }
        return outcomes;
    }

    private static void assertForksBehave(Path shardDir) throws Exception {
        try (var parent = new Lucene10ShardReader(shardDir)) {
            int maxDoc = parent.docCount();
            List<Outcome> serial = readAll(parent, 0, maxDoc);

            ShardReader forked = parent.fork();
            var fork = assertInstanceOf(Lucene10ShardReader.class, forked);
            assertEquals(maxDoc, fork.docCount());
            assertEquals("lucene10", fork.runtimeName());
            for (int doc = 0; doc < maxDoc; doc++) {
                assertEquals(parent.isLive(doc), fork.isLive(doc), "liveness of doc " + doc);
                assertEquals(parent.storedSourceAbsent(doc), fork.storedSourceAbsent(doc), "source field of doc " + doc);
            }
            assertEquals(serial, readAll(fork, 0, maxDoc), "a fork reads what its parent reads");
            fork.close();
            fork.close();

            assertEquals(serial, readAll(parent, 0, maxDoc), "closing a fork leaves the parent readable");
            try (var another = (Lucene10ShardReader) parent.fork()) {
                assertEquals(serial, readAll(another, 0, maxDoc), "the parent still forks");
            }

            // Four threads, each on its own fork, each starting mid-shard, all at once.
            int threads = 4;
            var start = new CyclicBarrier(threads);
            var pool = Executors.newFixedThreadPool(threads);
            try {
                var forks = new ArrayList<Lucene10ShardReader>();
                for (int t = 0; t < threads; t++) forks.add((Lucene10ShardReader) parent.fork());
                var results = new ArrayList<Future<List<Outcome>>>();
                for (int t = 0; t < threads; t++) {
                    var reader = forks.get(t);
                    int from = maxDoc * t / threads;
                    Callable<List<Outcome>> body = () -> {
                        start.await(30, TimeUnit.SECONDS);
                        var outcomes = new ArrayList<>(readAll(reader, from, maxDoc));
                        outcomes.addAll(readAll(reader, 0, from));
                        return outcomes;
                    };
                    results.add(pool.submit(body));
                }
                for (int t = 0; t < threads; t++) {
                    int from = maxDoc * t / threads;
                    var expected = new ArrayList<>(readAll(parent, from, maxDoc));
                    expected.addAll(readAll(parent, 0, from));
                    assertEquals(expected, results.get(t).get(60, TimeUnit.SECONDS), "concurrent fork " + t);
                }
                for (var reader : forks) reader.close();
            } finally {
                pool.shutdownNow();
            }
            assertEquals(serial, readAll(parent, 0, maxDoc));
        }
    }

    @ParameterizedTest
    @EnumSource(Lucene10ShardReaderEquivalenceTest.StoredCodec.class)
    void forksOfGeneratedShards(Lucene10ShardReaderEquivalenceTest.StoredCodec codec, @TempDir Path tmp) throws Exception {
        Lucene10ShardReaderEquivalenceTest.buildShard(tmp, codec.codec, 99);
        assertForksBehave(tmp);
    }

    static List<Lucene10ShardReaderEquivalenceTest.FixtureShard> es7Shards() throws Exception {
        return Lucene10ShardReaderEquivalenceTest.fixtureShards(major -> major == 8).stream()
            .filter(shard -> !shard.index().equals("es7_completion")).toList();
    }

    @Test
    void es7FixturesAreCovered() throws Exception {
        var labels = es7Shards().stream().map(Lucene10ShardReaderEquivalenceTest.FixtureShard::label).toList();
        for (String expected : List.of("es7-best-compression-repo/es7_best_compression", "es7-features-repo/es7_hard_deletes",
                "demo-repo/test_data_7_17_9"))
            assertTrue(labels.stream().anyMatch(label -> label.startsWith(expected)), expected + " missing from " + labels);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("es7Shards")
    void forksOfEs7Shards(Lucene10ShardReaderEquivalenceTest.FixtureShard fixture, @TempDir Path tmp) throws Exception {
        assertForksBehave(Lucene10ShardReaderEquivalenceTest.materialize(fixture, tmp.resolve("shard")));
    }

    @Test
    void closingTheParentClosesTheSharedReader(@TempDir Path tmp) throws Exception {
        Lucene10ShardReaderEquivalenceTest.buildShard(tmp, Lucene10ShardReaderEquivalenceTest.StoredCodec.LZ4.codec, 5);
        var parent = new Lucene10ShardReader(tmp);
        var fork = parent.fork();
        fork.close();
        parent.close();
        assertThrows(org.apache.lucene.store.AlreadyClosedException.class, () -> parent.fork().readSource(0, (b, l) -> {}));
    }
}
