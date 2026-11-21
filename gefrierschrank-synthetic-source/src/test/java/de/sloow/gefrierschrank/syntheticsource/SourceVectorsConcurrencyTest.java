package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Indices of one run are checked for excluded vectors concurrently once several indices export at a
 * time, so {@link SourceVectors#probe} must give every caller the answer a lone caller gets,
 * including when the calls race to create and initialize the shared isolated loader.
 */
class SourceVectorsConcurrencyTest {

    private static final int THREADS = 4;
    private static final int ROUNDS = 3;

    @Test
    @Timeout(300)
    void concurrentProbesOnAFreshSharedLoaderAnswerLikeSerialCalls() throws Exception {
        List<JsonNode> cases = cases();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            for (int round = 0; round < ROUNDS; round++) {
                // Every round's first probe creates the shared loader and initializes ES inside it.
                IsolatedEsClassLoader.discard(IsolatedEsClassLoader.shared());
                var start = new CyclicBarrier(THREADS);
                List<Future<List<SourceVectors.Result>>> concurrent = new ArrayList<>();
                for (int thread = 0; thread < THREADS; thread++) {
                    List<Integer> order = new ArrayList<>();
                    for (int i = 0; i < cases.size(); i++) order.add(i);
                    Collections.rotate(order, thread * 3);
                    concurrent.add(pool.submit(() -> {
                        start.await(60, TimeUnit.SECONDS);
                        SourceVectors.Result[] results = new SourceVectors.Result[cases.size()];
                        for (int i : order) results[i] = SourceVectors.probe("idx-" + i, cases.get(i));
                        return List.of(results);
                    }));
                }
                List<List<SourceVectors.Result>> answers = new ArrayList<>();
                for (var future : concurrent) answers.add(future.get(240, TimeUnit.SECONDS));
                IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();

                List<SourceVectors.Result> serial = new ArrayList<>();
                for (int i = 0; i < cases.size(); i++) serial.add(SourceVectors.probe("idx-" + i, cases.get(i)));
                assertEquals(java.util.EnumSet.allOf(SourceVectors.Verdict.class),
                    java.util.EnumSet.copyOf(serial.stream().map(SourceVectors.Result::verdict).toList()), serial.toString());
                for (int thread = 0; thread < THREADS; thread++) {
                    assertEquals(serial, answers.get(thread), "round " + round + " thread " + thread);
                }
                assertSame(loader, IsolatedEsClassLoader.shared(), "the racing first uses left one shared loader");
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /** Every verdict: excluded, none by version, setting, synthetic source or field parameters, unsupported. */
    private static List<JsonNode> cases() throws Exception {
        String dense = "{\"properties\":{\"title\":{\"type\":\"keyword\"},"
            + "\"embedding\":{\"type\":\"dense_vector\",\"dims\":3,\"similarity\":\"cosine\"}}}";
        String nested = "{\"properties\":{\"meta\":{\"properties\":{\"embedding\":{\"type\":\"dense_vector\",\"dims\":3}}},"
            + "\"chunks\":{\"type\":\"nested\",\"properties\":{\"vector\":{\"type\":\"dense_vector\",\"dims\":3}}}}}";
        return List.of(
            SourceVectorsTest.metadata("9060000", Map.of(), dense),
            SourceVectorsTest.metadata("8521000", Map.of(), dense),
            SourceVectorsTest.metadata("9060000", Map.of("index.mapping.exclude_source_vectors", "false"), dense),
            SourceVectorsTest.metadata("9060000", Map.of("index.mapping.source.mode", "synthetic"), dense),
            SourceVectorsTest.metadata("9060000", Map.of(), nested),
            SourceVectorsTest.metadata("9060000", Map.of(),
                "{\"properties\":{\"tokens\":{\"type\":\"sparse_vector\"}}}"),
            SourceVectorsTest.metadata("9060000", Map.of(),
                "{\"properties\":{\"embedding\":{\"type\":\"dense_vector\",\"dims\":3,\"index\":false}}}"),
            SourceVectorsTest.metadata("9060000", Map.of(),
                "{\"properties\":{\"embedding\":{\"type\":\"dense_vector\",\"dims\":-3}}}"),
            SourceVectorsTest.metadata("9060000", Map.of(),
                "{\"properties\":{\"field\":{\"type\":\"rank_vectors\"},\"embedding\":{\"type\":\"dense_vector\",\"dims\":3}}}"),
            SourceVectorsTest.metadata("9060000", Map.of(), "{\"properties\":{\"title\":{\"type\":\"keyword\"}}}"));
    }
}
