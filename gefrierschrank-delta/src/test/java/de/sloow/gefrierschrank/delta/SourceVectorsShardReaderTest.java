package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.syntheticsource.IsolatedSourceVectorsSession;
import de.sloow.gefrierschrank.syntheticsource.SourceVectorsMismatchException;
import de.sloow.gefrierschrank.syntheticsource.VectorRehydrationException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SourceVectorsShardReaderTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    /** doc 0: no vectors; doc 1: root vector; doc 2: nested vector; doc 3: root vector colliding with a stored key. */
    private static final String[] STORED = {"{\"t\":\"a\"}", "{\"t\":\"b\"}", "{\"t\":\"c\",\"o\":{}}", "{\"v\":null}"};

    @Test
    void indicesWithoutRehydrationGetTheDelegateItself() throws Exception {
        var delegate = new FakeReader();
        JsonNode metadata = JSON.readTree("{}");
        assertSame(delegate, SourceVectorsShardReader.wrap(delegate, Path.of("x"), "idx", metadata, SourceVectorsPlan.none()));
        assertSame(delegate, SourceVectorsShardReader.wrap(delegate, Path.of("x"), "idx", null, SourceVectorsPlan.none()));
        assertSame(delegate, SourceVectorsShardReader.wrap(delegate, Path.of("x"), "idx", metadata, null));
    }

    @Test
    void eachDocumentTakesItsPathAndKind() throws Exception {
        var delegate = new FakeReader();
        var session = new FakeSession(4);
        try (ShardReader reader = SourceVectorsShardReader.wrap(delegate, session)) {
            byte[][] seen = new byte[1][];
            reader.readSource(0, (bytes, length) -> seen[0] = bytes);
            assertSame(delegate.handedOut.get(0), seen[0], "no vectors: the delegate's very array");
            assertEquals("stored", reader.documentMetadata(0).sourceKind());

            assertEquals("{\"t\":\"b\",\"v\":[1.5]}", source(reader, 1));
            assertEquals("stored+vectors", reader.documentMetadata(1).sourceKind());

            assertEquals("ES{\"t\":\"c\",\"o\":{}}", source(reader, 2));
            assertEquals("stored+vectors-reserialized", reader.documentMetadata(2).sourceKind());

            assertEquals("ES{\"v\":null}", source(reader, 3), "a stored key collision goes to Elasticsearch");
            assertEquals("stored+vectors-reserialized", reader.documentMetadata(3).sourceKind());
            assertEquals("stored", reader.documentMetadata(1).sourceKind(), "another doc keeps the delegate's kind");
        }
    }

    @Test
    void theSuffixFollowsTheDelegatesBaseKind() throws Exception {
        var delegate = new FakeReader();
        delegate.kind = "stored-smile";
        try (ShardReader reader = SourceVectorsShardReader.wrap(delegate, new FakeSession(4))) {
            source(reader, 1);
            assertEquals("stored-smile+vectors", reader.documentMetadata(1).sourceKind());
            source(reader, 2);
            assertEquals("stored-smile+vectors-reserialized", reader.documentMetadata(2).sourceKind());
        }
    }

    @Test
    void forksGetTheirOwnCursorOnTheirOwnThreadAndOnlyTheRootClosesTheSession() throws Exception {
        var delegate = new FakeReader();
        var session = new FakeSession(4);
        ShardReader root = SourceVectorsShardReader.wrap(delegate, session);
        ShardReader fork = root.fork();
        assertNotSame(root, fork);
        var thread = Executors.newSingleThreadExecutor();
        try {
            String forkThread = thread.submit(() -> {
                source(fork, 1);
                return Thread.currentThread().getName();
            }).get();
            source(root, 1);
            assertEquals(List.of(forkThread, Thread.currentThread().getName()), session.cursorThreads);
        } finally {
            thread.shutdownNow();
        }
        fork.close();
        assertFalse(session.closed);
        assertEquals(1, delegate.closedForks);
        root.close();
        assertTrue(session.closed);
        assertTrue(delegate.closed);
    }

    @Test
    void aCursorFailurePropagatesWithoutTheReconstructionPhrase() throws Exception {
        var session = new FakeSession(4);
        session.failOn = 1;
        try (ShardReader reader = SourceVectorsShardReader.wrap(new FakeReader(), session)) {
            var failure = assertThrows(VectorRehydrationException.class, () -> source(reader, 1));
            assertFalse(failure.getMessage().contains("synthetic source"));
        }
    }

    @Test
    void aDocCountMismatchClosesTheSessionAndKeepsTheDelegate() {
        var delegate = new FakeReader();
        var session = new FakeSession(5);
        assertThrows(SourceVectorsMismatchException.class, () -> SourceVectorsShardReader.wrap(delegate, session));
        assertTrue(session.closed);
        assertFalse(delegate.closed);
    }

    @Test
    void anExcludedButNotRehydratedPlanIsRefused() throws Exception {
        JsonNode rankVectors = JSON.readTree("{\"settings\":{\"index.version.created\":\"9060000\"},"
            + "\"mappings\":{\"properties\":{\"v\":{\"type\":\"rank_vectors\"}}}}");
        var delegate = new FakeReader();
        assertThrows(SourceVectorsPlan.ExcludedSourceVectorsException.class, () -> SourceVectorsShardReader.wrap(
            delegate, Path.of("x"), "idx", rankVectors, SourceVectorsPlan.forIndex("idx", rankVectors)));
        assertFalse(delegate.closed, "the caller keeps the delegate");
    }

    @Test
    void theFallbackWrapClosesTheDelegateWhenTheVectorLayerFails() throws Exception {
        JsonNode rankVectors = JSON.readTree("{\"settings\":{\"index.version.created\":\"9060000\"},"
            + "\"mappings\":{\"properties\":{\"v\":{\"type\":\"rank_vectors\"}}}}");
        var delegate = new FakeReader();
        assertThrows(SourceVectorsPlan.ExcludedSourceVectorsException.class,
            () -> SyntheticSourceFallbackShardReader.wrap(delegate, Path.of("x"), "idx", rankVectors));
        assertTrue(delegate.closed);
    }

    private static String source(ShardReader reader, int doc) throws IOException {
        String[] out = new String[1];
        reader.readSource(doc, (bytes, length) -> out[0] = new String(bytes, 0, length, StandardCharsets.UTF_8));
        return out[0];
    }

    private static final class FakeReader implements ShardReader {
        final List<byte[]> handedOut = new ArrayList<>();
        String kind = "stored";
        boolean closed;
        int closedForks;
        private final FakeReader parent;

        FakeReader() {
            this(null);
        }

        private FakeReader(FakeReader parent) {
            this.parent = parent;
        }

        @Override public String runtimeName() { return "fake"; }
        @Override public int docCount() { return STORED.length; }

        @Override
        public ShardReader fork() {
            return new FakeReader(this);
        }

        @Override
        public DocumentMetadata documentMetadata(int docId) {
            return new DocumentMetadata("id" + docId, null, parent == null ? kind : parent.kind, docId);
        }

        @Override
        public void readSource(int docId, SourceConsumer consumer) {
            byte[] bytes = STORED[docId].getBytes(StandardCharsets.UTF_8);
            handedOut.add(bytes);
            consumer.accept(bytes, bytes.length);
        }

        @Override
        public void close() {
            closed = true;
            if (parent != null) parent.closedForks++;
        }
    }

    private static final class FakeSession implements IsolatedSourceVectorsSession {
        final List<String> cursorThreads = new CopyOnWriteArrayList<>();
        final int docCount;
        volatile boolean closed;
        int failOn = -1;

        FakeSession(int docCount) {
            this.docCount = docCount;
        }

        @Override public int docCount() { return docCount; }

        @Override
        public Cursor newCursor() {
            cursorThreads.add(Thread.currentThread().getName());
            return new Cursor() {
                private int doc;

                @Override
                public int load(int globalDocId) throws IOException {
                    if (globalDocId == failOn) throw new VectorRehydrationException(new IllegalStateException("doc text"));
                    doc = globalDocId;
                    return doc == 0 ? 0 : 1;
                }

                @Override
                public boolean rootLeavesOnly() {
                    return doc != 2;
                }

                @Override
                public String[] rootKeys() {
                    return new String[] {"v"};
                }

                @Override
                public byte[] rootMembers() {
                    return "\"v\":[1.5]".getBytes(StandardCharsets.UTF_8);
                }

                @Override
                public byte[] reserialize(byte[] stored, int length) {
                    return ("ES" + new String(stored, 0, length, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
                }
            };
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
