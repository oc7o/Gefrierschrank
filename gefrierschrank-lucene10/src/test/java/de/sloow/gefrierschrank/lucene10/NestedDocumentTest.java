package de.sloow.gefrierschrank.lucene10;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.util.BytesRef;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class NestedDocumentTest {
    @Test
    void readsOnlyLiveRootsAndKeepsTheirNestedSource(@TempDir Path tmp) throws Exception {
        String source = "{\"children\":[{\"value\":1}]}";
        try (var dir = new NIOFSDirectory(tmp)) {
            try (var writer = new IndexWriter(dir, new IndexWriterConfig())) {
                var child = new Document();
                child.add(new StringField("_nested_path", "children", Field.Store.NO));
                var root = new Document();
                root.add(new StoredField("_id", new BytesRef(new byte[]{(byte)254, 0x12, 0x3f})));
                root.add(new StoredField("_routing", "tenant"));
                root.add(new StoredField("_source", new BytesRef(source)));
                writer.addDocuments(List.of(child, root));
            }
        }
        try (var reader = new Lucene10ShardReader(tmp)) {
            assertFalse(reader.isLive(0));
            assertTrue(reader.isLive(1));
            assertEquals("123", reader.documentMetadata(1).id());
            assertEquals("tenant", reader.documentMetadata(1).routing());
            reader.readSource(1, (bytes, length) -> assertEquals(source,
                new String(bytes, 0, length, java.nio.charset.StandardCharsets.UTF_8)));
        }
    }
}
