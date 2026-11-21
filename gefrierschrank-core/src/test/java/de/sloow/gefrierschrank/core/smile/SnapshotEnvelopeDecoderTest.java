package de.sloow.gefrierschrank.core.smile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Decoding of ChecksumBlobStoreFormat envelopes against real ES 7/8/9 fixture bytes.
 */
class SnapshotEnvelopeDecoderTest {

    private static final Path FIXTURES = Path.of("src/test/resources/fixtures/demo-repo");

    private byte[] read(String relPath) throws IOException {
        try (InputStream in = Files.newInputStream(FIXTURES.resolve(relPath))) {
            return in.readAllBytes();
        }
    }

    @Test
    void decodesRootSnapshotEnvelope() throws IOException {
        byte[] raw = read("snap-R78Q0cvSRYSueg5T5Q1s6w.dat");
        Envelope envelope = EnvelopeDecoder.decode(raw);

        assertEquals("snapshot", envelope.codecName());
        assertEquals(1, envelope.codecVersion());
        assertEquals(0, envelope.compressionMode());

        JsonNode smile = new ObjectMapper(new SmileFactory()).readTree(envelope.content());
        assertEquals("snap_7_17_9", smile.get("snapshot").get("name").asText());
    }

    @Test
    void decodesShardSnapshotEnvelopeWithFileMapping() throws IOException {
        byte[] raw = read("indices/rCI54kLnSKCo4USf20Y76Q/0/snap-R78Q0cvSRYSueg5T5Q1s6w.dat");
        Envelope envelope = EnvelopeDecoder.decode(raw);

        assertEquals("snapshot", envelope.codecName());
        JsonNode smile = new ObjectMapper(new SmileFactory()).readTree(envelope.content());
        JsonNode files = smile.get("files");
        assertTrue(files.isArray());
        assertTrue(files.size() >= 3, "es7 shard has cfs/cfe/si at minimum");

        JsonNode first = files.get(0);
        assertTrue(first.has("name"), "obfuscated blob name");
        assertTrue(first.has("physical_name"), "real lucene file name");
        assertTrue(first.has("length"));
        assertTrue(first.has("written_by"), "lucene version that wrote the segment");
    }

    @Test
    void decodesIndexMetadataEnvelope() throws IOException {
        byte[] raw = read("indices/rCI54kLnSKCo4USf20Y76Q/meta-WwHhcqABimzIlgn1d3R5.dat");
        Envelope envelope = EnvelopeDecoder.decode(raw);

        assertEquals("index-metadata", envelope.codecName());
        JsonNode smile = new ObjectMapper(new SmileFactory()).readTree(envelope.content());
        // index metadata blob is keyed by index name: {"<index-name>": {state, mappings, settings...}}
        String indexName = smile.fieldNames().next();
        assertTrue(indexName.startsWith("test_data_"), "index name key, got: " + indexName);
        JsonNode metadata = smile.get(indexName);
        assertTrue(metadata.has("mappings") || metadata.has("settings"),
            "nested index metadata structure");
    }

    @Test
    void decodesEs8AndEs9Envelopes() throws IOException {
        byte[] es8 = read("indices/Z_nnpp14S7SwTz5rTG53pA/0/snap-h2mJ0DXuT4OA76OqN3LDZA.dat");
        Envelope e8 = EnvelopeDecoder.decode(es8);
        assertEquals("snapshot", e8.codecName());

        byte[] es9 = read("indices/2TYwDSpVTuqkcVKp-jesCA/0/snap-yF7yANBeS-y9vO5Yd1BsAQ.dat");
        Envelope e9 = EnvelopeDecoder.decode(es9);
        assertEquals("snapshot", e9.codecName());
    }

    @Test
    void rejectsGarbage() {
        assertThrows(IOException.class, () -> EnvelopeDecoder.decode(new byte[]{1, 2, 3}));
    }
}
