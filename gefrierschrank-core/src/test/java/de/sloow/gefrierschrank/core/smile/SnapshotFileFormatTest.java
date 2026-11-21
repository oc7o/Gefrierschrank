package de.sloow.gefrierschrank.core.smile;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.Inflater;
import java.util.zip.DataFormatException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Empirical format-oracle tests against real ES 7/8/9 snapshot repository files.
 * These tests pin down the exact byte layout of ChecksumBlobStoreFormat files
 * before we commit to parser abstractions.
 */
class SnapshotFileFormatTest {

    private static final Path FIXTURES = Path.of("src/test/resources/fixtures/demo-repo");

    private byte[] read(String relPath) throws IOException {
        try (InputStream in = Files.newInputStream(FIXTURES.resolve(relPath))) {
            return in.readAllBytes();
        }
    }

    /** Reproduces the ES write path: header("codec", v1) + compress(...) + footer(...) */
    private byte[] esEnvelope(String relPath) throws IOException {
        byte[] data = read(relPath);

        // parse lucene header manually
        assertEquals(0x3FD76C17, readIntBE(data, 0), "lucene magic");
        int nameLen = data[4] & 0xFF;
        String codecName = new String(data, 5, nameLen, StandardCharsets.UTF_8);
        int headerEnd = 5 + nameLen + 4;

        // "DFL\0" = current compression mode marker
        assertEquals("DFL\0", new String(data, headerEnd, 4, StandardCharsets.UTF_8),
            "compression marker");
        int deflateStart = headerEnd + 4;

        // inflate to end of stream, remainder is the trailing structure
        Inflater inflater = new Inflater(true);
        inflater.setInput(data, deflateStart, data.length - deflateStart);
        byte[] out = new byte[data.length * 10];
        int n;
        try {
            n = inflater.inflate(out);
        } catch (DataFormatException e) {
            throw new RuntimeException(e);
        }
        int streamEnd = data.length - inflater.getRemaining();
        inflater.end();
        byte[] uncompressed = java.util.Arrays.copyOf(out, n);

        return java.util.Arrays.copyOf(data, streamEnd + 0);
    }

    private static int readIntBE(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24) | ((b[off+1] & 0xFF) << 16)
            | ((b[off+2] & 0xFF) << 8) | (b[off+3] & 0xFF);
    }

    @Test
    void luceneHeaderParses() throws IOException {
        byte[] data = read("snap-R78Q0cvSRYSueg5T5Q1s6w.dat");
        assertEquals(0x3FD76C17, readIntBE(data, 0));
        int nameLen = data[4] & 0xFF;
        assertEquals("snapshot", new String(data, 5, nameLen, StandardCharsets.UTF_8));
        int version = readIntBE(data, 5 + nameLen);
        assertEquals(1, version);
    }

    @Test
    void rootIndexFileIsPlainJson() throws IOException {
        byte[] data = read("index-2");
        String json = new String(data, StandardCharsets.UTF_8);
        assertTrue(json.startsWith("{\"min_version\""), json.substring(0, 40));
        assertTrue(json.contains("\"snapshots\""));
        assertTrue(json.contains("\"indices\""));
    }

    @Test
    void indexLatestIsEightByteBigEndianGeneration() throws IOException {
        byte[] data = read("index.latest");
        assertEquals(8, data.length);
        long gen = 0;
        for (byte b : data) gen = (gen << 8) | (b & 0xFF);
        assertEquals(2L, gen);
    }
}
