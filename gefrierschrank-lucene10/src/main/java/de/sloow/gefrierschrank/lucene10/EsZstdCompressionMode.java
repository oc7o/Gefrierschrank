package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;

/** Read-only stand-in for Elasticsearch's {@code ZstdCompressionMode}: this tool never writes segments. */
final class EsZstdCompressionMode extends CompressionMode {

    static final EsZstdCompressionMode INSTANCE = new EsZstdCompressionMode();

    private EsZstdCompressionMode() {}

    @Override
    public Compressor newCompressor() {
        throw new UnsupportedOperationException("read-only format");
    }

    @Override
    public Decompressor newDecompressor() {
        return new EsZstdDecompressor();
    }
}
