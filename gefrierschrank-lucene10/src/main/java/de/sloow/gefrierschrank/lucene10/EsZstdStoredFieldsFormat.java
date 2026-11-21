package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/**
 * Read-only stand-in for Elasticsearch's {@code Zstd814StoredFieldsFormat}: ES's own class
 * extends Lucene's stock {@code Lucene90CompressingStoredFieldsFormat} unchanged, swapping only
 * the per-block compression algorithm to ZSTD, so we do the same and let Lucene's own fdt/fdx/fdm
 * container-format reader do the rest.
 *
 * <p>ES tags each segment with which of two write-time tuning presets (BEST_SPEED /
 * BEST_COMPRESSION - differing in block size, docs-per-block, and zstd compression level)
 * produced it, via the segment attribute {@code "Zstd814StoredFieldsFormat.mode"}. This class
 * does not branch on that attribute: verified via javap against
 * Lucene90CompressingStoredFieldsFormat/-Reader that
 * {@code fieldsReader(Directory, SegmentInfo, FieldInfos, IOContext)} forwards only the format
 * name and the {@link org.apache.lucene.codecs.compressing.CompressionMode} to the reader -
 * block size and docs-per-block are write-only tuning knobs the reader never consults (the
 * actual chunk size is recovered per-segment from the segment's own "fdm" meta file), and ZSTD
 * decompression doesn't depend on the level used to compress. So a single instance, constructed
 * with either preset's constants, reads segments written under both presets correctly.
 */
final class EsZstdStoredFieldsFormat extends Lucene90CompressingStoredFieldsFormat {

    static final EsZstdStoredFieldsFormat INSTANCE = new EsZstdStoredFieldsFormat();

    private EsZstdStoredFieldsFormat() {
        // Name and shape must match ES's real format exactly for the fdt/fdx header checks to pass;
        // 14336/128/10 are the BEST_SPEED preset's writer-only constants (see class javadoc).
        super("ZstdStoredFields814", EsZstdCompressionMode.INSTANCE, 14336, 128, 10);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) {
        throw new UnsupportedOperationException("read-only format");
    }
}
