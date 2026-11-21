package de.sloow.gefrierschrank.delta;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.actions.AddFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Test view of what the writer actually put into Parquet footers and the Delta log. */
final class ParquetFooters {

    record Chunk(String column, CompressionCodecName codec, Set<Encoding> encodings, boolean hasDictionaryPage,
                 long dictionaryPageOffset, int pageCount, long totalUncompressedSize) {}

    record RowGroup(long rows, List<Chunk> chunks) {
        Chunk column(String name) {
            return chunks.stream().filter(c -> c.column().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError("no column chunk " + name));
        }
    }

    record FileFooter(String path, long size, List<RowGroup> rowGroups) {
        long rows() {
            return rowGroups.stream().mapToLong(RowGroup::rows).sum();
        }

        int pageCount(String column) {
            return rowGroups.stream().mapToInt(group -> group.column(column).pageCount()).sum();
        }

        Set<CompressionCodecName> codecs() {
            var codecs = new java.util.TreeSet<CompressionCodecName>();
            for (RowGroup group : rowGroups) for (Chunk chunk : group.chunks()) codecs.add(chunk.codec());
            return codecs;
        }
    }

    private ParquetFooters() {}

    /** AddFiles of the current snapshot; one shard's files in the order the writer rolled them. */
    static List<AddFile> addFiles(Configuration conf, String tableUri) {
        List<AddFile> files = new ArrayList<>(DeltaLog.forTable(conf, tableUri).update().getAllFiles());
        files.sort(java.util.Comparator.comparing((AddFile file) -> prefix(file.getPath()))
            .thenComparingLong(file -> counter(file.getPath())));
        return files;
    }

    // Data files are named <per-write prefix>-<counter>.parquet.
    private static String prefix(String path) {
        return path.substring(0, path.lastIndexOf('-'));
    }

    private static long counter(String path) {
        return Long.parseLong(path.substring(path.lastIndexOf('-') + 1, path.length() - ".parquet".length()));
    }

    /** One footer per AddFile of the current table snapshot, in log order. */
    static List<FileFooter> read(Configuration conf, String tableUri) throws IOException {
        List<FileFooter> footers = new ArrayList<>();
        for (AddFile file : addFiles(conf, tableUri)) {
            footers.add(read(conf, new Path(tableUri + "/" + file.getPath())));
        }
        return footers;
    }

    static FileFooter read(Configuration conf, Path file) throws IOException {
        var input = HadoopInputFile.fromPath(file, conf);
        try (var reader = ParquetFileReader.open(input)) {
            List<RowGroup> groups = new ArrayList<>();
            for (BlockMetaData block : reader.getFooter().getBlocks()) {
                List<Chunk> chunks = new ArrayList<>();
                for (ColumnChunkMetaData column : block.getColumns()) {
                    var offsets = reader.readOffsetIndex(column);
                    chunks.add(new Chunk(column.getPath().toDotString(), column.getCodec(),
                        Set.copyOf(column.getEncodings()), column.hasDictionaryPage(),
                        column.getDictionaryPageOffset(), offsets == null ? -1 : offsets.getPageCount(),
                        column.getTotalUncompressedSize()));
                }
                groups.add(new RowGroup(block.getRowCount(), chunks));
            }
            return new FileFooter(file.getName(), input.getLength(), groups);
        }
    }

    /** Every Parquet data file physically present in a local table directory, referenced or not. */
    static Set<String> parquetFilesOnDisk(java.nio.file.Path tableDir) throws IOException {
        if (!Files.exists(tableDir)) return Set.of();
        try (var files = Files.list(tableDir)) {
            return files.map(p -> p.getFileName().toString()).filter(name -> name.endsWith(".parquet"))
                .collect(java.util.stream.Collectors.toCollection(java.util.TreeSet::new));
        }
    }

    static Set<String> addFilePaths(Configuration conf, String tableUri) {
        var paths = new java.util.TreeSet<String>();
        for (AddFile file : addFiles(conf, tableUri)) paths.add(file.getPath());
        return paths;
    }
}
