package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.delta.DeltaTableWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import picocli.CommandLine;

import java.util.function.Function;

/** Parquet output options of {@code dump-to-delta}. */
public class ParquetOutputOptions {

    enum Codec { zstd, snappy, uncompressed }

    @CommandLine.Spec(CommandLine.Spec.Target.MIXEE)
    CommandLine.Model.CommandSpec mixee;

    @CommandLine.Option(names = "--parquet-codec", paramLabel = "CODEC",
        description = "Compression of the Parquet data files: ${COMPLETION-CANDIDATES}. Default: zstd, or " +
            "uncompressed where this JVM cannot load the ZSTD native library. Reading ZSTD needs parquet-mr >= 1.10 " +
            "(Spark >= 3.2), Trino, DuckDB or delta-rs; choose snappy or uncompressed for older readers.")
    Codec codec;

    // A native library that fails to load cannot be provoked in-process; a child JVM test covers the real one.
    Function<CompressionCodecName, String> codecProbe = DeltaTableWriter::parquetCodecFailure;

    /**
     * Makes a chosen codec the default of every Delta write in this JVM and resolves the codec now.
     * A chosen codec whose native library cannot load becomes a clean startup error instead of an
     * Error in the middle of the first shard. Without a choice (option or JVM system property), the
     * writer's default applies, and its fallback warning is printed here, before any export output.
     */
    void apply() {
        if (codec != null) System.setProperty(DeltaTableWriter.PARQUET_CODEC_KEY, codec.name());
        CompressionCodecName resolved;
        try {
            resolved = DeltaTableWriter.parquetCodec(new Configuration());
        } catch (IllegalArgumentException invalid) {
            throw usageError(invalid.getMessage());
        }
        String failure = codecProbe.apply(resolved);
        if (failure == null) return;
        String tempDirOption = resolved == CompressionCodecName.SNAPPY
            ? "-Dorg.xerial.snappy.tempdir=<dir>" : "-DZstdTempFolder=<dir>";
        throw usageError("Parquet codec " + resolved.name().toLowerCase(java.util.Locale.ROOT)
            + " cannot run in this JVM (" + failure + "): its native library could not be loaded. "
            + "Give the JVM an executable temp directory with " + tempDirOption + " or -Djava.io.tmpdir=<dir>, "
            + "or write Parquet without a native library using --parquet-codec uncompressed.");
    }

    private RuntimeException usageError(String message) {
        if (mixee == null) return new IllegalStateException(message);
        return new CommandLine.ParameterException(mixee.commandLine(), message);
    }
}
