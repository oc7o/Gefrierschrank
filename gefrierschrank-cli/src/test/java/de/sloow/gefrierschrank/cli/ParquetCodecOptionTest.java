package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.delta.DeltaTableWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.*;

class ParquetCodecOptionTest {

    private String previousCodec;

    @BeforeEach
    void clearCodecProperty() {
        previousCodec = System.clearProperty(DeltaTableWriter.PARQUET_CODEC_KEY);
    }

    @AfterEach
    void restoreCodecProperty() {
        if (previousCodec == null) System.clearProperty(DeltaTableWriter.PARQUET_CODEC_KEY);
        else System.setProperty(DeltaTableWriter.PARQUET_CODEC_KEY, previousCodec);
    }

    @CommandLine.Command(name = "probe")
    static final class ProbeCommand implements Callable<Integer> {
        @CommandLine.Mixin ParquetOutputOptions parquetOutput = new ParquetOutputOptions();

        @Override public Integer call() {
            parquetOutput.apply();
            return 0;
        }
    }

    @Test
    void dumpToDeltaParsesTheCodecAndLeavesTheDefaultToTheWriter() {
        var defaults = new DumpToDeltaCommand();
        new CommandLine(defaults).parseArgs("repo", "--index", "i", "-o", "out");
        assertNull(defaults.parquetOutput.codec);
        defaults.parquetOutput.apply();
        assertNull(System.getProperty(DeltaTableWriter.PARQUET_CODEC_KEY));
        assertEquals(DeltaTableWriter.defaultParquetCodec(),
            DeltaTableWriter.parquetCodec(new org.apache.hadoop.conf.Configuration()));
        assertEquals(CompressionCodecName.ZSTD, DeltaTableWriter.defaultParquetCodec(), "zstd-jni loads on build hosts");

        for (var codec : ParquetOutputOptions.Codec.values()) {
            var command = new DumpToDeltaCommand();
            new CommandLine(command).parseArgs("repo", "--index", "i", "-o", "out", "--parquet-codec", codec.name());
            assertEquals(codec, command.parquetOutput.codec);
        }
    }

    @Test
    void aJvmCodecPropertyIsKeptWithoutTheOptionAndValidatedAtStartup() {
        System.setProperty(DeltaTableWriter.PARQUET_CODEC_KEY, "snappy");
        assertEquals(0, new CommandLine(new ProbeCommand()).execute());
        assertEquals("snappy", System.getProperty(DeltaTableWriter.PARQUET_CODEC_KEY));

        assertEquals(0, new CommandLine(new ProbeCommand()).execute("--parquet-codec", "uncompressed"));
        assertEquals("uncompressed", System.getProperty(DeltaTableWriter.PARQUET_CODEC_KEY), "the option wins");

        System.setProperty(DeltaTableWriter.PARQUET_CODEC_KEY, "gzip");
        var err = new StringWriter();
        int exit = new CommandLine(new ProbeCommand()).setErr(new PrintWriter(err)).execute();
        assertEquals(CommandLine.ExitCode.USAGE, exit);
        assertTrue(err.toString().contains(DeltaTableWriter.PARQUET_CODEC_KEY + " must be zstd, snappy or uncompressed"), err.toString());
    }

    @Test
    void unknownCodecIsAUsageError() {
        var err = new StringWriter();
        int exit = new CommandLine(new ProbeCommand()).setErr(new PrintWriter(err)).execute("--parquet-codec", "gzip");
        assertEquals(CommandLine.ExitCode.USAGE, exit);
        assertTrue(err.toString().contains("zstd, snappy, uncompressed"), err.toString());
        assertNull(System.getProperty(DeltaTableWriter.PARQUET_CODEC_KEY), "a rejected command line changes nothing");
    }

    @Test
    void applyMakesTheCodecTheProcessDefaultForDeltaWrites() {
        for (var codec : ParquetOutputOptions.Codec.values()) {
            int exit = new CommandLine(new ProbeCommand()).execute("--parquet-codec", codec.name());
            assertEquals(0, exit, codec.name());
            assertEquals(CompressionCodecName.valueOf(codec.name().toUpperCase()),
                DeltaTableWriter.parquetCodec(new org.apache.hadoop.conf.Configuration()));
        }
    }

    @Test
    void everyCodecCompressesInThisJvm() {
        for (var codec : ParquetOutputOptions.Codec.values())
            assertNull(DeltaTableWriter.parquetCodecFailure(CompressionCodecName.valueOf(codec.name().toUpperCase())), codec.name());
    }

    @Test
    void nativeLoadFailureOfAChosenCodecIsACleanStartupErrorNamingTheWayOut() {
        for (String codec : new String[]{"zstd", "snappy"}) {
            var command = new ProbeCommand();
            command.parquetOutput.codecProbe = chosen -> "UnsatisfiedLinkError";
            var err = new StringWriter();
            int exit = new CommandLine(command).setErr(new PrintWriter(err)).execute("--parquet-codec", codec);
            assertEquals(CommandLine.ExitCode.USAGE, exit, codec);
            String message = err.toString();
            assertTrue(message.contains("Parquet codec " + codec + " cannot run in this JVM (UnsatisfiedLinkError)"), message);
            assertTrue(message.contains("--parquet-codec uncompressed"), message);
            assertTrue(message.contains("-Djava.io.tmpdir=<dir>"), message);
            assertTrue(message.contains(codec.equals("zstd") ? "-DZstdTempFolder=<dir>" : "-Dorg.xerial.snappy.tempdir=<dir>"), message);
            assertFalse(message.contains("\tat "), "no stack trace: " + message);
        }
    }
}
