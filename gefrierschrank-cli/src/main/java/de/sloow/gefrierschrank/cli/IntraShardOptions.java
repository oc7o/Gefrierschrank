package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.delta.DocumentMemoryGate;
import de.sloow.gefrierschrank.delta.ShardRanges;
import de.sloow.gefrierschrank.syntheticsource.IsolatedReconstructionWorker;
import picocli.CommandLine;

/** {@code --shard-threads} of {@code dump-to-delta}: how many doc-id ranges of one shard are read and staged at once. */
public class IntraShardOptions {

    @CommandLine.Spec(CommandLine.Spec.Target.MIXEE)
    CommandLine.Model.CommandSpec mixee;

    String shardThreads;

    @CommandLine.Option(names = "--shard-threads", paramLabel = "auto|N",
        description = "Threads that read and write one shard, as contiguous document ranges, 1-" + ShardRanges.MAX_SHARD_THREADS
            + " or auto (cores / --parallelism, at most 8). Default: " + ShardRanges.DEFAULT_SHARD_THREADS
            + ". Each range needs its own Parquet row group in heap, so the heap caps the ranges per shard; "
            + "a range covers at least 250,000 live documents. The rows are the same at every setting.")
    void shardThreads(String value) {
        validate(value);
        shardThreads = value;
    }

    /**
     * Makes a chosen setting the default of every export in this JVM. Without the option, a
     * {@code -Dgefrierschrank.shard.threads} property is validated here, so a typo is a usage error
     * before any repository access instead of a failed index; so are
     * {@code -Dgefrierschrank.document.reserveMultiplier} and
     * {@code -Dgefrierschrank.reconstruction.directory}.
     */
    void apply() {
        if (shardThreads != null) System.setProperty(ShardRanges.SHARD_THREADS_KEY, shardThreads);
        String value = System.getProperty(ShardRanges.SHARD_THREADS_KEY);
        if (value != null) validate(value.trim());
        String multiplier = System.getProperty(DocumentMemoryGate.RESERVE_MULTIPLIER_PROPERTY);
        if (multiplier != null) {
            try {
                DocumentMemoryGate.reserveMultiplier(multiplier);
            } catch (IllegalArgumentException invalid) {
                usage(invalid.getMessage());
            }
        }
        String directory = invalidReconstructionDirectory();
        if (directory != null) usage(directory);
    }

    /**
     * The fixed message for a {@code -Dgefrierschrank.reconstruction.directory} the isolated reader
     * would refuse, or null. Unchecked, every synthetic-source shard would be quarantined with an
     * IllegalArgumentException whose message reaches no report.
     */
    static String invalidReconstructionDirectory() {
        String value = System.getProperty(IsolatedReconstructionWorker.DIRECTORY_PROPERTY);
        if (value == null || value.equals("mmap") || value.equals("niofs")) return null;
        return "-D" + IsolatedReconstructionWorker.DIRECTORY_PROPERTY + " must be mmap or niofs";
    }

    private void usage(String message) {
        if (mixee == null) throw new IllegalStateException(message);
        throw new CommandLine.ParameterException(mixee.commandLine(), message);
    }

    private void validate(String value) {
        try {
            ShardRanges.resolve(value, 1);
        } catch (IllegalArgumentException invalid) {
            String message = "--shard-threads must be auto or between 1 and " + ShardRanges.MAX_SHARD_THREADS;
            if (mixee == null) throw new IllegalStateException(message);
            throw new CommandLine.ParameterException(mixee.commandLine(), message);
        }
    }
}
