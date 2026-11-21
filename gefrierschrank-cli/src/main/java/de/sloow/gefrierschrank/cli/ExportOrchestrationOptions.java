package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.delta.ExportSettings;
import de.sloow.gefrierschrank.delta.SnapshotSelection;
import picocli.CommandLine;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

import java.util.Stack;

/** dump-to-delta options for run-wide parallelism, its temp-disk budget, and which snapshot is exported. */
final class ExportOrchestrationOptions {

    private static final String ALL_INDICES = "--all-indices";

    @Spec(Spec.Target.MIXEE)
    CommandSpec spec;

    int parallelism = 1;
    long tempBudgetMiB;

    @Option(names = "--parallelism", paramLabel = "N", defaultValue = "1",
        description = "Shards processed concurrently across the whole run (1-" + ExportSettings.MAX_PARALLELISM
            + ", default 1 = one shard at a time, in order). With --all-indices, up to N indices are "
            + "exported at once, started in listing order, and their shards share the same bound. Each "
            + "concurrent shard is fully downloaded to the temp directory before it is read, so temp disk "
            + "grows with N; see --temp-budget.")
    void parallelism(int value) {
        if (value < 1 || value > ExportSettings.MAX_PARALLELISM)
            throw new ParameterException(spec.commandLine(),
                "--parallelism must be between 1 and " + ExportSettings.MAX_PARALLELISM);
        parallelism = value;
    }

    @Option(names = "--temp-budget", paramLabel = "SIZE",
        description = "Temp disk that concurrently processed shards may occupy, e.g. 500G, 2T or 800M "
            + "(binary units; a bare number means MiB). A shard larger than the budget runs alone. "
            + "Default with --parallelism above 1: 80%% of the free space of java.io.tmpdir at start.")
    void tempBudget(String value) {
        long mib = parseMiB(value);
        if (mib < 1 || mib > Integer.MAX_VALUE)
            throw new ParameterException(spec.commandLine(), "--temp-budget must be a size like 500G, 2T or 800M");
        tempBudgetMiB = mib;
    }

    static long parseMiB(String value) {
        var match = java.util.regex.Pattern.compile("(\\d{1,12})\\s*([mgtMGT]?)(?:i?[bB])?").matcher(value.trim());
        if (!match.matches()) return -1;
        long number = Long.parseLong(match.group(1));
        return switch (match.group(2).toUpperCase(java.util.Locale.ROOT)) {
            case "G" -> number * 1024;
            case "T" -> number * 1024 * 1024;
            default -> number;
        };
    }

    @Option(names = "--snapshot", paramLabel = "auto|latest|NAME|UUID", defaultValue = "auto",
        parameterConsumer = SnapshotConsumer.class,
        description = "Snapshot to export. auto (default): a table that already holds shards of one "
            + "still-retained snapshot is completed from that snapshot, so a rerun after the next "
            + "snapshot resumes instead of failing; the report and manifest say when a newer snapshot "
            + "exists. Otherwise, and for new tables, the latest retained snapshot. latest: always "
            + "the latest retained snapshot; a table started from another snapshot is refused. "
            + "NAME or UUID: that snapshot (only with --index).")
    String snapshot = "auto";

    ExportSettings settings(boolean allIndices) {
        var selection = SnapshotSelection.parse(snapshot);
        if (allIndices && selection.mode() == SnapshotSelection.Mode.EXPLICIT)
            throw explicitSnapshotNeedsIndex(spec);
        return new ExportSettings(selection, parallelism, tempBudgetMiB);
    }

    private static ParameterException explicitSnapshotNeedsIndex(CommandSpec spec) {
        return new ParameterException(spec.commandLine(),
            "--snapshot NAME|UUID selects one index's snapshot and requires --index; "
                + "use --snapshot auto or latest with --all-indices");
    }

    /**
     * Rejects a named snapshot next to --all-indices while parsing, so the usage error exits 2
     * before any repository access. picocli offers no hook after all options are parsed, so this
     * checks both directions here: --all-indices already parsed, or still ahead in the arguments.
     */
    static final class SnapshotConsumer implements CommandLine.IParameterConsumer {
        @Override
        public void consumeParameters(Stack<String> args, ArgSpec argSpec, CommandSpec commandSpec) {
            if (args.isEmpty())
                throw new ParameterException(commandSpec.commandLine(), "Missing value for --snapshot");
            String value = args.pop();
            SnapshotSelection selection;
            try {
                selection = SnapshotSelection.parse(value);
            } catch (IllegalArgumentException e) {
                throw new ParameterException(commandSpec.commandLine(), "Invalid --snapshot value");
            }
            if (selection.mode() == SnapshotSelection.Mode.EXPLICIT
                && (allIndicesParsed(commandSpec) || args.contains(ALL_INDICES)))
                throw explicitSnapshotNeedsIndex(commandSpec);
            argSpec.setValue(value);
        }

        private static boolean allIndicesParsed(CommandSpec commandSpec) {
            return commandSpec.userObject() instanceof DumpToDeltaCommand command
                && command.indexSelection != null && command.indexSelection.allIndices;
        }
    }
}
