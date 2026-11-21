package de.sloow.gefrierschrank.cli;

import picocli.CommandLine;
import picocli.CommandLine.Option;

/** dump-to-delta's --mounted-shells: whether a mounted shell of an index exported in the same run gets its own table. */
final class MountedShellsOption {

    enum Mode { EXPORT, SKIP_IF_ORIGINAL }

    @Option(names = "--mounted-shells", paramLabel = "export|skip-if-original", defaultValue = "export",
        converter = ModeConverter.class,
        description = "With --all-indices: export (default) writes one table per index name; skip-if-original "
            + "writes no table for a mounted searchable-snapshot shell whose original index is exported in the "
            + "same run from exactly the same backing index, snapshot and shard count; the manifest records it "
            + "as ALIAS_OF the original's table. A shell that already has a table is exported as before.")
    Mode mode = Mode.EXPORT;

    boolean skipIfOriginal() {
        return mode == Mode.SKIP_IF_ORIGINAL;
    }

    static final class ModeConverter implements CommandLine.ITypeConverter<Mode> {
        @Override
        public Mode convert(String value) {
            return switch (value) {
                case "export" -> Mode.EXPORT;
                case "skip-if-original" -> Mode.SKIP_IF_ORIGINAL;
                default -> throw new CommandLine.TypeConversionException("must be export or skip-if-original");
            };
        }
    }
}
