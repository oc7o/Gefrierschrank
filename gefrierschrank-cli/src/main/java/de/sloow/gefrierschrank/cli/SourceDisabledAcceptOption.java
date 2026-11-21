package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage;
import picocli.CommandLine;

/**
 * {@code dump-to-delta --source-disabled-accept unmapped-values}: export documents without stored
 * {@code _source} although values sent under {@code dynamic: false} or {@code dynamic: runtime}
 * objects are lost uncounted. Declared on a setter so the choice takes effect while parsing; any
 * other value is a usage error (exit 2) before any repository read.
 */
public class SourceDisabledAcceptOption {

    @CommandLine.Spec(CommandLine.Spec.Target.MIXEE)
    CommandLine.Model.CommandSpec mixee;

    @CommandLine.Option(names = "--source-disabled-accept", paramLabel = "WHAT",
        description = "Only value: " + SyntheticCoverage.ACCEPT_UNMAPPED_VALUES + ". Reconstructs documents without stored "
            + "_source although values sent under dynamic:false or dynamic:runtime objects were kept only in the "
            + "disabled _source and are lost, uncounted. Without it such shards quarantine (UnreconstructableSourceException). "
            + "Each affected index prints one warning; its shard reports carry unrecoverableUnmappedValues: true.")
    void accept(String what) {
        if (!SyntheticCoverage.ACCEPT_UNMAPPED_VALUES.equalsIgnoreCase(what.strip())) {
            String message = "--source-disabled-accept accepts only " + SyntheticCoverage.ACCEPT_UNMAPPED_VALUES;
            if (mixee == null) throw new IllegalArgumentException(message);
            throw new CommandLine.ParameterException(mixee.commandLine(), message);
        }
        System.setProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY, SyntheticCoverage.ACCEPT_UNMAPPED_VALUES);
    }
}
