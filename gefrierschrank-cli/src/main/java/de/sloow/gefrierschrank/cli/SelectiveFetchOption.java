package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.SelectiveFetch;
import picocli.CommandLine;

/** {@code --selective-fetch} of {@code dump-to-delta}: download only the shard files the export reads. */
public class SelectiveFetchOption {

    @CommandLine.Spec(CommandLine.Spec.Target.MIXEE)
    CommandLine.Model.CommandSpec mixee;

    @CommandLine.Option(names = "--selective-fetch",
        description = "Download only the shard files the export reads (segment and field infos, stored fields, live"
            + " documents, doc values) for stored-source indices; postings, points, norms, term vectors and vectors"
            + " stay in the repository and are not verified. Also fails shards that reconstruction would refuse after"
            + " their commit files only. A shard that turns out to need more downloads the rest and is read again"
            + " in the same run. Default: off (-D" + SelectiveFetch.PROPERTY + "=true has the same effect).")
    boolean selectiveFetch;

    /**
     * Makes the option the default of every export in this JVM; without it, a
     * {@code -Dgefrierschrank.download.selective} property is validated here, so a typo is a usage
     * error before any repository access instead of a failed shard. The same holds for
     * {@code -Dgefrierschrank.download.selective.syntheticMinBytes}, read only when a shard is planned.
     */
    void apply() {
        if (selectiveFetch) System.setProperty(SelectiveFetch.PROPERTY, "true");
        try {
            SelectiveFetch.enabled();
            SelectiveFetch.syntheticProbeMinBytes();
        } catch (SelectiveFetch.InvalidSettingException invalid) {
            if (mixee == null) throw new IllegalStateException(invalid.getMessage());
            throw new CommandLine.ParameterException(mixee.commandLine(), invalid.getMessage());
        }
    }
}
