package de.sloow.gefrierschrank.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "gefrierschrank",
    mixinStandardHelpOptions = true,
    versionProvider = GefrierschrankCommand.Version.class,
    description = "Read Elasticsearch frozen-tier snapshot repositories without an Elasticsearch instance.",
    subcommands = {InfoCommand.class, ShardsCommand.class, FilesCommand.class, DumpCommand.class,
        DumpToDeltaCommand.class, SurveyCommand.class, OrphansCommand.class, FieldTypesCommand.class}
)
public class GefrierschrankCommand {

    static final class Version implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() {
            return new String[] {BuildInfo.describe()};
        }
    }
}
