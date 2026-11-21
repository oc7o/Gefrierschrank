package de.sloow.gefrierschrank.cli;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Model.PositionalParamSpec;

import java.util.ArrayList;
import java.util.IllegalFormatException;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

/**
 * picocli renders every description line through String.format, so a bare '%' makes it log a
 * "Could not format" warning on each help and usage error. Its tracer writes to the System.err
 * captured when picocli loaded, so this asserts on the formatting itself, not on stderr. The
 * lines are the declared ones: ArgSpec.description() has already been formatted.
 */
class UsageHelpTest {

    @Test
    void everyDescriptionLineOfEveryCommandIsAValidFormatString() {
        var failures = new ArrayList<String>();
        int checked = 0;
        for (CommandLine command : commandTree(new CommandLine(new GefrierschrankCommand()))) {
            var spec = command.getCommandSpec();
            var lines = new ArrayList<String>(List.of(spec.usageMessage().description()));
            for (ArgSpec arg : spec.args()) lines.addAll(List.of(declaredDescription(arg)));
            for (String line : lines) {
                checked++;
                try {
                    String.format(Locale.ROOT, line);
                } catch (IllegalFormatException e) {
                    failures.add(spec.qualifiedName() + ": " + line);
                }
            }
        }
        assertTrue(checked > 50, "walked the whole command tree: " + checked + " lines");
        assertEquals(List.of(), failures);
    }

    @Test
    void theTempBudgetDefaultRendersItsPercentSign() {
        String usage = new CommandLine(new GefrierschrankCommand()).getSubcommands().get("dump-to-delta")
            .getUsageMessage(CommandLine.Help.Ansi.OFF).replaceAll("\\s+", " ");
        assertTrue(usage.contains("80% of the free space"), usage);
    }

    @Test
    void versionNamesTheBuildsMavenVersion() {
        String[] version = new CommandLine(new GefrierschrankCommand()).getCommandSpec().version();
        assertEquals(1, version.length);
        assertNotEquals(BuildInfo.UNKNOWN_VERSION, BuildInfo.version());
        assertTrue(version[0].startsWith("gefrierschrank " + BuildInfo.version() + " (built "), version[0]);
    }

    private static String[] declaredDescription(ArgSpec arg) {
        return arg instanceof OptionSpec option ? option.toBuilder().description()
            : ((PositionalParamSpec) arg).toBuilder().description();
    }

    private static List<CommandLine> commandTree(CommandLine root) {
        var all = new ArrayList<CommandLine>();
        all.add(root);
        for (CommandLine sub : root.getSubcommands().values()) all.addAll(commandTree(sub));
        return all;
    }
}
