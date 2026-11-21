package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.TempRoot;
import picocli.CommandLine;

public final class Main {
    public static void main(String[] args) {
        int exitCode;
        // Before any command: the sweep frees ended runs' temp space before a temp budget is taken.
        try (var session = TempRoot.install()) {
            exitCode = session.run(() -> new CommandLine(new GefrierschrankCommand()).execute(args));
        }
        System.exit(exitCode);
    }
}
