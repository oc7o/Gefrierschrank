package de.sloow.gefrierschrank.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Runs a command with its stderr captured, or a main class in a child JVM, for the CLI tests. */
final class CommandRuns {

    private CommandRuns() {}

    record ChildRun(int exit, String output) {}

    /** Runs mainClass in a JVM on this test class path with extra environment variables; stdout and stderr merged. */
    static ChildRun runChildJvm(Path tmp, Map<String, String> environment, List<String> jvmOptions,
                                String mainClass, String... args) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString()));
        command.addAll(jvmOptions);
        command.addAll(List.of("-cp", System.getProperty("java.class.path"), mainClass));
        command.addAll(List.of(args));
        Path output = Files.createTempFile(tmp, "child", ".log");
        var builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile());
        builder.environment().putAll(environment);
        Process process = builder.start();
        assertTrue(process.waitFor(3, TimeUnit.MINUTES), "child JVM did not finish");
        return new ChildRun(process.exitValue(), Files.readString(output));
    }

    record Captured(int exit, String stderr) {}

    /** Runs body with System.err captured; test classes run sequentially in one JVM. */
    static Captured captureStderr(Callable<Integer> body) throws Exception {
        PrintStream previous = System.err;
        var buffer = new ByteArrayOutputStream();
        System.setErr(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        try {
            int exit = body.call();
            return new Captured(exit, buffer.toString(StandardCharsets.UTF_8));
        } finally {
            System.setErr(previous);
        }
    }
}
