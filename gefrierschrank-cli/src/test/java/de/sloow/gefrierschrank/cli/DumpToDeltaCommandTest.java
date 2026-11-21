package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.delta.RunReport;
import de.sloow.gefrierschrank.delta.SnapshotSelection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DumpToDeltaCommandTest {

    @Test
    void externalRepositoryCredentialsStaySeparateFromPrimary() {
        var env = Map.of("AWS_ACCESS_KEY_ID", "primary", "FROZEN_AWS_ACCESS_KEY_ID", "backing",
            "FROZEN_AWS_SECRET_ACCESS_KEY", "backing-secret", "FROZEN_AWS_SESSION_TOKEN", "session");
        var backing = DumpToDeltaCommand.repositoryEnvironment("FROZEN", env::get);
        assertEquals("backing", DumpToDeltaCommand.resolveSourceCredentials(backing).accessKey());
        assertEquals("session", backing.apply("AWS_SESSION_TOKEN"));
        assertNull(backing.apply("AWS_REGION"));
    }

    @Test
    void noDataSkipsCannotClaimCompleteCoverage() {
        var skipped = new RunReport.ShardReport(0, "snap", RunReport.Outcome.SKIPPED_NO_DATA,
            0, 0, "t", 0, null, null);
        var report = new RunReport("idx", "t", "t", 0, 1, 0, 1, 0, 0, 0, List.of(skipped));
        assertEquals(3, DumpToDeltaCommand.exitCode(report));
    }

    @Test
    void manifestIncludesPendingIndicesDuringAnInterruptedRun(@TempDir Path tmp) throws Exception {
        var command = new DumpToDeltaCommand();
        assertThrows(AssertionError.class, () -> command.runAllIndices(List.of("a", "b"), name -> {
            if (name.equals("b")) throw new AssertionError("simulated interruption");
            return new RunReport(name, "t", "t", 0, 1, 1, 0, 0, 1, 1, List.of());
        }, tmp));
        var manifest = new com.fasterxml.jackson.databind.ObjectMapper().readTree(tmp.resolve("manifest.json").toFile());
        assertEquals(2, manifest.path("totalIndices").asInt());
        assertFalse(manifest.path("complete").asBoolean());
        assertEquals("b", manifest.path("pendingIndices").path(0).asText());
    }

    @Test
    void exitCodeIsZeroWhenNoShardIsQuarantined() {
        RunReport report = new RunReport("idx", "t0", "t1", 5, 2, 2, 0, 0, 100, 1000, List.of());

        assertEquals(0, DumpToDeltaCommand.exitCode(report));
    }

    @Test
    void exitCodeIsThreeWhenAnyShardIsQuarantined() {
        RunReport report = new RunReport("idx", "t0", "t1", 5, 3, 1, 1, 1, 50, 500, List.of());

        assertEquals(3, DumpToDeltaCommand.exitCode(report));
    }

    @Test
    void targetSpecificCredentialsArePreferredWhenSet() {
        var env = Map.of(
            "TARGET_AWS_ACCESS_KEY_ID", "target-key",
            "TARGET_AWS_SECRET_ACCESS_KEY", "target-secret",
            "AWS_ACCESS_KEY_ID", "source-key",
            "AWS_SECRET_ACCESS_KEY", "source-secret");

        var creds = DumpToDeltaCommand.resolveTargetCredentials(env::get);

        assertEquals("target-key", creds.accessKey());
        assertEquals("target-secret", creds.secretKey());
    }

    @Test
    void targetCredentialsFallBackToGeneralAwsVarsWhenTargetSpecificAbsent() {
        var env = Map.of(
            "AWS_ACCESS_KEY_ID", "source-key",
            "AWS_SECRET_ACCESS_KEY", "source-secret");

        var creds = DumpToDeltaCommand.resolveTargetCredentials(env::get);

        assertEquals("source-key", creds.accessKey());
        assertEquals("source-secret", creds.secretKey());
    }

    @Test
    void missingAllTargetCredentialsThrows() {
        Map<String, String> env = Map.of();

        assertThrows(IllegalArgumentException.class,
            () -> DumpToDeltaCommand.resolveTargetCredentials(env::get));
    }

    @Test
    void sourceCredentialsAreReadFromGeneralAwsVars() {
        var env = Map.of(
            "AWS_ACCESS_KEY_ID", "source-key",
            "AWS_SECRET_ACCESS_KEY", "source-secret");

        var creds = DumpToDeltaCommand.resolveSourceCredentials(env::get);

        assertEquals("source-key", creds.accessKey());
        assertEquals("source-secret", creds.secretKey());
    }

    @Test
    void missingSourceCredentialsThrows() {
        Map<String, String> env = Map.of();

        assertThrows(IllegalArgumentException.class,
            () -> DumpToDeltaCommand.resolveSourceCredentials(env::get));
    }

    @Test
    void targetEndpointMustBeExplicit() {
        assertThrows(IllegalArgumentException.class,
            () -> DumpToDeltaCommand.resolveTargetEndpoint(null));
    }

    @Test
    void targetEndpointIsReturnedWhenSet() {
        assertEquals("http://target:9000", DumpToDeltaCommand.resolveTargetEndpoint("http://target:9000"));
    }

    // --- --all-indices: aggregate exit code (mirrors exitCode(RunReport) above, one level up) ---

    @Test
    void aggregateExitCodeIsZeroWhenAllIndicesCleanlyComplete() {
        var entries = List.of(
            new AllIndicesManifest.IndexEntry("a", AllIndicesManifest.Outcome.COMPLETED, "a.json", null, null),
            new AllIndicesManifest.IndexEntry("b", AllIndicesManifest.Outcome.COMPLETED, "b.json", null, null));

        assertEquals(0, DumpToDeltaCommand.aggregateExitCode(entries));
    }

    @Test
    void aggregateExitCodeIsThreeWhenAnyIndexHasQuarantinedShards() {
        var entries = List.of(
            new AllIndicesManifest.IndexEntry("a", AllIndicesManifest.Outcome.COMPLETED, "a.json", null, null),
            new AllIndicesManifest.IndexEntry("b", AllIndicesManifest.Outcome.COMPLETED_WITH_QUARANTINE, "b.json", null, null));

        assertEquals(3, DumpToDeltaCommand.aggregateExitCode(entries));
    }

    @Test
    void aggregateExitCodeIsFourWhenAnyIndexFailedOutright() {
        var entries = List.of(
            new AllIndicesManifest.IndexEntry("a", AllIndicesManifest.Outcome.COMPLETED_WITH_QUARANTINE, "a.json", null, null),
            new AllIndicesManifest.IndexEntry("b", AllIndicesManifest.Outcome.FAILED, null, "IOException", "boom"));

        assertEquals(4, DumpToDeltaCommand.aggregateExitCode(entries));
    }

    // --- --all-indices: manifest aggregation ---

    @Test
    void manifestCountsReflectAMixOfOutcomes() {
        var entries = List.of(
            new AllIndicesManifest.IndexEntry("a", AllIndicesManifest.Outcome.COMPLETED, "a.json", null, null),
            new AllIndicesManifest.IndexEntry("b", AllIndicesManifest.Outcome.COMPLETED_WITH_QUARANTINE, "b.json", null, null),
            new AllIndicesManifest.IndexEntry("c", AllIndicesManifest.Outcome.FAILED, null, "IOException", "boom"));

        AllIndicesManifest manifest = DumpToDeltaCommand.buildManifest(entries);

        assertEquals(3, manifest.totalIndices());
        assertEquals(1, manifest.completedCount());
        assertEquals(1, manifest.completedWithQuarantineCount());
        assertEquals(1, manifest.failedCount());
        assertEquals(entries, manifest.indices());
    }

    // --- --all-indices: one index's total failure must not stop the others ---

    @Test
    void oneIndexFailingOutrightDoesNotStopTheOthersFromBeingProcessed() {
        List<String> processed = new ArrayList<>();
        DumpToDeltaCommand.IndexProcessor processor = name -> {
            processed.add(name);
            if (name.equals("b")) {
                throw new IOException("shard catalog unreachable");
            }
            return new RunReport(name, "t0", "t1", 5, 1, 1, 0, 0, 10, 100, List.of());
        };

        int exitCode = new DumpToDeltaCommand().runAllIndices(List.of("a", "b", "c"), processor, null);

        assertEquals(List.of("a", "b", "c"), processed, "all three indices must be attempted despite b throwing");
        assertEquals(4, exitCode, "one failed-outright index must be reflected in the aggregate exit code");
    }

    @Test
    void allCleanIndicesYieldExitCodeZero() {
        DumpToDeltaCommand.IndexProcessor processor =
            name -> new RunReport(name, "t0", "t1", 5, 1, 1, 0, 0, 10, 100, List.of());

        int exitCode = new DumpToDeltaCommand().runAllIndices(List.of("a", "b"), processor, null);

        assertEquals(0, exitCode);
    }

    // --- --all-indices: per-index output/report path derivation from the root/directory ---

    @Test
    void resolveIndexTableUriAppendsIndexNameToRoot() {
        assertEquals("s3a://bucket/root/foo", DumpToDeltaCommand.resolveIndexTableUri("s3a://bucket/root", "foo"));
    }

    @Test
    void resolveIndexTableUriHandlesTrailingSlashOnRoot() {
        assertEquals("s3a://bucket/root/foo", DumpToDeltaCommand.resolveIndexTableUri("s3a://bucket/root/", "foo"));
    }

    @Test
    void resolvePerIndexReportPathLandsUnderTheGivenDirectory(@TempDir Path tmp) {
        Path resolved = DumpToDeltaCommand.resolvePerIndexReportPath(tmp, "foo");

        assertEquals(tmp.toAbsolutePath().normalize().resolve("foo.json"), resolved);
    }

    @Test
    void resolvePerIndexReportPathRejectsPathTraversal(@TempDir Path tmp) {
        assertThrows(IllegalArgumentException.class,
            () -> DumpToDeltaCommand.resolvePerIndexReportPath(tmp, "../escape"));
    }

    // --- --index and --all-indices are mutually exclusive ---

    @Test
    void indexAndAllIndicesTogetherAreRejected() {
        CommandLine cmd = new CommandLine(new DumpToDeltaCommand());

        assertThrows(CommandLine.ParameterException.class, () ->
            cmd.parseArgs("some-repo", "-o", "/tmp/out", "--index", "foo", "--all-indices"));
    }

    @Test
    void neitherIndexNorAllIndicesIsRejected() {
        CommandLine cmd = new CommandLine(new DumpToDeltaCommand());

        assertThrows(CommandLine.ParameterException.class, () ->
            cmd.parseArgs("some-repo", "-o", "/tmp/out"));
    }

    @Test
    void allIndicesAloneParsesSuccessfully() {
        CommandLine cmd = new CommandLine(new DumpToDeltaCommand());

        assertDoesNotThrow(() -> cmd.parseArgs("some-repo", "-o", "/tmp/out", "--all-indices"));
    }

    @Test
    void indexAloneParsesSuccessfully() {
        CommandLine cmd = new CommandLine(new DumpToDeltaCommand());

        assertDoesNotThrow(() -> cmd.parseArgs("some-repo", "-o", "/tmp/out", "--index", "foo"));
    }

    // --- --snapshot ---

    @Test
    void snapshotDefaultsToAuto() {
        var command = new DumpToDeltaCommand();
        new CommandLine(command).parseArgs("some-repo", "-o", "/tmp/out", "--index", "foo");

        assertSame(SnapshotSelection.AUTO, command.orchestration.settings(false).snapshotSelection());
        assertSame(SnapshotSelection.DEFAULT, command.orchestration.settings(false).snapshotSelection());
    }

    @Test
    void aNamedSnapshotIsAcceptedWithIndex() {
        var command = new DumpToDeltaCommand();
        new CommandLine(command).parseArgs("some-repo", "-o", "/tmp/out", "--index", "foo", "--snapshot", "snap_x");

        assertEquals(SnapshotSelection.explicit("snap_x"), command.orchestration.settings(false).snapshotSelection());
    }

    @Test
    void aNamedSnapshotWithAllIndicesIsAUsageErrorInEitherOrder() {
        for (String[] args : List.of(
            new String[]{"some-repo", "-o", "/tmp/out", "--all-indices", "--snapshot", "snap_x"},
            new String[]{"some-repo", "--snapshot", "snap_x", "-o", "/tmp/out", "--all-indices"},
            new String[]{"some-repo", "-o", "/tmp/out", "--all-indices", "--snapshot=snap_x"},
            new String[]{"some-repo", "--snapshot=uuid-1", "--all-indices", "-o", "/tmp/out"})) {
            assertThrows(CommandLine.ParameterException.class,
                () -> new CommandLine(new DumpToDeltaCommand()).parseArgs(args), String.join(" ", args));
        }
        assertEquals(2, new CommandLine(new DumpToDeltaCommand())
            .execute("some-repo", "-o", "/tmp/out", "--all-indices", "--snapshot", "snap_x"));
    }

    @Test
    void keywordSnapshotsParseWithAllIndices() {
        for (String keyword : List.of("auto", "latest")) {
            var command = new DumpToDeltaCommand();
            new CommandLine(command).parseArgs("some-repo", "-o", "/tmp/out", "--all-indices", "--snapshot", keyword);
            assertEquals(SnapshotSelection.parse(keyword), command.orchestration.settings(true).snapshotSelection());
        }
    }

    @Test
    void anUnknownSnapshotNameFailsTheRunWithTheSanitizedMessage(@TempDir Path tmp) {
        var err = new java.io.ByteArrayOutputStream();
        var previous = System.err;
        int exit;
        System.setErr(new java.io.PrintStream(err, true));
        try {
            exit = new CommandLine(new DumpToDeltaCommand()).execute(
                Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo").toString(),
                "--index", "test_data_7_17_9", "-o", tmp.resolve("out").toString(), "--snapshot", "no-such-snapshot");
        } finally {
            System.setErr(previous);
        }
        assertEquals(1, exit);
        assertTrue(err.toString().startsWith("error: java.io.IOException at "), err.toString());
        assertFalse(err.toString().contains("no-such-snapshot"), "raw exception messages stay out of the output");
        assertFalse(java.nio.file.Files.exists(tmp.resolve("out")), "nothing is written for an unknown snapshot");
    }

    // --- --parallelism / --temp-budget ---

    @Test
    void parallelismDefaultsToOneShardAtATimeWithoutABudget() {
        var command = new DumpToDeltaCommand();
        new CommandLine(command).parseArgs("some-repo", "-o", "/tmp/out", "--all-indices");

        var settings = command.orchestration.settings(true);
        assertEquals(1, settings.parallelism());
        assertEquals(0, settings.tempBudgetMiB());
    }

    @Test
    void parallelismAndTempBudgetParse() {
        var command = new DumpToDeltaCommand();
        new CommandLine(command).parseArgs("some-repo", "-o", "/tmp/out", "--all-indices",
            "--parallelism", "4", "--temp-budget", "500G");

        var settings = command.orchestration.settings(true);
        assertEquals(4, settings.parallelism());
        assertEquals(500L * 1024, settings.tempBudgetMiB());
    }

    @Test
    void outOfRangeParallelismAndMalformedBudgetsAreUsageErrors() {
        for (String[] extra : List.of(new String[]{"--parallelism", "0"}, new String[]{"--parallelism", "257"},
            new String[]{"--temp-budget", "1.5T"}, new String[]{"--temp-budget", "0"},
            new String[]{"--temp-budget", "lots"}, new String[]{"--temp-budget", "4096T"})) {
            var args = new ArrayList<>(List.of("some-repo", "-o", "/tmp/out", "--index", "foo"));
            args.addAll(List.of(extra));
            assertThrows(CommandLine.ParameterException.class,
                () -> new CommandLine(new DumpToDeltaCommand()).parseArgs(args.toArray(String[]::new)),
                String.join(" ", extra));
        }
        assertEquals(2, new CommandLine(new DumpToDeltaCommand())
            .execute("some-repo", "-o", "/tmp/out", "--index", "foo", "--parallelism", "0"));
    }

    @Test
    void tempBudgetSizesAreBinaryWithMiBAsTheBareUnit() {
        assertEquals(800, ExportOrchestrationOptions.parseMiB("800"));
        assertEquals(800, ExportOrchestrationOptions.parseMiB("800M"));
        assertEquals(800, ExportOrchestrationOptions.parseMiB("800MiB"));
        assertEquals(2048, ExportOrchestrationOptions.parseMiB("2g"));
        assertEquals(2048, ExportOrchestrationOptions.parseMiB("2 GB"));
        assertEquals(3L * 1024 * 1024, ExportOrchestrationOptions.parseMiB("3T"));
        assertEquals(-1, ExportOrchestrationOptions.parseMiB("-1G"));
        assertEquals(-1, ExportOrchestrationOptions.parseMiB("12K"));
    }

    // --- snapshot visibility in the --all-indices manifest ---

    @Test
    void manifestRecordsEachIndexSnapshotAndCountsTablesBehindLatest(@TempDir Path tmp) throws Exception {
        DumpToDeltaCommand.IndexProcessor processor = name -> {
            if (name.equals("c")) throw new IOException("unreachable");
            var report = new RunReport(name, "t0", "t1", 5, 1, 1, 0, 0, 10, 100, List.of());
            return report.withSnapshot(new RunReport.SnapshotInfo("auto", name + "-snap", name + "-name",
                "newest", name.equals("a")));
        };

        int exitCode = new DumpToDeltaCommand().runAllIndices(List.of("a", "b", "c"), processor, tmp);

        assertEquals(4, exitCode, "exit codes are unchanged by snapshot visibility");
        var manifest = new com.fasterxml.jackson.databind.ObjectMapper().readTree(tmp.resolve("manifest.json").toFile());
        assertEquals(1, manifest.path("behindLatestCount").asInt());
        assertEquals("a-snap", manifest.path("indices").path(0).path("snapshotUuid").asText());
        assertTrue(manifest.path("indices").path(0).path("newerSnapshotAvailable").asBoolean());
        assertFalse(manifest.path("indices").path(1).path("newerSnapshotAvailable").asBoolean());
        assertTrue(manifest.path("indices").path(2).path("snapshotUuid").isNull());
        assertTrue(manifest.path("indices").path(2).path("newerSnapshotAvailable").isNull());
    }

    @Test
    void compatibilityConstructorsLeaveSnapshotFieldsEmpty() {
        var entry = new AllIndicesManifest.IndexEntry("a", AllIndicesManifest.Outcome.COMPLETED, "a.json", null, null);
        assertNull(entry.snapshotUuid());
        assertNull(entry.newerSnapshotAvailable());
        assertNull(new RunReport("idx", "t0", "t1", 5, 2, 2, 0, 0, 100, 1000, List.of()).snapshot());
        assertEquals(0, DumpToDeltaCommand.buildManifest(List.of(entry)).behindLatestCount());
    }

    // --- --mounted-shells ---

    @Test
    void aDefaultManifestKeepsThe1_1_0Shape() throws Exception {
        var entries = List.of(
            new AllIndicesManifest.IndexEntry("a", AllIndicesManifest.Outcome.COMPLETED, "a.json", null, null, "s1", false),
            new AllIndicesManifest.IndexEntry("b", AllIndicesManifest.Outcome.FAILED, null, "IOException", "boom"));
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper()
            .enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
        String expected = """
            {
              "totalIndices" : 3,
              "completedCount" : 1,
              "completedWithQuarantineCount" : 0,
              "failedCount" : 1,
              "indices" : [ {
                "indexName" : "a",
                "outcome" : "COMPLETED",
                "reportFile" : "a.json",
                "errorClass" : null,
                "errorMessage" : null,
                "snapshotUuid" : "s1",
                "newerSnapshotAvailable" : false
              }, {
                "indexName" : "b",
                "outcome" : "FAILED",
                "reportFile" : null,
                "errorClass" : "IOException",
                "errorMessage" : "boom",
                "snapshotUuid" : null,
                "newerSnapshotAvailable" : null
              } ],
              "pendingIndices" : [ "c" ],
              "complete" : false,
              "behindLatestCount" : 0
            }""";
        assertEquals(expected, mapper.writeValueAsString(DumpToDeltaCommand.buildManifest(entries, List.of("a", "b", "c"), false))
            .replace(System.lineSeparator(), "\n"));
        String aliasMode = mapper.writeValueAsString(DumpToDeltaCommand.buildManifest(
            List.of(entries.get(0), AllIndicesManifest.IndexEntry.alias("partial-a", "a")), List.of("a", "partial-a"), true));
        assertTrue(aliasMode.contains("\"aliasOf\" : \"a\""), aliasMode);
        assertTrue(aliasMode.contains("\"aliasCount\" : 1"), aliasMode);
        assertTrue(aliasMode.contains("\"complete\" : true"), aliasMode);
    }

    @Test
    void aliasEntriesAreNeutralForTheExitCode() {
        var alias = AllIndicesManifest.IndexEntry.alias("partial-a", "a");
        var completed = new AllIndicesManifest.IndexEntry("a", AllIndicesManifest.Outcome.COMPLETED, "a.json", null, null);
        var quarantined = new AllIndicesManifest.IndexEntry("a", AllIndicesManifest.Outcome.COMPLETED_WITH_QUARANTINE, "a.json", null, null);
        var failed = new AllIndicesManifest.IndexEntry("b", AllIndicesManifest.Outcome.FAILED, null, "IOException", "boom");
        assertEquals(0, DumpToDeltaCommand.aggregateExitCode(List.of(alias, completed)));
        assertEquals(3, DumpToDeltaCommand.aggregateExitCode(List.of(alias, quarantined)));
        assertEquals(4, DumpToDeltaCommand.aggregateExitCode(List.of(alias, completed, failed)));
        assertEquals(1, DumpToDeltaCommand.buildManifest(List.of(alias, completed), List.of("partial-a", "a"), true).aliasCount());
        assertNull(DumpToDeltaCommand.buildManifest(List.of(completed)).aliasCount());
    }

    @Test
    void skipIfOriginalWithIndexIsAUsageErrorBeforeAnyRepositoryRead(@TempDir Path tmp) throws Exception {
        String missing = tmp.resolve("no-such-repository").toString();
        for (String[] args : List.of(
                new String[]{missing, "--mounted-shells", "skip-if-original", "--index", "x", "-o", tmp.resolve("t").toString()},
                new String[]{missing, "--index", "x", "-o", tmp.resolve("t").toString(), "--mounted-shells", "skip-if-original"})) {
            var out = new java.io.ByteArrayOutputStream();
            var err = new java.io.ByteArrayOutputStream();
            var cli = new CommandLine(new DumpToDeltaCommand());
            cli.setOut(new java.io.PrintWriter(out, true));
            cli.setErr(new java.io.PrintWriter(err, true));
            java.io.PrintStream previous = System.out;
            System.setOut(new java.io.PrintStream(out, true));
            int exit;
            try {
                exit = cli.execute(args);
            } finally {
                System.setOut(previous);
            }
            assertEquals(2, exit, err.toString());
            assertTrue(err.toString().contains("--mounted-shells skip-if-original requires --all-indices"), err.toString());
            assertEquals("", out.toString(), "nothing ran: not even the build line");
        }
        assertFalse(java.nio.file.Files.exists(tmp.resolve("t")));
    }

    @Test
    void mountedShellsParsesItsTwoValuesAndRejectsOthers() {
        var command = new DumpToDeltaCommand();
        new CommandLine(command).parseArgs("some-repo", "-o", "/tmp/out", "--all-indices");
        assertEquals(MountedShellsOption.Mode.EXPORT, command.mountedShells.mode);
        command = new DumpToDeltaCommand();
        new CommandLine(command).parseArgs("some-repo", "-o", "/tmp/out", "--all-indices", "--mounted-shells", "export");
        assertEquals(MountedShellsOption.Mode.EXPORT, command.mountedShells.mode);
        command = new DumpToDeltaCommand();
        new CommandLine(command).parseArgs("some-repo", "-o", "/tmp/out", "--all-indices", "--mounted-shells", "skip-if-original");
        assertTrue(command.mountedShells.skipIfOriginal());

        var err = new java.io.StringWriter();
        var cli = new CommandLine(new DumpToDeltaCommand());
        cli.setErr(new java.io.PrintWriter(err, true));
        assertEquals(2, cli.execute("some-repo", "-o", "/tmp/out", "--all-indices", "--mounted-shells", "bogus"));
        assertTrue(err.toString().contains("must be export or skip-if-original"), err.toString());
    }
}
