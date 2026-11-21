package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ilm-shell-repo is ILM-shaped: SLM and ILM searchable_snapshot share one repository (ES 9.3).
 * applogs-a/-b were snapshotted hot by SLM, then each got an ILM snapshot, was mounted as
 * partial-applogs-a/-b and deleted; a later SLM snapshot holds the shells and plain-c. Each
 * original's latest snapshot is exactly what its shell mounts, so dump-to-delta --all-indices
 * would read the same shard data twice.
 */
class ShellGroupSurveyTest {

    private static final Path ILM = Path.of("src/test/resources/fixtures/ilm-shell-repo");

    @Test
    void anIlmShapedRepositoryReportsItsExactIdentityGroups() {
        var csv = SurveyCommandTest.run(new FileSystemBlobStore(ILM), 16, "--csv");
        assertEquals(0, csv.exitCode(), csv.err());
        assertEquals(Map.of("applogs-a", "exportable", "applogs-b", "exportable", "plain-c", "exportable",
            "partial-applogs-a", "mounted(stored)", "partial-applogs-b", "mounted(stored)"), SurveyCommandTest.verdicts(csv.out()));

        Map<String, String[]> rows = csv.out().lines().skip(1).map(line -> line.split(",", -1))
            .collect(Collectors.toMap(fields -> fields[0], fields -> fields));
        for (String original : List.of("applogs-a", "applogs-b")) {
            String[] originalRow = rows.get(original);
            String[] shellRow = rows.get("partial-" + original);
            assertTrue(originalRow[6].startsWith("primary/") && originalRow[6].endsWith("/" + original), originalRow[6]);
            assertEquals(originalRow[6], shellRow[6], "same group");
            assertEquals(originalRow[4], shellRow[4], "same shard bytes");
            assertEquals(originalRow[6], shellRow[5], "the shell's backing is the group's identity");
        }
        assertEquals("", rows.get("plain-c")[6]);
        assertTrue(!rows.get("applogs-a")[6].equals(rows.get("applogs-b")[6]));

        var table = SurveyCommandTest.run(new FileSystemBlobStore(ILM), 16);
        assertTrue(table.out().contains("Exact-identity groups (same backing index, snapshot and shard count"), table.out());
        assertTrue(table.out().contains(": applogs-a, partial-applogs-a"), table.out());
        assertTrue(table.out().contains(": applogs-b, partial-applogs-b"), table.out());
        assertTrue(table.out().contains("  2 groups, "), table.out());
    }

    /**
     * frozen-repo's original has a newer snapshot than the one its shell mounts: overlapping
     * blobs, but not the same snapshot, so no group. The other fixtures have no mounts.
     */
    @Test
    void otherRepositoriesHaveNoGroups() {
        for (Path repository : List.of(SurveyCommandTest.CORE_FIXTURES.resolve("frozen-repo"),
                SurveyCommandTest.CORE_FIXTURES.resolve("reused-index-repo"),
                SurveyCommandTest.SYNTHETIC_FIXTURES.resolve("es93-repo"))) {
            var table = SurveyCommandTest.run(new FileSystemBlobStore(repository), 16);
            assertEquals(0, table.exitCode(), table.err());
            assertTrue(table.out().contains("Exact-identity groups: none"), repository + "\n" + table.out());
            var csv = SurveyCommandTest.run(new FileSystemBlobStore(repository), 16, "--csv");
            csv.out().lines().skip(1).forEach(line -> assertEquals("", line.split(",", -1)[6], line));
        }
    }
}
