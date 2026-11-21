package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.cli.AllIndicesManifest.Outcome;
import de.sloow.gefrierschrank.core.repo.RepositorySources.Identity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IndexScheduleTest {

    private static final Identity X = new Identity("primary", "id-x", "x", "snap-1", 1);
    private static final List<String> NAMES = List.of("partial-x", "a", "restored-x", "x", "b");

    private static MountedShellPlan shells() {
        return MountedShellPlan.of(List.of(new MountedShellPlan.Planned("partial-x", X, true, false),
            new MountedShellPlan.Planned("restored-x", X, true, false), new MountedShellPlan.Planned("x", X, false, false)));
    }

    private static List<String> drain(IndexSchedule schedule) {
        var order = new ArrayList<String>();
        while (schedule.hasNext()) order.add(NAMES.get(schedule.next()));
        return order;
    }

    @Test
    void withoutShellsTheScheduleIsTheListingOrder() {
        var schedule = new IndexSchedule(NAMES, MountedShellPlan.NONE);
        assertEquals(NAMES, drain(schedule));
        assertEquals(List.of(), schedule.release("x", Outcome.COMPLETED, X));
        assertFalse(schedule.hasNext());
    }

    @Test
    void membersAreNeverAdmittedBeforeTheirLeadersRelease() {
        var schedule = new IndexSchedule(NAMES, shells());
        assertEquals(List.of("a", "x", "b"), drain(schedule));
    }

    @Test
    void aCompletedOrQuarantinedLeaderWithThePlannedIdentityMakesAliases() {
        for (Outcome outcome : List.of(Outcome.COMPLETED, Outcome.COMPLETED_WITH_QUARANTINE)) {
            var schedule = new IndexSchedule(NAMES, shells());
            drain(schedule);
            assertEquals(List.of(new IndexSchedule.Decision(0, "partial-x", true, "x"),
                new IndexSchedule.Decision(2, "restored-x", true, "x")), schedule.release("x", outcome, X), outcome.name());
            assertFalse(schedule.hasNext(), "aliases are never admitted");
        }
    }

    @Test
    void aFailedLeaderANullOrADifferentIdentityAdmitsTheMembersNextInListingOrder() {
        var drifted = new Identity("primary", "id-x", "x", "snap-2", 1);
        record Case(Outcome outcome, Identity exported) {}
        for (var c : List.of(new Case(Outcome.FAILED, null), new Case(Outcome.FAILED, X),
                new Case(Outcome.COMPLETED, null), new Case(Outcome.COMPLETED, drifted))) {
            var schedule = new IndexSchedule(NAMES, shells());
            assertEquals(1, schedule.next());
            assertEquals(3, schedule.next(), "the leader starts in its listing position");
            var decisions = schedule.release("x", c.outcome(), c.exported());
            assertTrue(decisions.stream().noneMatch(IndexSchedule.Decision::alias), c.toString());
            assertEquals(List.of("partial-x", "restored-x", "b"), drain(schedule), c.toString());
        }
    }
}
