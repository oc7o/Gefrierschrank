package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.RepositorySources.Identity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** The grouping rules of --mounted-shells skip-if-original, on planned identities alone. */
class MountedShellPlanTest {

    private static final Identity X = new Identity("primary", "id-x", "x", "snap-1", 2);

    private static MountedShellPlan.Planned original(String name, Identity identity) {
        return new MountedShellPlan.Planned(name, identity, false, false);
    }

    private static MountedShellPlan.Planned shell(String name, Identity identity) {
        return new MountedShellPlan.Planned(name, identity, true, false);
    }

    @Test
    void anOriginalLeadsItsShell() {
        var plan = MountedShellPlan.of(List.of(original("x", X), shell("partial-x", X)));
        assertTrue(plan.active());
        assertEquals(Optional.of("x"), plan.leaderOf("partial-x"));
        assertEquals(Optional.empty(), plan.leaderOf("x"));
        assertEquals(List.of("partial-x"), plan.membersOf("x"));
        assertEquals(X, plan.identityOf("x"));
        assertEquals(1, plan.groups());
        assertEquals(1, plan.members());
        assertEquals(List.of(), plan.startedShells());
    }

    @Test
    void severalShellsOfOneOriginalAreAllMembersInListingOrder() {
        var plan = MountedShellPlan.of(List.of(shell("restored-x", X), original("x", X), shell("partial-x", X)));
        assertEquals(List.of("restored-x", "partial-x"), plan.membersOf("x"));
        assertEquals(2, plan.members());
        assertEquals(1, plan.groups());
    }

    @Test
    void aShellOfAShellResolvesToTheOriginalAndIsAMember() {
        var plan = MountedShellPlan.of(List.of(original("x", X), shell("partial-x", X), shell("partial-partial-x", X)));
        assertEquals(List.of("partial-x", "partial-partial-x"), plan.membersOf("x"));
    }

    @Test
    void everyIdentityComponentMustBeEqual() {
        for (Identity other : List.of(
                new Identity("frozen", "id-x", "x", "snap-1", 2),
                new Identity("primary", "id-y", "x", "snap-1", 2),
                new Identity("primary", "id-x", "y", "snap-1", 2),
                new Identity("primary", "id-x", "x", "snap-2", 2),
                new Identity("primary", "id-x", "x", "snap-1", 3))) {
            var plan = MountedShellPlan.of(List.of(original("x", X), shell("partial-x", other)));
            assertEquals(Optional.empty(), plan.leaderOf("partial-x"), other.toString());
            assertEquals(0, plan.groups(), other.toString());
        }
    }

    @Test
    void shellsWithoutTheirOriginalAllExport() {
        var plan = MountedShellPlan.of(List.of(shell("partial-x", X), shell("restored-x", X)));
        assertEquals(Optional.empty(), plan.leaderOf("partial-x"));
        assertEquals(Optional.empty(), plan.leaderOf("restored-x"));
        assertEquals(0, plan.members());
    }

    @Test
    void anOriginalInAnotherRepositoryIsNoLeader() {
        var backing = new Identity("frozen", "id-x", "x", "snap-1", 2);
        var plan = MountedShellPlan.of(List.of(original("x", backing), shell("partial-x", backing)));
        assertEquals(Optional.empty(), plan.leaderOf("partial-x"));
    }

    @Test
    void aNameOtherThanTheBackingIndexIsNoLeader() {
        var plan = MountedShellPlan.of(List.of(original("renamed", X), shell("partial-x", X)));
        assertEquals(Optional.empty(), plan.leaderOf("partial-x"));
    }

    @Test
    void aShellThatAlreadyHasATableExportsAndIsListed() {
        var started = new MountedShellPlan.Planned("partial-x", X, true, true);
        var plan = MountedShellPlan.of(List.of(original("x", X), started, shell("restored-x", X)));
        assertEquals(Optional.empty(), plan.leaderOf("partial-x"));
        assertEquals(List.of("partial-x"), plan.startedShells());
        assertEquals(List.of("restored-x"), plan.membersOf("x"));

        var onlyStarted = MountedShellPlan.of(List.of(original("x", X), started));
        assertEquals(List.of("partial-x"), onlyStarted.startedShells());
        assertEquals(0, onlyStarted.groups());
        assertEquals(List.of(), onlyStarted.membersOf("x"));
    }

    @Test
    void aNameThatCouldNotBePlannedIsNeverGrouped() {
        var plan = MountedShellPlan.of(List.of(original("x", X), new MountedShellPlan.Planned("partial-x", null, true, false)));
        assertEquals(Optional.empty(), plan.leaderOf("partial-x"));
        assertEquals(0, plan.groups());
    }

    @Test
    void shellsListedBeforeTheirOriginalAreMembersAsTheIlmFixtureLists() {
        var a = new Identity("primary", "id-a", "applogs-a", "ilm-a", 1);
        var b = new Identity("primary", "id-b", "applogs-b", "ilm-b", 1);
        var c = new Identity("primary", "id-c", "plain-c", "slm", 1);
        var plan = MountedShellPlan.of(List.of(original("plain-c", c), shell("partial-applogs-b", b),
            shell("partial-applogs-a", a), original("applogs-b", b), original("applogs-a", a)));
        assertEquals(Optional.of("applogs-a"), plan.leaderOf("partial-applogs-a"));
        assertEquals(Optional.of("applogs-b"), plan.leaderOf("partial-applogs-b"));
        assertEquals(Optional.empty(), plan.leaderOf("plain-c"));
        assertEquals(2, plan.groups());
    }

    @Test
    void noneHoldsNothingAndIsInactive() {
        assertFalse(MountedShellPlan.NONE.active());
        assertEquals(Optional.empty(), MountedShellPlan.NONE.leaderOf("x"));
        assertEquals(List.of(), MountedShellPlan.NONE.membersOf("x"));
        assertTrue(MountedShellPlan.of(List.of()).active(), "the mode is on even when nothing groups");
    }
}
