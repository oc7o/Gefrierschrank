package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.RepositorySources;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Admission order of an {@code --all-indices} run: listing order, except that the members of a
 * {@link MountedShellPlan} group wait for their leader's result. A leader is never held, so
 * the schedule cannot deadlock; with {@link MountedShellPlan#NONE} it is exactly listing order.
 */
final class IndexSchedule {

    /** A held member's fate once its leader's entry is recorded: an alias, or admitted next. */
    record Decision(int position, String name, boolean alias, String aliasOf) {}

    private final MountedShellPlan shells;
    private final Map<String, Integer> positions = new HashMap<>();
    private final Deque<Integer> admissible = new ArrayDeque<>();

    IndexSchedule(List<String> planned, MountedShellPlan shells) {
        this.shells = shells;
        for (int position = 0; position < planned.size(); position++) {
            String name = planned.get(position);
            positions.put(name, position);
            if (shells.leaderOf(name).isEmpty()) admissible.addLast(position);
        }
    }

    boolean hasNext() {
        return !admissible.isEmpty();
    }

    /** Listing position of the next name to start; removes it from the schedule. */
    int next() {
        return admissible.removeFirst();
    }

    /**
     * Decides {@code leader}'s members on the reporting thread, once the leader's entry is
     * recorded. They are aliases when the leader completed (quarantined shards included) and
     * exported exactly the planned identity; otherwise they go to the front of the schedule, in
     * listing order, and start next.
     */
    List<Decision> release(String leader, AllIndicesManifest.Outcome outcome, RepositorySources.Identity exported) {
        List<String> members = shells.membersOf(leader);
        if (members.isEmpty()) return List.of();
        boolean alias = (outcome == AllIndicesManifest.Outcome.COMPLETED
            || outcome == AllIndicesManifest.Outcome.COMPLETED_WITH_QUARANTINE)
            && exported != null && exported.equals(shells.identityOf(leader));
        List<Decision> decisions = new ArrayList<>(members.size());
        for (String member : members) decisions.add(new Decision(positions.get(member), member, alias, alias ? leader : null));
        if (!alias) {
            for (int i = decisions.size() - 1; i >= 0; i--) admissible.addFirst(decisions.get(i).position());
        }
        return decisions;
    }
}
