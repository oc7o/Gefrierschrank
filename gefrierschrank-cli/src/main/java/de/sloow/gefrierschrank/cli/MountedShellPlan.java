package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.RepositorySources;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Which names of an {@code --all-indices} run are mounted shells of an original exported in the
 * same run, for {@code --mounted-shells skip-if-original}. Pure: it only groups what planning
 * found. A group is survey's exact identity (source, repository index ID, backing index, snapshot
 * UUID, shard count). It has a leader only when the original itself is one of its names: not
 * mounted, named like the backing index, in the primary repository. Its members are the other
 * names, all mounted shells, whose table does not exist yet; a shell that already has a table is
 * exported (resumed) as before.
 */
final class MountedShellPlan {

    static final String PRIMARY_SOURCE = "primary";

    /** {@code identity} null: the name could not be planned and is never grouped. */
    record Planned(String name, RepositorySources.Identity identity, boolean mounted, boolean tableStarted) {}

    /** The default mode: no leaders, no members, and the manifest has no alias keys. */
    static final MountedShellPlan NONE = new MountedShellPlan(false, Map.of(), Map.of(), Map.of(), List.of());

    private final boolean active;
    private final Map<String, String> leaderOf;
    private final Map<String, List<String>> membersOf;
    private final Map<String, RepositorySources.Identity> identityOf;
    private final List<String> startedShells;

    private MountedShellPlan(boolean active, Map<String, String> leaderOf, Map<String, List<String>> membersOf,
                             Map<String, RepositorySources.Identity> identityOf, List<String> startedShells) {
        this.active = active;
        this.leaderOf = leaderOf;
        this.membersOf = membersOf;
        this.identityOf = identityOf;
        this.startedShells = startedShells;
    }

    /** {@code planned} in listing order; names absent from it are ungrouped. */
    static MountedShellPlan of(List<Planned> planned) {
        Map<RepositorySources.Identity, List<Planned>> groups = new LinkedHashMap<>();
        for (Planned name : planned) {
            if (name.identity() != null) groups.computeIfAbsent(name.identity(), id -> new ArrayList<>()).add(name);
        }
        Map<String, String> leaderOf = new LinkedHashMap<>();
        Map<String, List<String>> membersOf = new LinkedHashMap<>();
        Map<String, RepositorySources.Identity> identityOf = new LinkedHashMap<>();
        List<String> started = new ArrayList<>();
        groups.forEach((identity, names) -> {
            if (names.size() < 2) return;
            List<Planned> originals = names.stream().filter(name -> isOriginal(name, identity)).toList();
            if (originals.size() != 1) return;
            String leader = originals.get(0).name();
            List<String> members = new ArrayList<>();
            for (Planned name : names) {
                if (name.name().equals(leader) || !name.mounted()) continue;
                if (name.tableStarted()) {
                    started.add(name.name());
                } else {
                    members.add(name.name());
                    leaderOf.put(name.name(), leader);
                }
            }
            if (members.isEmpty()) return;
            membersOf.put(leader, List.copyOf(members));
            identityOf.put(leader, identity);
        });
        return new MountedShellPlan(true, Map.copyOf(leaderOf), Map.copyOf(membersOf), Map.copyOf(identityOf),
            List.copyOf(started));
    }

    private static boolean isOriginal(Planned name, RepositorySources.Identity identity) {
        return !name.mounted() && name.name().equals(identity.index()) && PRIMARY_SOURCE.equals(identity.source());
    }

    /** Whether the mode is on; only then does the manifest carry {@code aliasCount}. */
    boolean active() {
        return active;
    }

    Optional<String> leaderOf(String member) {
        return Optional.ofNullable(leaderOf.get(member));
    }

    /** The members held for {@code leader}, in listing order; empty for any other name. */
    List<String> membersOf(String leader) {
        return membersOf.getOrDefault(leader, List.of());
    }

    /** The identity {@code leader} was planned to export; its members alias only if it exports exactly this. */
    RepositorySources.Identity identityOf(String leader) {
        return identityOf.get(leader);
    }

    /** Groups with at least one member. */
    int groups() {
        return membersOf.size();
    }

    int members() {
        return leaderOf.size();
    }

    /** Shells of a leader that already have a table, in listing order; they are exported. */
    List<String> startedShells() {
        return startedShells;
    }
}
