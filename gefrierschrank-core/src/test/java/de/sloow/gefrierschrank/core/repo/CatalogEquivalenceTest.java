package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The memoized catalog answers every lookup exactly like the per-call scans it replaced
 * ({@link LegacyRepositoryInspector}): same values, same tie-breaks and fallbacks, same exception
 * classes and messages, over every fixture repository and over hand-written index-N files with
 * equal, missing and -1 start times, dangling references and name/UUID collisions.
 */
class CatalogEquivalenceTest {

    private static final List<Path> FIXTURE_ROOTS = List.of(
        Path.of("src/test/resources/fixtures"),
        Path.of("../gefrierschrank-synthetic-source/src/test/resources/fixtures"),
        Path.of("../gefrierschrank-cli/src/test/resources/fixtures"));

    static Stream<Path> fixtureRepositories() throws IOException {
        List<Path> repositories = new ArrayList<>();
        for (Path root : FIXTURE_ROOTS) {
            try (var children = Files.list(root)) {
                children.filter(dir -> Files.isRegularFile(dir.resolve("index.latest"))).sorted().forEach(repositories::add);
            }
        }
        assertFalse(repositories.size() < 11, "fixture repositories not found: " + repositories);
        return repositories.stream();
    }

    @ParameterizedTest
    @MethodSource("fixtureRepositories")
    void fixtureRepositoriesAnswerExactlyAsBefore(Path repository) {
        assertSameAnswers(repository);
    }

    @Test
    void handWrittenCatalogsWithTiesLegacyTimesAndDanglingReferencesAnswerExactlyAsBefore(@TempDir Path repository)
            throws IOException {
        Files.write(repository.resolve("index.latest"), new byte[] {0, 0, 0, 0, 0, 0, 0, 5});
        Files.writeString(repository.resolve("index-5"), """
            {"min_version":"7.12.0","uuid":"_na_",
             "snapshots":[
               {"name":"first","uuid":"s1","state":1,"version":"7.17.9","start_time_millis":100,
                "index_metadata_lookup":{"id-tied":"lk-a"}},
               {"name":"tie-a","uuid":"s2","state":1,"version":"8.11.0","index_version":8521000,"start_time_millis":200,
                "index_metadata_lookup":{"id-tied":"lk-b","id-mixed":"lk-missing"}},
               {"name":"tie-b","uuid":"s3","state":3,"version":"8.11.0","index_version":9060000,"start_time_millis":200},
               {"name":"legacy-1","uuid":"s4","state":1,"version":"6.8.0"},
               {"name":"legacy-2","uuid":"s5","state":2,"version":"6.8.0","start_time_millis":-1},
               {"name":"s2","uuid":"s6","state":1,"version":"8.11.0","start_time_millis":50},
               {"name":"first","uuid":"s7","state":1,"version":"8.11.0","start_time_millis":300},
               {"name":"dup","uuid":"s3","state":1,"version":"9.9.9","start_time_millis":999,
                "index_metadata_lookup":{"id-tied":"lk-dup"}},
               {"name":"no-uuid","state":1}
             ],
             "index_metadata_identifiers":{"lk-a":"meta-a","lk-b":"meta-b","lk-dup":"meta-dup"},
             "indices":{
               "tied":{"id":"id-tied","snapshots":["s3","s1","s2"],"shard_generations":["g0",{"uuid":"g1"},""]},
               "legacy":{"id":"id-legacy","snapshots":["s5","s4"],"shard_generations":["g0"]},
               "mixed":{"id":"id-mixed","snapshots":["s4","s2","s6"],"shard_generations":[]},
               "dangling":{"id":"id-dangling","snapshots":["s1","ghost"],"shard_generations":["g0"]},
               "empty":{"id":"id-empty","snapshots":[],"shard_generations":["g0"]},
               "names":{"id":"id-names","snapshots":["s6","s7","s2"],"shard_generations":["g0"]},
               "dotted.ünïcode":{"id":"id-dotted","snapshots":["s7"]}
             }}
            """);
        assertSameAnswers(repository);
    }

    private static void assertSameAnswers(Path repository) {
        var legacy = new LegacyRepositoryInspector(new FileSystemBlobStore(repository));
        var current = new RepositoryInspector(new FileSystemBlobStore(repository));
        assertEquals(answers(new Legacy(legacy)), answers(new Current(current)), repository.toString());
    }

    /** The public lookups of both implementations, as one interface the comparison can drive. */
    private interface Lookups {
        String repositoryUuid() throws IOException;
        RepositoryOverview overview() throws IOException;
        IndexOverview index(String name) throws IOException;
        String latestSnapshotUuid(String name) throws IOException;
        SnapshotOverview requireSnapshot(String name, String uuid) throws IOException;
        SnapshotOverview snapshotOfIndex(String name, String nameOrUuid) throws IOException;
        Object indexMetadata(String name) throws IOException;
        Object indexMetadata(String name, String uuid) throws IOException;
        IndexMapping indexMapping(String name) throws IOException;
        ShardSnapshot shardSnapshot(String name, int shard) throws IOException;
        ShardSnapshot shardSnapshot(String name, int shard, String uuid) throws IOException;
        Set<String> reachableBlobKeys() throws IOException;
        boolean containsSnapshot(String name, String repoUuid, String uuid) throws IOException;
    }

    private record Legacy(LegacyRepositoryInspector i) implements Lookups {
        public String repositoryUuid() throws IOException { return i.repositoryUuid(); }
        public RepositoryOverview overview() throws IOException { return i.overview(); }
        public IndexOverview index(String n) throws IOException { return i.index(n); }
        public String latestSnapshotUuid(String n) throws IOException { return i.latestSnapshotUuid(n); }
        public SnapshotOverview requireSnapshot(String n, String u) throws IOException { return i.requireSnapshot(n, u); }
        public SnapshotOverview snapshotOfIndex(String n, String u) throws IOException { return i.snapshotOfIndex(n, u); }
        public Object indexMetadata(String n) throws IOException { return i.indexMetadata(n); }
        public Object indexMetadata(String n, String u) throws IOException { return i.indexMetadata(n, u); }
        public IndexMapping indexMapping(String n) throws IOException { return i.indexMapping(n); }
        public ShardSnapshot shardSnapshot(String n, int s) throws IOException { return i.shardSnapshot(n, s); }
        public ShardSnapshot shardSnapshot(String n, int s, String u) throws IOException { return i.shardSnapshot(n, s, u); }
        public Set<String> reachableBlobKeys() throws IOException { return i.reachableBlobKeys(); }

        /** RepositorySources.resolve's scan before containsSnapshot existed. */
        public boolean containsSnapshot(String n, String r, String u) throws IOException {
            return i.overview().indices().stream().anyMatch(x -> x.name().equals(n)
                && x.repoUuid().equals(r) && x.snapshotUuids().contains(u));
        }
    }

    private record Current(RepositoryInspector i) implements Lookups {
        public String repositoryUuid() throws IOException { return i.repositoryUuid(); }
        public RepositoryOverview overview() throws IOException { return i.overview(); }
        public IndexOverview index(String n) throws IOException { return i.index(n); }
        public String latestSnapshotUuid(String n) throws IOException { return i.latestSnapshotUuid(n); }
        public SnapshotOverview requireSnapshot(String n, String u) throws IOException { return i.requireSnapshot(n, u); }
        public SnapshotOverview snapshotOfIndex(String n, String u) throws IOException { return i.snapshotOfIndex(n, u); }
        public Object indexMetadata(String n) throws IOException { return i.indexMetadata(n); }
        public Object indexMetadata(String n, String u) throws IOException { return i.indexMetadata(n, u); }
        public IndexMapping indexMapping(String n) throws IOException { return i.indexMapping(n); }
        public ShardSnapshot shardSnapshot(String n, int s) throws IOException { return i.shardSnapshot(n, s); }
        public ShardSnapshot shardSnapshot(String n, int s, String u) throws IOException { return i.shardSnapshot(n, s, u); }
        public Set<String> reachableBlobKeys() throws IOException { return i.reachableBlobKeys(); }
        public boolean containsSnapshot(String n, String r, String u) throws IOException { return i.containsSnapshot(n, r, u); }
    }

    private interface Call {
        Object run() throws Exception;
    }

    /** Every lookup over every index and snapshot the catalog names, plus unknown ones. */
    private static List<String> answers(Lookups lookups) {
        List<String> out = new ArrayList<>();
        record(out, "repositoryUuid", lookups::repositoryUuid);
        record(out, "overview", () -> normalized(lookups.overview()));
        record(out, "reachableBlobKeys", () -> new TreeSet<>(lookups.reachableBlobKeys()));

        Set<String> names = new LinkedHashSet<>();
        Set<String> snapshotTokens = new LinkedHashSet<>();
        Set<String> repoUuids = new LinkedHashSet<>();
        try {
            RepositoryOverview overview = lookups.overview();
            overview.indices().forEach(i -> {
                names.add(i.name());
                repoUuids.add(i.repoUuid());
                snapshotTokens.addAll(i.snapshotUuids());
            });
            overview.snapshots().forEach(s -> {
                snapshotTokens.add(s.uuid());
                snapshotTokens.add(s.name());
            });
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        names.add("not-an-index");
        snapshotTokens.add("nope");
        snapshotTokens.add("");
        repoUuids.add("not-a-repo-uuid");

        for (String name : names) {
            record(out, "index " + name, () -> lookups.index(name));
            record(out, "latestSnapshotUuid " + name, () -> lookups.latestSnapshotUuid(name));
            record(out, "indexMetadata " + name, () -> lookups.indexMetadata(name));
            record(out, "indexMapping " + name, () -> lookups.indexMapping(name).properties());
            record(out, "shardSnapshot " + name + " 0", () -> lookups.shardSnapshot(name, 0));
            record(out, "shardSnapshot " + name + " -1", () -> lookups.shardSnapshot(name, -1));
            for (String token : snapshotTokens) {
                record(out, "requireSnapshot " + name + " " + token, () -> normalized(lookups.requireSnapshot(name, token)));
                record(out, "snapshotOfIndex " + name + " " + token, () -> normalized(lookups.snapshotOfIndex(name, token)));
                record(out, "indexMetadata " + name + " " + token, () -> lookups.indexMetadata(name, token));
                for (int shard = -1; shard <= 2; shard++) {
                    int shardId = shard;
                    record(out, "shardSnapshot " + name + " " + shardId + " " + token,
                        () -> lookups.shardSnapshot(name, shardId, token));
                }
                for (String repoUuid : repoUuids) {
                    record(out, "containsSnapshot " + name + " " + repoUuid + " " + token,
                        () -> lookups.containsSnapshot(name, repoUuid, token));
                }
            }
        }
        return out;
    }

    private static void record(List<String> out, String label, Call call) {
        try {
            out.add(label + " = " + call.run());
        } catch (Exception e) {
            out.add(label + " ! " + e.getClass().getName() + ": " + e.getMessage());
        }
    }

    /** The oracle predates SnapshotOverview.indexVersion; compare the components both carry. */
    private static String normalized(SnapshotOverview s) {
        return String.join("|", s.name(), s.uuid(), String.valueOf(s.state()), s.version(),
            String.valueOf(s.startTimeMillis()));
    }

    private static String normalized(RepositoryOverview o) {
        return o.latestGeneration() + " " + o.minVersion() + " "
            + o.snapshots().stream().map(CatalogEquivalenceTest::normalized).toList() + " " + o.indices();
    }
}
