package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.IndexOverview;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Every segment of every shard of the fixture repositories, with the per-segment coverage refusals
 * on the real leaf and on the FieldInfos-only leaf (isolated CoverageSegmentsProbe). Computed once
 * per JVM; repositories that do not exist (yet) are skipped.
 */
final class FixtureSegments {

    static final List<String> REPOSITORIES = List.of("synth-repo", "synth-deletes-repo", "es93-repo", "zstd-synth-repo",
        "synthcov-repo", "synthcov8-repo");

    record Segment(String repository, String index, int shard, String name, boolean hasSource, String real, String stub,
                   String unattributed) {
        String where() {
            return repository + "/" + index + "[" + shard + "] " + name;
        }
    }

    private static final ObjectMapper JSON = new ObjectMapper();
    private static List<Segment> segments;

    private FixtureSegments() {}

    static synchronized List<Segment> all() throws Exception {
        if (segments != null) return segments;
        Path tmp = Files.createTempDirectory("fixture-segments");
        List<Segment> found = new ArrayList<>();
        for (String repository : REPOSITORIES) {
            Path repo = Path.of("src/test/resources/fixtures", repository);
            if (!Files.isDirectory(repo)) continue;
            var store = new FileSystemBlobStore(repo);
            var inspector = new RepositoryInspector(store);
            for (IndexOverview index : inspector.overview().indices()) {
                JsonNode metadata = inspector.indexMetadata(index.name());
                for (int shard = 0; shard < index.shardGenerations().size(); shard++) {
                    Path shardDir = tmp.resolve(repository).resolve(index.name()).resolve(Integer.toString(shard));
                    Files.createDirectories(shardDir.getParent());
                    new ShardMaterializer(store).materialize(inspector.shardSnapshot(index.name(), shard), shardDir);
                    String json = probe(shardDir, SyntheticSourceReconstructor.extractMappingJson(metadata),
                        SyntheticSourceReconstructor.extractFlatSettings(metadata));
                    for (JsonNode segment : JSON.readTree(json)) {
                        found.add(new Segment(repository, index.name(), shard, segment.path("segment").asText(),
                            segment.path("hasSource").asBoolean(), segment.path("real").asText(), segment.path("stub").asText(),
                            segment.path("unattributed").asText()));
                    }
                }
            }
        }
        segments = List.copyOf(found);
        return segments;
    }

    private static String probe(Path shardDir, String mapping, Map<String, String> settings) throws Exception {
        IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            return (String) Class.forName("de.sloow.gefrierschrank.syntheticsource.isolated.CoverageSegmentsProbe", true, loader)
                .getMethod("refusalsPerSegment", String.class, String.class, Map.class)
                .invoke(null, shardDir.toString(), mapping, settings);
        } catch (InvocationTargetException e) {
            throw e.getTargetException() instanceof Exception cause ? cause : e;
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
