package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.CommitPoint;
import de.sloow.gefrierschrank.syntheticsource.SyntheticCoverage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;

/**
 * The residual commit-point preflight: after only the commit files of a shard are downloaded,
 * fails the shard that reconstruction would refuse, with the failure reconstruction would throw,
 * instead of after the whole shard. A segment that mixes stored and source-less documents carries
 * {@code _source} in its FieldInfos and is not checked here; it fails, if at all, at read time.
 */
final class CommitPointPreflight {

    /** The reconstruction coverage check of the live source-less segments of a shard. */
    @FunctionalInterface
    interface CoverageCheck {
        void check(Path shardDir, String indexName, JsonNode metadata, Set<String> sourcelessLiveSegments) throws IOException;
    }

    /** The reconstruction's own coverage predicate; a seam so tests can stand in a refusal. */
    static volatile CoverageCheck coverage = SyntheticCoverage::requireReconstructable;

    private CommitPointPreflight() {}

    static void require(CommitPoint commit, JsonNode metadata, String indexName, Path shardDir) throws IOException {
        if (metadata == null) return;
        Set<String> sourceless = new TreeSet<>();
        for (CommitPoint.Segment segment : commit.segments()) {
            if (segment.liveDocs() > 0 && !segment.hasSource()) sourceless.add(segment.name());
        }
        if (!sourceless.isEmpty()) coverage.check(shardDir, indexName, metadata, Set.copyOf(sourceless));
    }
}
