package de.sloow.gefrierschrank.syntheticsource;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * No false refusal: the rule must not refuse what Elasticsearch writes on every segment. S3 never
 * yields {@code unmapped} on any fixture segment (every segment carries {@code _primary_term},
 * segments with deletes carry {@code _tombstone}, neither is a mapper); and on the shards of the
 * coverage fixtures that are expected to commit, the per-segment rule yields no token at all.
 */
class CoverageNoFalseRefusalTest {

    /** Shards of synthcov-repo/synthcov8-repo that hold source-less segments and refuse by design. */
    static final Set<String> REFUSING_SHARDS = Set.of("cov_srcoff_text[1]", "cov_srcoff_copyto[1]", "cov_srcoff_mapping[1]",
        "cov_srcoff_dynamic[1]", "cov_srcmode_text[1]", "cov_srcmode_mapping[0]", "cov_srcmode_mapping[1]");

    @Test
    void noFixtureSegmentHoldsAFieldInfoNoMapperOwns() throws Exception {
        List<String> refused = new ArrayList<>();
        for (var segment : FixtureSegments.all()) {
            if (List.of(segment.real().split(" ")).contains("unmapped")) refused.add(segment.where() + " " + segment.unattributed());
        }
        assertEquals(List.of(), refused);
    }

    @Test
    void segmentsOfCommittingCoverageShardsYieldNoToken() throws Exception {
        List<String> refused = new ArrayList<>();
        for (var segment : FixtureSegments.all()) {
            if (!segment.repository().startsWith("synthcov")) continue;
            if (segment.hasSource()) continue;
            if (REFUSING_SHARDS.contains(segment.index() + "[" + segment.shard() + "]")) continue;
            if (!segment.real().isEmpty()) refused.add(segment.where() + ": " + segment.real());
        }
        assertEquals(List.of(), refused);
    }
}
