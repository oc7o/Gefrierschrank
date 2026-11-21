package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * End-to-end against REAL snapshot repository fixtures generated from a live ES 9.3.0 container
 * (see {@code src/test/resources/fixtures/synth-repo}, produced the same way
 * {@code gefrierschrank-core}'s {@code demo-repo} fixture is - real {@code _bulk} indexing, real
 * {@code _snapshot}, pulled off a real MinIO-backed repository, NOT hand-built segments).
 *
 * <p>{@code test_plain_synth}: a non-TSDB index with {@code index.mapping.source.mode: synthetic}
 * (requires a trial/platinum license on a real cluster; the fixture already reflects that, so no
 * license is needed to READ it here) - stock Lucene doc values, no ES-proprietary codec involved.
 * Its mapping also carries a {@code price} field of type {@code scaled_float} (scaling factor
 * 100) - added specifically to exercise {@code ScaledFloatFieldMapper}, which lives in ES's
 * {@code mapper-extras} module (see {@code org.elasticsearch.index.mapper.extras.package-info})
 * and is the field type whose absence from a bare {@code IndicesModule(List.of())} makes
 * reconstruction fail with {@code IllegalArgumentException: No mapper found for type
 * [scaled_float]}; the module's {@code MapperExtrasPlugin} wiring fixes that.
 *
 * <p>{@code test_tsdb_synth}: a real time-series index. {@code index.mode: time_series} defaults
 * {@code index.mapping.source.mode} to synthetic even with no explicit setting (verified
 * separately) and ES actually selects its proprietary {@code ES819TSDBDocValuesFormat} for the
 * metric/dimension fields (confirmed via {@code FieldInfo.attributes()} on the real segment) -
 * this is the case the whole module exists for.
 *
 * <p>{@code test_plain_synth}'s mapping was later extended (same fixture, same 50 documents - an
 * ordinary {@code _update} bulk against every existing doc, not a re-index, so doc count/ids are
 * unchanged) with four more fields exercising vendored, EL2.0-only-licensed x-pack mappers
 * (see {@code org.elasticsearch.xpack.aggregatemetric.mapper.package-info} etc.):
 * {@code agg_latency} ({@code aggregate_metric_double}, metrics min/max/sum/value_count, default
 * metric max), {@code resp_sizes} ({@code histogram}), {@code service_kind}
 * ({@code constant_keyword}, fixed value {@code "web"}), {@code search_tags} ({@code wildcard}).
 * Mapper-extras does not provide these four field types, and without the vendored mappers this
 * module's standalone {@code MapperRegistry} has none of them (checked directly via
 * {@code MapperRegistry.getMapperParser(type, IndexVersion.current())} on the built fat jar).
 */
class SyntheticSourceReconstructorTest {

    private static final Path FIXTURE_REPO =
        Path.of("src/test/resources/fixtures/synth-repo");
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void reconstructsSourceFromStockLuceneDocValues(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        String indexName = "test_plain_synth";

        ShardSnapshot shard = inspector.shardSnapshot(indexName, 0);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(shard, shardDir);

        JsonNode rawMetadata = inspector.indexMetadata(indexName);

        try (SyntheticSourceReconstructor reconstructor =
                 SyntheticSourceReconstructor.open(shardDir, indexName, rawMetadata)) {
            assertEquals(50, reconstructor.docCount());

            Set<Integer> seenIds = new HashSet<>();
            for (int docId = 0; docId < reconstructor.docCount(); docId++) {
                String json = reconstructor.readSource(docId);
                JsonNode parsed = JSON.readTree(json);
                assertTrue(parsed.has("id"), "reconstructed doc " + docId + " missing 'id': " + json);
                assertTrue(parsed.has("name"), "reconstructed doc " + docId + " missing 'name': " + json);
                assertTrue(parsed.has("score"));
                assertTrue(parsed.has("count"));
                assertTrue(parsed.has("active"));
                assertTrue(parsed.has("created"));
                assertTrue(parsed.has("price"), "reconstructed doc " + docId + " missing 'price' (scaled_float): " + json);
                assertTrue(parsed.has("agg_latency"), "reconstructed doc " + docId + " missing 'agg_latency' (aggregate_metric_double): " + json);
                assertTrue(parsed.has("resp_sizes"), "reconstructed doc " + docId + " missing 'resp_sizes' (histogram): " + json);
                assertTrue(parsed.has("service_kind"), "reconstructed doc " + docId + " missing 'service_kind' (constant_keyword): " + json);
                assertTrue(parsed.has("search_tags"), "reconstructed doc " + docId + " missing 'search_tags' (wildcard): " + json);
                assertTrue(parsed.has("app_version"), "reconstructed doc " + docId + " missing 'app_version' (version): " + json);
                assertTrue(parsed.has("tag_counts"), "reconstructed doc " + docId + " missing 'tag_counts' (counted_keyword): " + json);
                assertTrue(parsed.has("session_id"), "reconstructed doc " + docId + " missing 'session_id' (unsigned_long): " + json);
                assertTrue(parsed.has("bounds"), "reconstructed doc " + docId + " missing 'bounds' (shape): " + json);
                assertTrue(parsed.has("location"), "reconstructed doc " + docId + " missing 'location' (geo_shape): " + json);
                assertTrue(parsed.get("name").asText().startsWith("name-"),
                    "unexpected name field: " + json);
                seenIds.add(parsed.get("id").asInt());
            }
            assertEquals(50, seenIds.size(), "every doc's 'id' field must be distinct and reconstructed correctly");
        }
    }

    /**
     * Isolates {@code scaled_float} reconstruction specifically: this is the exact field type
     * ({@code ScaledFloatFieldMapper}, from ES's {@code mapper-extras} module - see
     * {@code org.elasticsearch.index.mapper.extras.package-info}) whose absence from this
     * module's standalone {@code MapperService} makes reconstruction fail with
     * {@code IllegalArgumentException: No mapper found for type [scaled_float]}. Checks the
     * reconstructed value is a plausible decoding (rounded to the field's scaling factor of 100,
     * i.e. two decimal places, not raw long doc-values or some other garbage), not merely present.
     */
    @Test
    void reconstructsScaledFloatFieldCorrectly(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        String indexName = "test_plain_synth";

        ShardSnapshot shard = inspector.shardSnapshot(indexName, 0);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(shard, shardDir);

        JsonNode rawMetadata = inspector.indexMetadata(indexName);

        try (SyntheticSourceReconstructor reconstructor =
                 SyntheticSourceReconstructor.open(shardDir, indexName, rawMetadata)) {
            assertEquals(50, reconstructor.docCount());

            int nonZeroPrices = 0;
            for (int docId = 0; docId < reconstructor.docCount(); docId++) {
                String json = reconstructor.readSource(docId);
                JsonNode parsed = JSON.readTree(json);
                assertTrue(parsed.has("price"), "reconstructed doc " + docId + " missing 'price': " + json);
                double price = parsed.get("price").asDouble();
                assertTrue(price >= 0, "unexpected negative price: " + json);
                // scaling_factor is 100: the mapper stores round(value * 100) as a long doc
                // value and divides back by 100.0 on read - so every reconstructed value must
                // land on an exact 1/100 boundary (within floating-point rounding), not some
                // arbitrary double.
                double scaledBack = Math.round(price * 100.0) / 100.0;
                assertEquals(scaledBack, price, 1e-9,
                    "price not on a 1/100 (scaling_factor) boundary: " + price + " in doc " + json);
                if (price > 0) {
                    nonZeroPrices++;
                }
            }
            assertTrue(nonZeroPrices > 0, "expected at least some non-zero reconstructed price values");
        }
    }

    /**
     * Isolates the vendored, EL2.0-only-licensed x-pack mappers for
     * {@code aggregate_metric_double}, {@code histogram}, {@code constant_keyword}, and
     * {@code wildcard} (see {@code org.elasticsearch.xpack.aggregatemetric.mapper.package-info}
     * and its sibling packages' own package-infos for exact provenance/license) the same way
     * {@link #reconstructsScaledFloatFieldCorrectly} isolates {@code scaled_float}: checks
     * reconstructed values are plausible decodings of the real values indexed via the fixture's
     * regeneration script, not merely present. Mapper-extras ({@code scaled_float} etc.) does not
     * provide these four field types, and without the vendored mappers this module's standalone
     * {@code MapperRegistry} has none of them.
     */
    @Test
    void reconstructsAggregateMetricHistogramConstantKeywordAndWildcardCorrectly(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        String indexName = "test_plain_synth";

        ShardSnapshot shard = inspector.shardSnapshot(indexName, 0);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(shard, shardDir);

        JsonNode rawMetadata = inspector.indexMetadata(indexName);

        try (SyntheticSourceReconstructor reconstructor =
                 SyntheticSourceReconstructor.open(shardDir, indexName, rawMetadata)) {
            assertEquals(50, reconstructor.docCount());

            for (int docId = 0; docId < reconstructor.docCount(); docId++) {
                String json = reconstructor.readSource(docId);
                JsonNode parsed = JSON.readTree(json);

                // aggregate_metric_double: fixture indexed min <= max, and sum/value_count as
                // exact integers/doubles - a subtly wrong metric-subfield decode (e.g. reading
                // sum's doc value into min, or truncating value_count) would fail these bounds.
                JsonNode agg = parsed.get("agg_latency");
                assertTrue(agg != null && agg.has("min") && agg.has("max") && agg.has("sum") && agg.has("value_count"),
                    "reconstructed doc " + docId + " missing an agg_latency (aggregate_metric_double) metric subfield: " + json);
                double min = agg.get("min").asDouble();
                double max = agg.get("max").asDouble();
                long valueCount = agg.get("value_count").asLong();
                assertTrue(min <= max, "aggregate_metric_double min > max in doc " + docId + ": " + json);
                assertTrue(valueCount >= 1 && valueCount <= 500,
                    "aggregate_metric_double value_count out of the fixture's generated range in doc " + docId + ": " + json);

                // histogram: fixture indexed the fixed bucket boundaries [64,128,256,512,1024]
                // with per-doc random counts - a wrong doc-values decode would show different
                // bucket values or a mismatched values/counts array length.
                JsonNode hist = parsed.get("resp_sizes");
                assertTrue(hist != null && hist.has("values") && hist.has("counts"),
                    "reconstructed doc " + docId + " missing resp_sizes (histogram) values/counts: " + json);
                assertEquals(5, hist.get("values").size(), "histogram bucket count mismatch in doc " + docId + ": " + json);
                assertEquals(5, hist.get("counts").size(), "histogram count-array length mismatch in doc " + docId + ": " + json);
                double[] expectedBuckets = {64.0, 128.0, 256.0, 512.0, 1024.0};
                for (int b = 0; b < expectedBuckets.length; b++) {
                    assertEquals(expectedBuckets[b], hist.get("values").get(b).asDouble(), 1e-9,
                        "histogram bucket " + b + " boundary wrong in doc " + docId + ": " + json);
                }

                // constant_keyword: every doc must reconstruct the exact fixed value "web" - a
                // constant_keyword field stores no per-document doc values at all (it's encoded
                // purely in the mapping), so this specifically checks the mapper supplies the
                // mapping-level constant on every doc, not merely that it doesn't crash.
                assertTrue(parsed.has("service_kind"), "reconstructed doc " + docId + " missing service_kind (constant_keyword): " + json);
                assertEquals("web", parsed.get("service_kind").asText(),
                    "constant_keyword value wrong in doc " + docId + ": " + json);

                // wildcard: fixture indexed a distinctive per-doc pattern containing literal
                // wildcard/ngram-relevant characters ('*') - checks the reconstructed string is
                // byte-for-byte what was indexed, not e.g. an ngram-tokenized or truncated
                // representation.
                assertTrue(parsed.has("search_tags"), "reconstructed doc " + docId + " missing search_tags (wildcard): " + json);
                String tag = parsed.get("search_tags").asText();
                assertTrue(tag.startsWith("tag-" + String.format("%04d", docId) + "-"),
                    "wildcard value doesn't match the fixture's per-doc pattern in doc " + docId + ": " + json);
                assertTrue(tag.contains("-*-search*wildcard*"),
                    "wildcard value lost its literal '*' characters in doc " + docId + ": " + json);
            }
        }
    }

    /**
     * Isolates the vendored x-pack mappers for {@code version}
     * ({@link org.elasticsearch.xpack.versionfield.VersionStringFieldMapper}, from x-pack's
     * {@code mapper-version} plugin), {@code counted_keyword}
     * ({@link org.elasticsearch.xpack.countedkeyword.CountedKeywordFieldMapper}, from
     * {@code mapper-counted-keyword}), {@code unsigned_long}
     * ({@link org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper}, from
     * {@code mapper-unsigned-long}), {@code shape}
     * ({@link org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper}, Cartesian) and
     * {@code geo_shape}
     * ({@link org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper},
     * lon/lat) - see those packages' own {@code package-info.java} for exact provenance/license.
     * Without the vendored mappers this module's standalone {@code MapperRegistry} has none of these
     * five field types. {@code semantic_text} (also missing, ML-inference-backed) is intentionally
     * out of scope here.
     *
     * <p>Fixture values are deterministic (not random) so this test recomputes and checks the
     * EXACT expected value per doc id - the same generation formulas used when the fixture's
     * {@code test_plain_synth} index was extended via {@code _update} (see
     * {@code scripts/generate_test_snapshots.py}) - not merely that the fields are present:
     * <ul>
     *   <li>{@code app_version}: cycles through a fixed list of semver-ish strings by
     *       {@code docId % 10}, including pre-release ({@code -rc1}, {@code -beta},
     *       {@code -alpha.1}) and build-metadata ({@code +build.5}) suffixes - checks the
     *       version STRING round-trips exactly (a version field's {@code VersionEncoder} does a
     *       non-trivial custom binary encoding for semver sort order; a subtly wrong decode
     *       could still produce a plausible-looking but wrong string).</li>
     *   <li>{@code tag_counts}: per-doc repeated values - checks the exact per-KEYWORD count
     *       (not just which keywords are present), including the single-value case where
     *       {@code CountedKeywordFieldMapper} emits a scalar instead of a one-element array.</li>
     *   <li>{@code session_id}: half the docs (even ids) use values strictly greater than
     *       {@code Long.MAX_VALUE} (e.g. doc 0 = 9223372036854775808, i.e. 2^63, which reads as
     *       a NEGATIVE number if the mapper's shifted-signed-long storage trick is decoded
     *       incorrectly) - this is exactly the bug {@code unsigned_long} exists to prevent, and
     *       the odd-id docs additionally check ordinary small values still decode correctly.</li>
     *   <li>{@code bounds} (Cartesian {@code shape}) and {@code location} (lon/lat
     *       {@code geo_shape}): alternate between {@code POINT} and {@code POLYGON} WKT per doc
     *       - checks the exact coordinates round-trip (not merely "looks like WKT"), which would
     *       catch e.g. a lon/lat swap or an off-by-one in the polygon ring.</li>
     * </ul>
     */
    @Test
    void reconstructsVersionCountedKeywordUnsignedLongAndShapesCorrectly(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        String indexName = "test_plain_synth";

        ShardSnapshot shard = inspector.shardSnapshot(indexName, 0);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(shard, shardDir);

        JsonNode rawMetadata = inspector.indexMetadata(indexName);

        String[] versions = {
            "1.0.0", "1.2.3", "2.0.0-rc1", "2.1.0", "2.4.1-beta",
            "0.9.5", "3.0.0", "10.2.1", "2.1.0-alpha.1", "1.0.0+build.5"
        };
        String[] tagPool = {"red", "green", "blue", "alpha", "beta"};

        try (SyntheticSourceReconstructor reconstructor =
                 SyntheticSourceReconstructor.open(shardDir, indexName, rawMetadata)) {
            assertEquals(50, reconstructor.docCount());

            int docsWithSessionIdAboveSignedMax = 0;

            for (int docId = 0; docId < reconstructor.docCount(); docId++) {
                String json = reconstructor.readSource(docId);
                JsonNode parsed = JSON.readTree(json);

                // version: exact semver-ish string round-trip.
                assertTrue(parsed.has("app_version"), "reconstructed doc " + docId + " missing app_version (version): " + json);
                String expectedVersion = versions[docId % versions.length];
                assertEquals(expectedVersion, parsed.get("app_version").asText(),
                    "version value wrong in doc " + docId + ": " + json);

                // counted_keyword: exact per-keyword count, matching the fixture's generation
                // formula (num_distinct = 1 + docId % 3 keywords, each repeated 1 + (docId+j) % 3
                // times).
                assertTrue(parsed.has("tag_counts"), "reconstructed doc " + docId + " missing tag_counts (counted_keyword): " + json);
                Map<String, Integer> expectedCounts = new HashMap<>();
                int numDistinct = 1 + (docId % 3);
                for (int j = 0; j < numDistinct; j++) {
                    String tag = tagPool[(docId + j) % tagPool.length];
                    int repeat = 1 + ((docId + j) % 3);
                    expectedCounts.merge(tag, repeat, Integer::sum);
                }
                Map<String, Integer> actualCounts = new HashMap<>();
                JsonNode tagCountsNode = parsed.get("tag_counts");
                if (tagCountsNode.isArray()) {
                    for (JsonNode tagNode : tagCountsNode) {
                        actualCounts.merge(tagNode.asText(), 1, Integer::sum);
                    }
                } else {
                    actualCounts.merge(tagCountsNode.asText(), 1, Integer::sum);
                }
                assertEquals(expectedCounts, actualCounts,
                    "counted_keyword per-value counts wrong in doc " + docId + ": " + json);

                // unsigned_long: exact value, deliberately including values > Long.MAX_VALUE.
                assertTrue(parsed.has("session_id"), "reconstructed doc " + docId + " missing session_id (unsigned_long): " + json);
                java.math.BigInteger expectedSessionId;
                if (docId % 2 == 0) {
                    expectedSessionId = java.math.BigInteger.valueOf(Long.MAX_VALUE)
                        .add(java.math.BigInteger.ONE)
                        .add(java.math.BigInteger.valueOf(1_000_000_000_000L).multiply(java.math.BigInteger.valueOf(docId)));
                } else {
                    expectedSessionId = java.math.BigInteger.valueOf(docId * 12345L);
                }
                java.math.BigInteger actualSessionId = new java.math.BigInteger(parsed.get("session_id").asText());
                assertEquals(expectedSessionId, actualSessionId,
                    "unsigned_long value wrong in doc " + docId + ": " + json);
                if (expectedSessionId.compareTo(java.math.BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
                    docsWithSessionIdAboveSignedMax++;
                    // The whole point of unsigned_long: this value would be NEGATIVE if the
                    // mapper's shifted-signed-long storage trick were decoded incorrectly.
                    assertTrue(actualSessionId.signum() > 0,
                        "unsigned_long value above Long.MAX_VALUE decoded as non-positive (sign bug) in doc "
                            + docId + ": " + json);
                }

                // shape (Cartesian): exact WKT round-trip, alternating polygon/point per doc.
                assertTrue(parsed.has("bounds"), "reconstructed doc " + docId + " missing bounds (shape): " + json);
                String bounds = parsed.get("bounds").asText();
                if (docId % 2 == 0) {
                    String expectedBounds = String.format(
                        "POLYGON ((%d %d, %d %d, %d %d, %d %d, %d %d))",
                        docId, docId, docId + 10, docId, docId + 10, docId + 10, docId, docId + 10, docId, docId);
                    assertEquals(expectedBounds, bounds, "shape polygon wrong in doc " + docId + ": " + json);
                } else {
                    String expectedBounds = String.format("POINT (%s %s)",
                        formatCoord(docId * 1.5), formatCoord(docId * 2.5));
                    assertEquals(expectedBounds, bounds, "shape point wrong in doc " + docId + ": " + json);
                }

                // geo_shape (lon/lat): exact WKT round-trip, alternating point/polygon per doc.
                assertTrue(parsed.has("location"), "reconstructed doc " + docId + " missing location (geo_shape): " + json);
                String location = parsed.get("location").asText();
                int lon = -170 + (docId * 6) % 340;
                int lat = -80 + (docId * 3) % 160;
                if (docId % 3 == 0) {
                    String expectedLocation = String.format("POINT (%d %d)", lon, lat);
                    assertEquals(expectedLocation, location, "geo_shape point wrong in doc " + docId + ": " + json);
                } else {
                    String expectedLocation = String.format(
                        "POLYGON ((%d %d, %d %d, %d %d, %d %d, %d %d))",
                        lon, lat, lon + 1, lat, lon + 1, lat + 1, lon, lat + 1, lon, lat);
                    assertEquals(expectedLocation, location, "geo_shape polygon wrong in doc " + docId + ": " + json);
                }
            }

            assertTrue(docsWithSessionIdAboveSignedMax > 0,
                "expected at least some unsigned_long fixture values above Long.MAX_VALUE - "
                    + "otherwise this test would never exercise the signed/unsigned decode bug");
        }
    }

    @Test
    void rejectsUnsupportedSyntheticFieldsInsteadOfSilentlyDroppingThem(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        String indexName = "test_plain_synth";
        ShardSnapshot shard = inspector.shardSnapshot(indexName, 0);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(shard, shardDir);

        ObjectNode metadata = JSON.createObjectNode();
        metadata.set("settings", inspector.indexMetadata(indexName).path("settings"));
        ObjectNode mapping = (ObjectNode) JSON.readTree(
            SyntheticSourceReconstructor.extractMappingJson(inspector.indexMetadata(indexName)));
        mapping.withObject("properties").putObject("semantic_summary").put("type", "semantic_text");
        metadata.set("mappings", mapping);

        assertThrows(SyntheticSourceReconstructor.UnsupportedSyntheticFieldException.class,
            () -> SyntheticSourceReconstructor.open(shardDir, indexName, metadata));
    }

    /**
     * A {@code text} or {@code match_only_text} field needs an analyzer, and a standalone
     * MapperService built with a null IndexAnalyzers fails ES's own requireNonNull on the analyzer
     * with a message-less NullPointerException. Reconstruction must supply analyzers, so that such
     * a field cannot fail a shard whose other fields are all reconstructable.
     */
    @Test
    void reconstructsWhenTheMappingHasAnalyzedTextFields(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        String indexName = "test_plain_synth";
        ShardSnapshot shard = inspector.shardSnapshot(indexName, 0);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(shard, shardDir);

        ObjectNode metadata = JSON.createObjectNode();
        metadata.set("settings", inspector.indexMetadata(indexName).path("settings"));
        ObjectNode mapping = (ObjectNode) JSON.readTree(
            SyntheticSourceReconstructor.extractMappingJson(inspector.indexMetadata(indexName)));
        ObjectNode properties = mapping.withObject("properties");
        properties.putObject("body").put("type", "text");
        properties.putObject("log_line").put("type", "match_only_text");
        metadata.set("mappings", mapping);

        try (SyntheticSourceReconstructor reconstructor =
                 SyntheticSourceReconstructor.open(shardDir, indexName, metadata)) {
            JsonNode doc = JSON.readTree(reconstructor.readSource(0));
            assertEquals(1.0, doc.get("price").asDouble(), 1e-9,
                "ordinary fields must reconstruct when the mapping declares analyzed text fields");
            assertEquals("web", doc.get("service_kind").asText());
        }
    }

    @Test
    void ignoresIndexTimeDynamicTemplatesDuringReconstruction(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        String index = "test_plain_synth";
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot(index, 0), shardDir);
        ObjectNode metadata = JSON.createObjectNode();
        metadata.set("settings", inspector.indexMetadata(index).path("settings"));
        ObjectNode mapping = (ObjectNode) JSON.readTree(
            SyntheticSourceReconstructor.extractMappingJson(inspector.indexMetadata(index)));
        mapping.set("dynamic_templates", JSON.readTree("""
            [{"strings": {"match_mapping_type": "string", "mapping": {"type": "text", "analyzer": "unavailable_custom_analyzer"}}}]
            """));
        metadata.set("mappings", mapping);
        try (var reconstructor = SyntheticSourceReconstructor.open(shardDir, index, metadata)) {
            assertEquals("web", JSON.readTree(reconstructor.readSource(0)).get("service_kind").asText());
        }
        assertTrue(metadata.path("mappings").has("dynamic_templates"));
    }

    /**
     * An ES 8+ mapping with {@code _source} disabled opens through the rewrite; this segment holds
     * many fields the one-field mapping does not know, so reading refuses with {@code unmapped}
     * (a FieldInfo no mapper owns), naming no field. With the kill switch off the open fails as in 1.1.0.
     */
    @Test
    void disabledSourceIsReportedExplicitly(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot("test_plain_synth", 0), shardDir);
        JsonNode metadata = JSON.readTree("""
            {"settings":{"index.number_of_shards":"1", "index.number_of_replicas":"0",
                "index.version.created":"8500000"},
             "mappings":{"_source":{"enabled":false},"properties":{"name":{"type":"keyword"}}}}
            """);
        try (var reconstructor = SyntheticSourceReconstructor.open(shardDir, "disabled", metadata)) {
            var refusal = org.junit.jupiter.api.Assertions.assertThrows(UnreconstructableSourceException.class,
                () -> reconstructor.readSource(0));
            assertEquals("unmapped", refusal.tokens());
        }
        String previous = System.getProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY);
        System.setProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, "false");
        try {
            var failure = org.junit.jupiter.api.Assertions.assertThrows(java.io.IOException.class,
                () -> SyntheticSourceReconstructor.open(shardDir, "disabled", metadata));
            assertEquals("SourceNotSyntheticException", failure.getClass().getSimpleName());
        } finally {
            if (previous == null) System.clearProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY);
            else System.setProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, previous);
        }
    }

    /**
     * ES 8.14+ {@code best_compression} segments store fields with ZSTD. ES's own decompressor
     * needs an ES distribution's native libzstd; in a standalone process its buffers are null, so
     * every document read through it fails with a message-less NullPointerException in
     * {@code ZstdCompressionMode$ZstdDecompressor.decompress}. Compares with ES's own {@code _search}
     * output for the same snapshot (generated by {@code scripts/generate_zstd_synth_fixture.py}:
     * ES 8.17, two compound segments per index, both tagged {@code BEST_COMPRESSION}).
     */
    @ParameterizedTest
    @CsvSource({"test_zstd_synth, zstd-synth-expected.json", "test_zstd_logsdb, zstd-logsdb-expected.json"})
    void reconstructsSourceFromZstdCompressedStoredFields(String indexName, String expectedFile, @TempDir Path tmp)
            throws Exception {
        Path repo = Path.of("src/test/resources/fixtures/zstd-synth-repo");
        JsonNode expected = JSON.readTree(Path.of("src/test/resources/fixtures", expectedFile).toFile());
        var store = new FileSystemBlobStore(repo);
        var inspector = new RepositoryInspector(store);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(inspector.shardSnapshot(indexName, 0), shardDir);

        Set<JsonNode> expectedSources = new HashSet<>();
        expected.forEach(expectedSources::add);
        Set<JsonNode> reconstructed = new HashSet<>();
        try (SyntheticSourceReconstructor reconstructor =
                 SyntheticSourceReconstructor.open(shardDir, indexName, inspector.indexMetadata(indexName))) {
            assertEquals(expected.size(), reconstructor.docCount());
            for (int docId = 0; docId < reconstructor.docCount(); docId++) {
                reconstructed.add(JSON.readTree(reconstructor.readSource(docId)));
            }
        }
        assertEquals(expectedSources, reconstructed);
    }

    private static String formatCoord(double value) {
        // Matches Jackson's rendering of a whole-valued double indexed as WKT text by the
        // fixture script (e.g. 1.5 stays "1.5", but the script never emits values needing more
        // than one decimal place here, so a direct string conversion is exact and stable).
        if (value == Math.floor(value)) {
            return String.valueOf((long) value) + ".0";
        }
        return String.valueOf(value);
    }

    /**
     * Every synthetic fixture index, as "repository directory/index". The per-doc reconstructed
     * text of each was captured into {@link #GOLDEN} on 1.0.0's reconstruction path and must never
     * drift: already exported rows and resumed runs must keep one rendering.
     */
    static final List<String> GOLDEN_INDICES = List.of(
        "synth-repo/test_plain_synth", "synth-repo/test_tsdb_synth",
        "zstd-synth-repo/test_zstd_synth", "zstd-synth-repo/test_zstd_logsdb",
        "synth-deletes-repo/test_deletes_lz4", "synth-deletes-repo/test_deletes_zstd",
        "synth-deletes-repo/test_deletes_nested");
    static final Path GOLDEN = Path.of("src/test/resources/fixtures/synth-reconstructed-golden.json");

    @ParameterizedTest
    @MethodSource("goldenIndices")
    void reconstructedTextMatchesTheGolden(String fixtureIndex, @TempDir Path tmp) throws Exception {
        JsonNode golden = JSON.readTree(GOLDEN.toFile()).path("reconstructed").path(fixtureIndex);
        List<String> expected = new ArrayList<>();
        golden.forEach(doc -> expected.add(doc.asText()));
        assertEquals(expected, reconstructAll(fixtureIndex, tmp.resolve("text")), fixtureIndex);

        // The pipeline's bytes: 1.0.0 exported the UTF-8 encoding of exactly this text.
        try (var reconstructor = Fixture.materialize(fixtureIndex, tmp.resolve("bytes")).open()) {
            for (int docId = 0; docId < expected.size(); docId++) {
                if (!expected.get(docId).startsWith("!")) {
                    org.junit.jupiter.api.Assertions.assertArrayEquals(expected.get(docId).getBytes(StandardCharsets.UTF_8),
                        reconstructor.readSourceBytes(docId), fixtureIndex + " doc " + docId);
                }
            }
        }
    }

    static List<String> goldenIndices() {
        return GOLDEN_INDICES;
    }

    /**
     * Rewrites the "reconstructed" section of {@link #GOLDEN}. Run only deliberately, on the build
     * whose output is the reference: {@code mvn test -Dtest=SyntheticSourceReconstructorTest
     * -Dgefrierschrank.captureGolden=true}.
     */
    @Test
    @EnabledIfSystemProperty(named = "gefrierschrank.captureGolden", matches = "true")
    void captureReconstructedGolden(@TempDir Path tmp) throws Exception {
        ObjectNode root = Files.exists(GOLDEN) ? (ObjectNode) JSON.readTree(GOLDEN.toFile()) : JSON.createObjectNode();
        ObjectNode section = JSON.createObjectNode();
        for (String fixtureIndex : GOLDEN_INDICES) {
            ArrayNode docs = section.putArray(fixtureIndex);
            reconstructAll(fixtureIndex, tmp.resolve(fixtureIndex.replace('/', '_'))).forEach(docs::add);
        }
        root.set("reconstructed", section);
        Files.writeString(GOLDEN, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(root) + "\n",
            StandardCharsets.UTF_8);
    }

    /**
     * Reconstructs every doc id of the index in ascending order, including deleted and nested
     * slots: the reconstructor itself does not filter. A doc that fails is recorded as "!" plus
     * the exception's simple class name (JSON text never starts with '!').
     */
    static List<String> reconstructAll(String fixtureIndex, Path tmp) throws IOException {
        try (SyntheticSourceReconstructor reconstructor = Fixture.materialize(fixtureIndex, tmp).open()) {
            return readAll(reconstructor);
        }
    }

    static String readOrFailureMarker(SyntheticSourceReconstructor reconstructor, int docId) {
        try {
            return reconstructor.readSource(docId);
        } catch (Exception | AssertionError e) {
            return "!" + e.getClass().getSimpleName();
        }
    }

    @Test
    void sharesOneIsolatedLoaderAcrossOpens(@TempDir Path tmp) throws Exception {
        Fixture plain = Fixture.materialize("synth-repo/test_plain_synth", tmp.resolve("plain"));
        Fixture tsdb = Fixture.materialize("synth-repo/test_tsdb_synth", tmp.resolve("tsdb"));
        try (var first = plain.open(); var second = tsdb.open()) {
            assertSame(first.workerLoader(), second.workerLoader());
            assertSame(IsolatedEsClassLoader.shared(), first.workerLoader());
        }
    }

    /** One loader per synthetic shard used to add about 2,800 classes and 14 MB of Metaspace each. */
    @Test
    void repeatedOpensDoNotLoadNewClasses(@TempDir Path tmp) throws Exception {
        Fixture plain = Fixture.materialize("synth-repo/test_plain_synth", tmp);
        try (var warmUp = plain.open()) {
            warmUp.readSource(0);
        }
        var classLoading = java.lang.management.ManagementFactory.getClassLoadingMXBean();
        long before = classLoading.getLoadedClassCount();
        for (int i = 0; i < 5; i++) {
            try (var reconstructor = plain.open()) {
                reconstructor.readSource(i);
            }
        }
        long grown = classLoading.getLoadedClassCount() - before;
        assertTrue(grown < 500, "5 reopens loaded " + grown + " classes");
    }

    /**
     * Static state inside the shared loader must not bleed between indices: every doc of four
     * different mappings/codecs, reconstructed in interleaved order through the shared loader,
     * equals a reconstruction through a fresh private loader per index.
     */
    @Test
    void interleavedIndicesThroughTheSharedLoaderMatchFreshLoaders(@TempDir Path tmp) throws Exception {
        List<String> names = List.of("synth-repo/test_plain_synth", "synth-repo/test_tsdb_synth",
            "zstd-synth-repo/test_zstd_synth", "zstd-synth-repo/test_zstd_logsdb");
        List<Fixture> fixtures = new ArrayList<>();
        List<List<String>> fresh = new ArrayList<>();
        for (String name : names) {
            Fixture fixture = Fixture.materialize(name, tmp.resolve(name.replace('/', '_')));
            fixtures.add(fixture);
            try (IsolatedEsClassLoader privateLoader = IsolatedEsClassLoader.create();
                 var reconstructor = SyntheticSourceReconstructor.open(fixture.shardDir, fixture.index,
                     fixture.metadata, privateLoader)) {
                assertNotSame(IsolatedEsClassLoader.shared(), reconstructor.workerLoader());
                fresh.add(readAll(reconstructor));
            }
        }
        int[] order = {0, 1, 3, 2, 0, 3, 1};
        List<SyntheticSourceReconstructor> open = new ArrayList<>();
        try {
            for (Fixture fixture : fixtures) {
                open.add(fixture.open());
            }
            for (int i : order) {
                assertEquals(fresh.get(i), readAll(open.get(i)), names.get(i));
            }
        } finally {
            for (var reconstructor : open) {
                reconstructor.close();
            }
        }
    }

    @Test
    void failedOpensDoNotPoisonTheSharedLoader(@TempDir Path tmp) throws Exception {
        Fixture plain = Fixture.materialize("synth-repo/test_plain_synth", tmp);
        IsolatedEsClassLoader shared = IsolatedEsClassLoader.shared();
        JsonNode sourceDisabled = JSON.readTree("""
            {"settings":{"index.number_of_shards":"1", "index.number_of_replicas":"0",
                "index.version.created":"8500000"},
             "mappings":{"_source":{"enabled":false},"properties":{"name":{"type":"keyword"},
                "raw":{"type":"object","enabled":false}}}}
            """);
        var disabled = assertThrows(IOException.class,
            () -> SyntheticSourceReconstructor.open(plain.shardDir, "disabled", sourceDisabled));
        assertEquals("UnreconstructableSourceException", disabled.getClass().getSimpleName());

        ObjectNode semantic = JSON.createObjectNode();
        semantic.set("settings", plain.metadata.path("settings"));
        ObjectNode mapping = (ObjectNode) JSON.readTree(SyntheticSourceReconstructor.extractMappingJson(plain.metadata));
        mapping.withObject("properties").putObject("summary").put("type", "semantic_text");
        semantic.set("mappings", mapping);
        assertThrows(SyntheticSourceReconstructor.UnsupportedSyntheticFieldException.class,
            () -> SyntheticSourceReconstructor.open(plain.shardDir, plain.index, semantic));

        try (var reconstructor = plain.open()) {
            assertSame(shared, reconstructor.workerLoader());
            assertEquals(JSON.readTree(GOLDEN.toFile()).path("reconstructed").path(plain.name).path(0).asText(),
                reconstructor.readSource(0));
        }
    }

    @Test
    void discardReplacesOnlyTheCurrentSharedLoader() throws Exception {
        IsolatedEsClassLoader current = IsolatedEsClassLoader.shared();
        try (IsolatedEsClassLoader stale = IsolatedEsClassLoader.create()) {
            IsolatedEsClassLoader.discard(stale);
            assertSame(current, IsolatedEsClassLoader.shared());
        }
        IsolatedEsClassLoader.discard(current);
        IsolatedEsClassLoader replacement = IsolatedEsClassLoader.shared();
        assertNotSame(current, replacement);
        assertSame(replacement, IsolatedEsClassLoader.shared());
    }

    @Test
    void onlyLinkageFailuresDiscardTheSharedLoader() {
        assertTrue(SyntheticSourceReconstructor.hasLinkageError(new java.lang.reflect.InvocationTargetException(
            new ExceptionInInitializerError(new IllegalStateException("static init")))));
        assertTrue(SyntheticSourceReconstructor.hasLinkageError(new IOException(new NoClassDefFoundError("x"))));
        assertFalse(SyntheticSourceReconstructor.hasLinkageError(new java.lang.reflect.InvocationTargetException(
            new IllegalArgumentException("No mapper found for type [x]"))));
        assertFalse(SyntheticSourceReconstructor.hasLinkageError(new OutOfMemoryError("Metaspace")));
    }

    /** Readiness for shard parallelism: one reconstructor per shard, each on its own thread. */
    @Test
    void concurrentShardsOnTheSharedLoaderReproduceTheGolden(@TempDir Path tmp) throws Exception {
        List<String> names = List.of("synth-repo/test_tsdb_synth", "zstd-synth-repo/test_zstd_logsdb",
            "synth-deletes-repo/test_deletes_zstd", "synth-deletes-repo/test_deletes_lz4");
        JsonNode golden = JSON.readTree(GOLDEN.toFile()).path("reconstructed");
        var pool = java.util.concurrent.Executors.newFixedThreadPool(names.size());
        try {
            List<java.util.concurrent.Future<List<String>>> results = new ArrayList<>();
            var start = new java.util.concurrent.CountDownLatch(1);
            for (String name : names) {
                Fixture fixture = Fixture.materialize(name, tmp.resolve(name.replace('/', '_')));
                results.add(pool.submit(() -> {
                    start.await();
                    try (var reconstructor = fixture.open()) {
                        List<String> docs = new ArrayList<>();
                        for (int round = 0; round < 3; round++) {
                            docs = readAll(reconstructor);
                        }
                        return docs;
                    }
                }));
            }
            start.countDown();
            for (int i = 0; i < names.size(); i++) {
                List<String> expected = new ArrayList<>();
                golden.path(names.get(i)).forEach(doc -> expected.add(doc.asText()));
                assertEquals(expected, results.get(i).get(), names.get(i));
            }
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * The per-segment cursor must give every document the same text whatever the access order:
     * descending forces a rebuild per doc, stride-3 skips docs like deletes and nested children do,
     * repeated reads hit the same doc twice, and the interleaved pass alternates between segments.
     */
    @ParameterizedTest
    @MethodSource("goldenIndices")
    void everyAccessOrderReproducesTheGolden(String fixtureIndex, @TempDir Path tmp) throws Exception {
        List<String> golden = new ArrayList<>();
        JSON.readTree(GOLDEN.toFile()).path("reconstructed").path(fixtureIndex).forEach(doc -> golden.add(doc.asText()));
        int n = golden.size();
        List<Integer> descending = new ArrayList<>();
        List<Integer> stride = new ArrayList<>();
        List<Integer> repeated = new ArrayList<>();
        List<Integer> interleaved = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            descending.add(n - 1 - i);
            repeated.add(i);
            repeated.add(i);
        }
        for (int start = 0; start < 3; start++) {
            for (int doc = start; doc < n; doc += 3) {
                stride.add(doc);
            }
        }
        for (int i = 0; i < (n + 1) / 2; i++) {
            interleaved.add(i);
            if (i + (n + 1) / 2 < n) {
                interleaved.add(i + (n + 1) / 2);
            }
        }
        try (var reconstructor = Fixture.materialize(fixtureIndex, tmp).open()) {
            assertEquals(n, reconstructor.docCount());
            for (var order : Map.of("descending", descending, "stride-3", stride, "repeated", repeated,
                    "interleaved", interleaved).entrySet()) {
                for (int doc : order.getValue()) {
                    assertEquals(golden.get(doc), readOrFailureMarker(reconstructor, doc),
                        fixtureIndex + " " + order.getKey() + " doc " + doc);
                }
            }
            assertThrows(IndexOutOfBoundsException.class, () -> reconstructor.readSource(-1));
            assertThrows(IndexOutOfBoundsException.class, () -> reconstructor.readSource(n));
            assertEquals(golden.get(0), readOrFailureMarker(reconstructor, 0), "usable after a rejected doc id");
        }
    }

    /** Logs mappings can declare thousands of fields most documents lack; the reused loaders must skip them all. */
    @Test
    void manyAbsentMappedFieldsDoNotChangeReusedLoaders(@TempDir Path tmp) throws Exception {
        Fixture plain = Fixture.materialize("synth-repo/test_plain_synth", tmp);
        ObjectNode metadata = JSON.createObjectNode();
        ObjectNode settings = metadata.putObject("settings");
        settings.setAll((ObjectNode) plain.metadata.path("settings"));
        settings.put("index.mapping.total_fields.limit", "5000");
        ObjectNode mapping = (ObjectNode) JSON.readTree(SyntheticSourceReconstructor.extractMappingJson(plain.metadata));
        ObjectNode unused = mapping.withObject("properties").putObject("unused").putObject("properties");
        for (int i = 0; i < 2000; i++) {
            unused.putObject(String.format("f%04d", i)).put("type", i % 3 == 0 ? "long" : "keyword");
        }
        metadata.set("mappings", mapping);
        List<String> golden = new ArrayList<>();
        JSON.readTree(GOLDEN.toFile()).path("reconstructed").path(plain.name).forEach(doc -> golden.add(doc.asText()));
        try (var reconstructor = SyntheticSourceReconstructor.open(plain.shardDir, plain.index, metadata)) {
            assertEquals(golden, readAll(reconstructor));
        }
    }

    static List<String> readAll(SyntheticSourceReconstructor reconstructor) {
        List<String> docs = new ArrayList<>();
        for (int docId = 0; docId < reconstructor.docCount(); docId++) {
            docs.add(readOrFailureMarker(reconstructor, docId));
        }
        return docs;
    }

    record Fixture(String name, String index, Path shardDir, JsonNode metadata) {
        static Fixture materialize(String name, Path tmp) throws IOException {
            String repo = name.substring(0, name.indexOf('/'));
            String index = name.substring(name.indexOf('/') + 1);
            var store = new FileSystemBlobStore(Path.of("src/test/resources/fixtures", repo));
            var inspector = new RepositoryInspector(store);
            Path shardDir = Files.createDirectories(tmp).resolve("shard");
            new ShardMaterializer(store).materialize(inspector.shardSnapshot(index, 0), shardDir);
            return new Fixture(name, index, shardDir, inspector.indexMetadata(index));
        }

        SyntheticSourceReconstructor open() throws IOException {
            return SyntheticSourceReconstructor.open(shardDir, index, metadata);
        }
    }

    @Test
    void reconstructsSourceFromRealEs819TsdbCodec(@TempDir Path tmp) throws Exception {
        var store = new FileSystemBlobStore(FIXTURE_REPO);
        var inspector = new RepositoryInspector(store);
        String indexName = "test_tsdb_synth";

        ShardSnapshot shard = inspector.shardSnapshot(indexName, 0);
        Path shardDir = tmp.resolve("shard");
        new ShardMaterializer(store).materialize(shard, shardDir);

        JsonNode rawMetadata = inspector.indexMetadata(indexName);

        try (SyntheticSourceReconstructor reconstructor =
                 SyntheticSourceReconstructor.open(shardDir, indexName, rawMetadata)) {
            assertEquals(60, reconstructor.docCount());

            for (int docId = 0; docId < reconstructor.docCount(); docId++) {
                String json = reconstructor.readSource(docId);
                JsonNode parsed = JSON.readTree(json);
                assertTrue(parsed.has("host"), "reconstructed doc " + docId + " missing 'host': " + json);
                assertTrue(parsed.has("region"), "reconstructed doc " + docId + " missing 'region': " + json);
                assertTrue(parsed.has("cpu_pct"));
                assertTrue(parsed.has("mem_bytes"));
                assertTrue(parsed.has("requests"));
                assertTrue(parsed.has("status"));
                assertTrue(parsed.get("host").asText().startsWith("host-"),
                    "unexpected host field: " + json);
            }
        }
    }
}
