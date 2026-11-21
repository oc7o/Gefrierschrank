/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/spatial/src/main/java/org/elasticsearch/xpack/spatial/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - see
 * {@link org.elasticsearch.xpack.aggregatemetric.mapper} package-info for the full explanation of
 * how this differs from the tri-licensed {@code org.elasticsearch.index.mapper.extras} vendoring
 * elsewhere in this module. Same {@code LICENSE-ELASTIC-2.0} file covers every
 * {@code org.elasticsearch.xpack.spatial.*} subpackage - see each subpackage's own
 * {@code package-info.java} for its exact file list.
 *
 * <p>This top-level package itself is empty (no {@code .java} files land directly here) - it
 * exists only as the common root for the {@code shape} and {@code geo_shape} field-type mappers
 * and their supporting fielddata/aggregation-source/script classes, split across these
 * subpackages:
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.spatial.index.mapper} - the two field mappers themselves
 *       ({@code ShapeFieldMapper} for {@code shape}, {@code GeoShapeWithDocValuesFieldMapper} for
 *       {@code geo_shape}) plus a small {@code PointFieldMapper}/{@code ShapeQueryable}/
 *       {@code ShapeParser} support trio pulled in compiler-demanded.</li>
 *   <li>{@link org.elasticsearch.xpack.spatial.index.fielddata} and
 *       {@link org.elasticsearch.xpack.spatial.index.fielddata.plain} - the
 *       {@code ShapeValues}/{@code CartesianShapeValues}/{@code GeoShapeValues} value model and
 *       the {@code IndexFieldData}/{@code LeafFieldData} classes that decode doc values into it.</li>
 *   <li>{@link org.elasticsearch.xpack.spatial.common} - {@code CartesianPoint} and
 *       {@code CartesianBoundingBox}, the Cartesian-space value types those use.</li>
 *   <li>{@link org.elasticsearch.xpack.spatial.search.aggregations.support} - the
 *       {@code ValuesSource}/{@code ValuesSourceType} plumbing {@code fielddataBuilder()}
 *       references (this project's read-only reconstruction path never actually runs an
 *       aggregation through these, but the real mapper classes reference the types directly, so
 *       they must resolve at compile time - see {@code GefrierschrankXPackMapperPlugin}'s Javadoc
 *       for why the real aggregation-registrar machinery around them was NOT vendored/wired up).</li>
 *   <li>{@link org.elasticsearch.xpack.spatial.script.field} - {@code CartesianPointDocValuesField},
 *       needed only because {@code PointFieldMapper} (itself needed only because
 *       {@code CartesianPoint} references its {@code CONTENT_TYPE} constant) references it.</li>
 * </ul>
 *
 * <p>Without these mappers, reconstructing an index that uses {@code shape} or {@code geo_shape}
 * fails with {@code IllegalArgumentException: No mapper found for type [shape]} or
 * {@code ... [geo_shape]}.
 *
 * <p><strong>Deliberate scope decision on legacy geo_shape</strong>: ES 9.3.0's real
 * {@code GeoShapeWithDocValuesFieldMapper.TypeParser} branches to a separate, older
 * {@code org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper} (from ES's
 * {@code modules/legacy-geo} module) for indices created before ES 6.6 that still use the
 * deprecated {@code strategy}/{@code tree}/{@code tree_levels}/{@code precision}/
 * {@code distance_error_pct}/{@code points_only} parameters. That legacy mapper was deliberately
 * NOT vendored here: it pulls in a materially large, genuinely new (non-ES) dependency chain this
 * project does not otherwise need - {@code spatial4j}, {@code jts-core},
 * {@code lucene-spatial-extras}, {@code lucene-spatial3d}, {@code s2-geometry-library-java}. The
 * vendored {@code GeoShapeWithDocValuesFieldMapper.TypeParser.parse(...)} here still detects the
 * deprecated parameter names and throws a clear, explicit error naming them, rather than either
 * silently mis-parsing them or crashing with an unrelated {@code NoClassDefFoundError} - see that
 * class's inline comment at the deviation point for the exact reasoning. Reading a pre-6.6-created
 * index that carries one of those parameters would need {@code modules/legacy-geo} vendored
 * properly - a distinct, separate follow-up, not a silent gap here.
 *
 * <p>None of ES's real {@code SpatialPlugin} was vendored - see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}, the
 * project-written minimal facade used instead (including its own small, empty
 * {@code GeoFormatterFactory} construction in place of {@code SpatialPlugin}'s vector-tile
 * extension-loading machinery - see that facade's Javadoc).
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.spatial;
