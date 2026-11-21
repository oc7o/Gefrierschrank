/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path
 * {@code x-pack/plugin/spatial/src/main/java/org/elasticsearch/xpack/spatial/search/aggregations/support/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> - see {@link org.elasticsearch.xpack.spatial}
 * package-info for the full explanation.
 *
 * <p>Files vendored into this package - the {@code ValuesSource}/{@code ValuesSourceType}
 * aggregation-plumbing constants and classes {@code fielddataBuilder()} in both spatial mappers
 * references, compiler-demanded, NOT because this project's read-only reconstruction path
 * actually runs any aggregation through them (see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}'s
 * Javadoc: the real aggregation-registrar machinery around these types was deliberately not
 * wired up):
 * <ul>
 *   <li>{@code ShapeValuesSourceType.java}, {@code ShapeValuesSource.java} - the shared abstract
 *       base type/values-source both CRS-specific ones extend.</li>
 *   <li>{@code CartesianShapeValuesSourceType.java}, {@code CartesianShapeValuesSource.java} -
 *       {@code shape} (Cartesian).</li>
 *   <li>{@code GeoShapeValuesSourceType.java}, {@code GeoShapeValuesSource.java} -
 *       {@code geo_shape} (lon/lat).</li>
 *   <li>{@code CartesianPointValuesSourceType.java}, {@code CartesianPointValuesSource.java} -
 *       NOT used by the {@code shape}/{@code geo_shape} mappers directly; pulled in only because
 *       {@code CartesianShapeValuesSourceType} references the {@code point} field type's
 *       values-source machinery for its own wiring (same reason
 *       {@code CartesianPointIndexFieldData} is vendored in the sibling
 *       {@code index.fielddata.plain} package - see that package's own note).</li>
 * </ul>
 * All pulled in compiler-error-driven, not speculatively.
 *
 * <p>None of ES's real {@code SpatialPlugin} was vendored - see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.spatial.search.aggregations.support;
