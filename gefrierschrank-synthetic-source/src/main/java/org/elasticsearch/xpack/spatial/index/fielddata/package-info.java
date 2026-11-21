/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path
 * {@code x-pack/plugin/spatial/src/main/java/org/elasticsearch/xpack/spatial/index/fielddata/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> - see {@link org.elasticsearch.xpack.spatial}
 * package-info for the full explanation.
 *
 * <p>Files vendored into this package (all compiler-demanded by the two spatial field mappers in
 * {@link org.elasticsearch.xpack.spatial.index.mapper}, or by each other):
 * <ul>
 *   <li>{@code ShapeValues.java} - the shared abstract shape-value model both
 *       {@code CartesianShapeValues} and {@code GeoShapeValues} extend.</li>
 *   <li>{@code CartesianShapeValues.java}, {@code GeoShapeValues.java} - the concrete per-CRS
 *       value types {@code fielddataBuilder()} in each mapper returns.</li>
 *   <li>{@code GeoRelation.java}, {@code Component2DRelationVisitor.java},
 *       {@code LabelPositionVisitor.java}, {@code Tile2DVisitor.java} - small support types/
 *       visitors {@code ShapeValues}/{@code GeoShapeValues} reference directly for relation
 *       queries and label-position computation.</li>
 *   <li>{@code LeafShapeFieldData.java}, {@code IndexShapeFieldData.java},
 *       {@code IndexCartesianPointFieldData.java} - the {@code LeafFieldData}/{@code IndexFieldData}
 *       interface layer the {@code plain} subpackage's concrete implementations satisfy.</li>
 * </ul>
 *
 * <p>None of ES's real {@code SpatialPlugin} was vendored - see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.spatial.index.fielddata;
