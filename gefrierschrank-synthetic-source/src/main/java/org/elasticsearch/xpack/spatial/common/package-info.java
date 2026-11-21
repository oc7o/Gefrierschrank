/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/spatial/src/main/java/org/elasticsearch/xpack/spatial/common/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> - see {@link org.elasticsearch.xpack.spatial}
 * package-info for the full explanation.
 *
 * <p>Files vendored into this package:
 * <ul>
 *   <li>{@code CartesianPoint.java} - the Cartesian-space point value type
 *       {@code CartesianShapeDocValuesField} (in {@code ShapeFieldMapper}) uses for its centroid/
 *       bounding-box/label-position script accessors.</li>
 *   <li>{@code CartesianBoundingBox.java} - the bounding-box value type built from two
 *       {@code CartesianPoint}s, used for the same script-accessor purpose.</li>
 * </ul>
 * Both compiler-demanded by {@link org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper}.
 *
 * <p>None of ES's real {@code SpatialPlugin} was vendored - see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.spatial.common;
