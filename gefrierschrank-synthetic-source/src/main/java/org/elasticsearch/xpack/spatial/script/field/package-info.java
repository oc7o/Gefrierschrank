/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/spatial/src/main/java/org/elasticsearch/xpack/spatial/script/field/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> - see {@link org.elasticsearch.xpack.spatial}
 * package-info for the full explanation.
 *
 * <p>Just one file, {@code CartesianPointDocValuesField.java} - NOT used by the
 * {@code shape}/{@code geo_shape} mappers {@code GefrierschrankXPackMapperPlugin} registers
 * directly; vendored only because {@code PointFieldMapper} (in
 * {@link org.elasticsearch.xpack.spatial.index.mapper}, itself vendored only because
 * {@code CartesianPoint} references its {@code CONTENT_TYPE} constant) references it for its own
 * script doc-values support.
 *
 * <p>None of ES's real {@code SpatialPlugin} was vendored - see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.spatial.script.field;
