/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path
 * {@code x-pack/plugin/spatial/src/main/java/org/elasticsearch/xpack/spatial/index/fielddata/plain/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> - see {@link org.elasticsearch.xpack.spatial}
 * package-info for the full explanation.
 *
 * <p>Files vendored into this package - the concrete {@code IndexFieldData}/{@code LeafFieldData}
 * classes that decode real Lucene doc values into the value model in the parent
 * {@link org.elasticsearch.xpack.spatial.index.fielddata} package, one pair per CRS/field type:
 * <ul>
 *   <li>{@code CartesianShapeIndexFieldData.java}, {@code CartesianShapeDVAtomicShapeFieldData.java},
 *       {@code AbstractAtomicCartesianShapeFieldData.java} - {@code shape} (Cartesian).</li>
 *   <li>{@code LatLonShapeIndexFieldData.java}, {@code LatLonShapeDVAtomicShapeFieldData.java},
 *       {@code AbstractAtomicGeoShapeShapeFieldData.java} - {@code geo_shape} (lon/lat).</li>
 *   <li>{@code AbstractShapeIndexFieldData.java} - the shared abstract base both
 *       {@code CartesianShapeIndexFieldData} and {@code LatLonShapeIndexFieldData} extend.</li>
 *   <li>{@code CartesianPointIndexFieldData.java}, {@code CartesianPointDVLeafFieldData.java},
 *       {@code LeafCartesianPointFieldData.java} - NOT used by the {@code shape}/{@code geo_shape}
 *       mappers {@code GefrierschrankXPackMapperPlugin} registers; pulled in only because
 *       {@code CartesianShapeValuesSourceType}
 *       (in {@link org.elasticsearch.xpack.spatial.search.aggregations.support}) references the
 *       {@code point} field type's fielddata for its aggregation-source wiring, and that class is
 *       itself compiler-demanded by the shape mappers' {@code fielddataBuilder()}.</li>
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
package org.elasticsearch.xpack.spatial.index.fielddata.plain;
