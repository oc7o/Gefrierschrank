/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/spatial/src/main/java/org/elasticsearch/xpack/spatial/index/mapper/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> - see {@link org.elasticsearch.xpack.spatial}
 * package-info for the full explanation, including the deliberate legacy-geo-shape scope
 * decision.
 *
 * <p>Files vendored into this package:
 * <ul>
 *   <li>{@code ShapeFieldMapper.java} - the {@code shape} (Cartesian) field mapper (417 lines
 *       upstream).</li>
 *   <li>{@code GeoShapeWithDocValuesFieldMapper.java} - the {@code geo_shape} (lon/lat) field
 *       mapper (597 lines upstream) - the MODERN, doc-values-backed implementation ES 9.3.0
 *       actually uses by default. Locally patched at exactly one point (its nested
 *       {@code TypeParser.parse(...)}): the real upstream branches to
 *       {@code org.elasticsearch.legacygeo.mapper.LegacyGeoShapeFieldMapper} for pre-6.6 indices;
 *       here that branch is replaced with a small local constant set of the same deprecated
 *       parameter names plus a clear rejection error, rather than vendoring the whole separate
 *       {@code modules/legacy-geo} module - see {@link org.elasticsearch.xpack.spatial}
 *       package-info for why.</li>
 *   <li>{@code ShapeParser.java} - the {@code shape} field's
 *       {@code AbstractGeometryFieldMapper.Parser<Geometry>} implementation,
 *       compiler-demanded by {@code ShapeFieldMapper.Builder.build(...)}.</li>
 *   <li>{@code ShapeQueryable.java} - a small interface {@code ShapeFieldMapper.ShapeFieldType}
 *       implements.</li>
 *   <li>{@code PointFieldMapper.java} - NOT registered as a {@code Mapper.TypeParser} by
 *       {@code GefrierschrankXPackMapperPlugin} (which registers {@code shape} and
 *       {@code geo_shape} from this package, not the separate {@code point} field type) - vendored
 *       purely because {@code CartesianPoint} (in {@link org.elasticsearch.xpack.spatial.common})
 *       references its {@code CONTENT_TYPE} constant. If {@code point} support is wanted (an index
 *       using it fails with {@code IllegalArgumentException: No mapper found for type [point]}),
 *       this file is already here and only needs a {@code getMappers()} registration added.</li>
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
package org.elasticsearch.xpack.spatial.index.mapper;
