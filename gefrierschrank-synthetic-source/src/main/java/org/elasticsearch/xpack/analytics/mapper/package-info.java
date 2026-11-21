/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/analytics/src/main/java/org/elasticsearch/xpack/analytics/mapper/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - verified by
 * reading every file's header directly. See
 * {@link org.elasticsearch.xpack.aggregatemetric.mapper} package-info for the full explanation of
 * how this differs from the tri-licensed {@code org.elasticsearch.index.mapper.extras} vendoring
 * elsewhere in this module.
 *
 * <p>Why vendored rather than a Maven dependency: {@code histogram} is implemented in ES's
 * {@code analytics} x-pack plugin module, bundled into a real node's distribution at startup, not
 * published standalone to Maven Central for 9.3.0. Without this mapper, reconstructing an index
 * that uses {@code histogram} fails with
 * {@code IllegalArgumentException: No mapper found for type [histogram]}.
 *
 * <p>Files vendored into this package:
 * <ul>
 *   <li>{@code HistogramFieldMapper.java} - the mapper itself.</li>
 *   <li>{@code HistogramParser.java}, {@code ExponentialHistogramParser.java},
 *       {@code ParsedHistogramConverter.java}, {@code IndexWithCount.java} - sibling files in ES's
 *       own {@code org.elasticsearch.xpack.analytics.mapper} package that
 *       {@code HistogramFieldMapper}'s parsing path calls directly (compiler-demanded, not
 *       speculative).</li>
 * </ul>
 *
 * <p>{@code ExponentialHistogramParser}/{@code ParsedHistogramConverter} reference
 * {@code org.elasticsearch.exponentialhistogram.*} types (ExponentialHistogram,
 * ExponentialHistogramXContent, ZeroBucket, ExponentialScaleUtils) - unlike the vendored x-pack
 * types, that module ({@code libs/exponential-histogram}) genuinely IS continuously published
 * to Maven Central as {@code org.elasticsearch:exponential-histogram:9.3.0} under plain Apache-2.0
 * (verified: its jar's own {@code META-INF/LICENSE.txt}), so it is a normal {@code pom.xml}
 * dependency of this module, not vendored source - see this module's {@code pom.xml} for that
 * dependency's own comment.
 *
 * <p>{@link org.elasticsearch.xpack.analytics.aggregations.support} (AnalyticsValuesSourceType,
 * HistogramValuesSource) is pulled in for the same compiler-demanded reason as
 * {@code aggregatemetric.aggregations.support} - referenced by
 * {@code HistogramFieldType.fielddataBuilder()}, never exercised by any aggregation this module
 * runs (it runs none).
 *
 * <p>None of ES's real {@code AnalyticsPlugin} (aggregation registrars, transport actions, usage
 * reporting) was vendored - see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}, the
 * project-written minimal facade used instead.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.analytics.mapper;
