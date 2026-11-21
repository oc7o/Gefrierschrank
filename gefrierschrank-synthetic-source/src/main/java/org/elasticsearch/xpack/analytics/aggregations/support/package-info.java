/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/analytics/src/main/java/org/elasticsearch/xpack/analytics/aggregations/support/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - see
 * {@link org.elasticsearch.xpack.analytics.mapper} package-info for the full explanation.
 *
 * <p>Pulled in purely because the compiler demanded it: {@code HistogramFieldMapper} references
 * {@code AnalyticsValuesSourceType.HISTOGRAM}, whose class in turn references
 * {@code HistogramValuesSource} - neither is used by any aggregation-registration code path this
 * project actually calls.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.analytics.aggregations.support;
