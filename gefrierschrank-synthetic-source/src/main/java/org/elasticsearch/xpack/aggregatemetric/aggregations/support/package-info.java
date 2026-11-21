/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/mapper-aggregate-metric/src/main/java/org/elasticsearch/xpack/aggregatemetric/aggregations/support/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - see
 * {@link org.elasticsearch.xpack.aggregatemetric.mapper} package-info for the full explanation.
 *
 * <p>Pulled in purely because the compiler demanded it: {@code AggregateMetricDoubleFieldMapper}
 * references {@code AggregateMetricsValuesSourceType.AGGREGATE_METRIC}, whose class in turn
 * references {@code AggregateMetricsValuesSource} - neither is used by any aggregation-registration
 * code path this project actually calls (this module never registers or runs aggregations), they
 * are simply referenced at class-load/construction time from the mapper class this package's
 * sibling depends on.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.aggregatemetric.aggregations.support;
