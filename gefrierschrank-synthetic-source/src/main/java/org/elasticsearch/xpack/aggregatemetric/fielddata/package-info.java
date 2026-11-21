/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/mapper-aggregate-metric/src/main/java/org/elasticsearch/xpack/aggregatemetric/fielddata/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - see
 * {@link org.elasticsearch.xpack.aggregatemetric.mapper} package-info for the full explanation of
 * why this differs from the tri-licensed {@code org.elasticsearch.index.mapper.extras} vendoring
 * elsewhere in this module, and this project's LICENSE/pom.xml for where that distinction is
 * declared.
 *
 * <p>Pulled in purely because the compiler demanded it: {@code AggregateMetricDoubleFieldMapper}'s
 * {@code fielddataBuilder()} directly references {@code IndexAggregateMetricDoubleFieldData} and
 * {@code LeafAggregateMetricDoubleFieldData} - not vendored speculatively.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.aggregatemetric.fielddata;
