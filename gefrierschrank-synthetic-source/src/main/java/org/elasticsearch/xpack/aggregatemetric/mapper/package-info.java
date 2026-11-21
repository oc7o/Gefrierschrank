/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0}.
 *
 * <p><strong>License differs from {@code org.elasticsearch.index.mapper.extras} sitting alongside
 * this one</strong> - read this carefully before touching anything under {@code org.elasticsearch.xpack}
 * in this module: every file vendored here (this package and its sibling {@code org.elasticsearch.xpack.*}
 * packages - {@code aggregatemetric}, {@code analytics}, {@code constantkeyword}, {@code wildcard},
 * plus the small {@code org.elasticsearch.xpack.core.termsenum.action} helper) declares its header as
 * <strong>Elastic License 2.0 ONLY</strong> - verified by reading every individual file's license header
 * directly, not assumed. There is no AGPL-3.0/SSPL-1.0 alternative for these files, unlike the tri-licensed
 * {@code mapper-extras} vendoring in {@code org.elasticsearch.index.mapper.extras} (see that package's
 * {@code package-info.java}). EL2.0 is a source-available license with commercial-hosting restrictions,
 * materially more restrictive than "AGPL-3.0-but-otherwise-permissive" - this project's own
 * {@code LICENSE}/{@code pom.xml} declare this distinctly, not folded into the module's existing AGPL-3.0
 * declaration.
 *
 * <p>Why these live here instead of an ordinary Maven dependency: {@code aggregate_metric_double} is
 * implemented in ES's {@code mapper-aggregate-metric} x-pack plugin module, which - like every x-pack
 * plugin module - a real ES node loads bundled into its distribution at startup, not as a
 * continuously-published standalone Maven artifact for every version (verified: no
 * {@code org.elasticsearch.plugin:mapper-aggregate-metric} artifact exists on Maven Central for 9.3.0).
 * Without this mapper, reconstructing an index that uses {@code aggregate_metric_double} fails with
 * {@code IllegalArgumentException: No mapper found for type [aggregate_metric_double]}, and a direct
 * {@code MapperRegistry.getMapperParser(...)} query for the type returns {@code null}.
 *
 * <p>Files vendored into this package, from
 * {@code x-pack/plugin/mapper-aggregate-metric/src/main/java/org/elasticsearch/xpack/aggregatemetric/mapper/}:
 * <ul>
 *   <li>{@code AggregateMetricDoubleFieldMapper.java}</li>
 *   <li>{@code AggregateMetricDoubleBlockLoader.java}</li>
 * </ul>
 *
 * <p>Additional files pulled in only because the compiler genuinely demanded them (transitive
 * dependency-chain resolution done compile-error-driven, not speculatively) - see those packages'
 * own headers for their exact upstream paths:
 * <ul>
 *   <li>{@link org.elasticsearch.xpack.aggregatemetric.fielddata} (IndexAggregateMetricDoubleFieldData,
 *       LeafAggregateMetricDoubleFieldData) - referenced directly by
 *       {@code AggregateMetricDoubleFieldType.fielddataBuilder()}.</li>
 *   <li>{@link org.elasticsearch.xpack.aggregatemetric.aggregations.support}
 *       (AggregateMetricsValuesSourceType, AggregateMetricsValuesSource) - referenced by the same
 *       method for its {@code ValuesSourceType} constant; {@code AggregateMetricsValuesSource} itself
 *       needed only because {@code AggregateMetricsValuesSourceType} references it.</li>
 * </ul>
 *
 * <p>None of the real aggregation-registrar/action/usage-reporting machinery from ES's actual
 * {@code AggregateMetricMapperPlugin} was vendored or reimplemented - this module's standalone,
 * non-search {@code MapperService.merge(...)} path never calls it. Instead, see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin} - a
 * project-written, NOT vendored, minimal {@code MapperPlugin} facade that registers just this
 * package's (and its three sibling packages') {@code Mapper.TypeParser}s.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.aggregatemetric.mapper;
