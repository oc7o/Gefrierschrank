/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/wildcard/src/main/java/org/elasticsearch/xpack/wildcard/mapper/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - see
 * {@link org.elasticsearch.xpack.aggregatemetric.mapper} package-info for the full explanation of
 * how this differs from the tri-licensed {@code org.elasticsearch.index.mapper.extras} vendoring
 * elsewhere in this module.
 *
 * <p>Why vendored rather than a Maven dependency: {@code wildcard} is implemented in ES's
 * {@code wildcard} x-pack plugin module, bundled into a real node's distribution at startup, not
 * published standalone to Maven Central for 9.3.0. Without this mapper the type is missing from
 * the built fat jar's {@code MapperRegistry} (confirmed via a
 * {@code getMapperParser(type, IndexVersion.current())} query), so reconstructing an index that
 * uses this common field type would fail.
 *
 * <p>Files vendored into this package:
 * <ul>
 *   <li>{@code WildcardFieldMapper.java} - the mapper itself.</li>
 *   <li>{@code BinaryDvConfirmedQuery.java} - a sibling file in the same upstream package that
 *       {@code WildcardFieldMapper}'s query-confirmation path calls directly.</li>
 * </ul>
 * Its sibling {@link org.elasticsearch.xpack.wildcard} (WildcardDocValuesField) was pulled in only
 * because the compiler demanded it.
 *
 * <p>None of ES's real {@code WildcardPlugin} was vendored - see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}, the
 * project-written minimal facade used instead.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.wildcard.mapper;
