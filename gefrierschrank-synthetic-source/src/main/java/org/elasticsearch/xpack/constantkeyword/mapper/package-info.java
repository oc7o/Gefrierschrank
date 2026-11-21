/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/mapper-constant-keyword/src/main/java/org/elasticsearch/xpack/constantkeyword/mapper/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - see
 * {@link org.elasticsearch.xpack.aggregatemetric.mapper} package-info for the full explanation of
 * how this differs from the tri-licensed {@code org.elasticsearch.index.mapper.extras} vendoring
 * elsewhere in this module.
 *
 * <p>Why vendored rather than a Maven dependency: {@code constant_keyword} is implemented in ES's
 * {@code mapper-constant-keyword} x-pack plugin module, bundled into a real node's distribution at
 * startup, not published standalone to Maven Central for 9.3.0. Without this mapper,
 * reconstructing an index that uses {@code constant_keyword} fails with
 * {@code IllegalArgumentException: No mapper found for type [constant_keyword]}.
 *
 * <p>Only one file vendored into this package: {@code ConstantKeywordFieldMapper.java}. Its
 * sibling {@link org.elasticsearch.xpack.constantkeyword} (ConstantKeywordDocValuesField) and
 * {@link org.elasticsearch.xpack.core.termsenum.action} (SimpleTermCountEnum) were pulled in only
 * because the compiler demanded them.
 *
 * <p>None of ES's real {@code ConstantKeywordMapperPlugin} was vendored - see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}, the
 * project-written minimal facade used instead.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.constantkeyword.mapper;
