/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path
 * {@code x-pack/plugin/mapper-counted-keyword/src/main/java/org/elasticsearch/xpack/countedkeyword/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - see
 * {@link org.elasticsearch.xpack.aggregatemetric.mapper} package-info for the full explanation.
 * Same {@code LICENSE-ELASTIC-2.0} file covers this package too.
 *
 * <p>Why vendored rather than a Maven dependency: {@code counted_keyword} is implemented in ES's
 * {@code mapper-counted-keyword} x-pack plugin module, bundled into a real node's distribution at
 * startup, not published standalone to Maven Central for 9.3.0. Without this mapper,
 * reconstructing an index that uses {@code counted_keyword} fails with
 * {@code IllegalArgumentException: No mapper found for type [counted_keyword]}.
 *
 * <p>Files vendored into this package:
 * <ul>
 *   <li>{@code CountedKeywordFieldMapper.java} - the only file (552 lines upstream); it references
 *       only ES core types ({@code BinaryFieldMapper}, {@code KeywordFieldMapper}, etc.) already
 *       present in {@code org.elasticsearch:elasticsearch} - no further compiler-demanded files
 *       needed vendoring for this one.</li>
 * </ul>
 *
 * <p>None of ES's real {@code CountedKeywordMapperPlugin} was vendored - see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}, the
 * project-written minimal facade used instead.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.countedkeyword;
