/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path
 * {@code x-pack/plugin/mapper-unsigned-long/src/main/java/org/elasticsearch/xpack/unsignedlong/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - see
 * {@link org.elasticsearch.xpack.aggregatemetric.mapper} package-info for the full explanation.
 * Same {@code LICENSE-ELASTIC-2.0} file covers this package too.
 *
 * <p>Why vendored rather than a Maven dependency: {@code unsigned_long} is implemented in ES's
 * {@code mapper-unsigned-long} x-pack plugin module, bundled into a real node's distribution at
 * startup, not published standalone to Maven Central for 9.3.0. Without this mapper,
 * reconstructing an index that uses {@code unsigned_long} fails with
 * {@code IllegalArgumentException: No mapper found for type [unsigned_long]}. This mapper is
 * important to get exactly right: the whole type exists because a full
 * unsigned 64-bit range (0 to 2^64-1) doesn't fit in Java's signed {@code long} - ES stores it
 * shifted by {@code 2^63} so ordinary Lucene sort order still works, and a value above
 * {@code Long.MAX_VALUE} decodes as NEGATIVE if that shift is applied incorrectly (or skipped).
 * See this module's {@code SyntheticSourceReconstructorTest} for a fixture that deliberately
 * exercises values on both sides of that boundary.
 *
 * <p>Files vendored into this package, from
 * {@code x-pack/plugin/mapper-unsigned-long/src/main/java/org/elasticsearch/xpack/unsignedlong/}:
 * <ul>
 *   <li>{@code UnsignedLongFieldMapper.java} - the mapper itself (919 lines upstream).</li>
 *   <li>{@code UnsignedLongDocValuesField.java} - script doc-values support.</li>
 *   <li>{@code UnsignedLongIndexFieldData.java}, {@code UnsignedLongLeafFieldData.java},
 *       {@code SourceValueFetcherSortedUnsignedLongIndexFieldData.java} - the
 *       {@code IndexFieldData}/{@code LeafFieldData} chain {@code fielddataBuilder()} needs for
 *       both the doc-values-backed and the source-value-fetcher-backed (script) fielddata paths.</li>
 *   <li>{@code UnsignedLongScriptDocValues.java} - referenced by
 *       {@code UnsignedLongDocValuesField.toScriptDocValues()}.</li>
 * </ul>
 * All pulled in compiler-error-driven, not speculatively.
 *
 * <p>None of ES's real {@code UnsignedLongMapperPlugin} was vendored - see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}, the
 * project-written minimal facade used instead.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.unsignedlong;
