/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/mapper-version/src/main/java/org/elasticsearch/xpack/versionfield/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - see
 * {@link org.elasticsearch.xpack.aggregatemetric.mapper} package-info for the full explanation of
 * how this differs from the tri-licensed {@code org.elasticsearch.index.mapper.extras} vendoring
 * elsewhere in this module. Same {@code LICENSE-ELASTIC-2.0} file covers this package too - not a
 * new/different license, just more files under it.
 *
 * <p>Why vendored rather than a Maven dependency: the {@code version} field type is implemented in
 * ES's {@code mapper-version} x-pack plugin module, bundled into a real node's distribution at
 * startup, not published standalone to Maven Central for 9.3.0. Without this mapper,
 * reconstructing an index that uses the {@code version} field type fails with
 * {@code IllegalArgumentException: No mapper found for type [version]}.
 *
 * <p><strong>Do not confuse with {@code org.elasticsearch.index.mapper.VersionFieldMapper}</strong>
 * (ES core, not vendored here): that is the INTERNAL {@code _version} METADATA field
 * ({@code MetadataFieldMapper}, no real "version" {@code TypeParser}) - completely unrelated to
 * this package's real {@code version} field TYPE. Verified directly from {@link
 * VersionStringFieldMapper}'s own {@code CONTENT_TYPE} constant and {@code PARSER} field, not by
 * pattern-matching the class name - see that class's own Javadoc, which explicitly cross-references
 * the unrelated core class for exactly this reason.
 *
 * <p>Files vendored into this package:
 * <ul>
 *   <li>{@code VersionStringFieldMapper.java} - the mapper itself (472 lines upstream).</li>
 *   <li>{@code VersionEncoder.java} - the semver-aware binary encoder/decoder
 *       {@code VersionStringFieldMapper} calls directly for both indexing and doc-value decode.</li>
 *   <li>{@code VersionFieldWildcardQuery.java} - referenced by
 *       {@code VersionStringFieldType.wildcardQuery(...)}.</li>
 *   <li>{@code VersionStringDocValuesField.java} - referenced by
 *       {@code VersionStringFieldType.fielddataBuilder(...)}.</li>
 *   <li>{@code Version.java} - the semver-parsing value type
 *       {@code VersionStringDocValuesField}'s script doc-values support depends on.</li>
 * </ul>
 * All pulled in compiler-error-driven (compile, add exactly what's demanded, stop), not
 * speculatively - the same recipe used for the other x-pack packages in this module.
 *
 * <p>One local deviation from the exact upstream file, noted here for auditability:
 * {@code VersionEncoder.java}'s single use of {@code org.apache.commons.codec.Charsets.UTF_8} was
 * replaced with {@code java.nio.charset.StandardCharsets.UTF_8} (functionally identical - both are
 * just the UTF-8 {@code Charset} constant) rather than adding Apache Commons Codec as a new
 * dependency for a single constant reference. Flagged explicitly per this project's vendoring
 * policy of not silently patching vendored files.
 *
 * <p>None of ES's real {@code VersionFieldPlugin} was vendored - see
 * {@code de.sloow.gefrierschrank.syntheticsource.isolated.GefrierschrankXPackMapperPlugin}, the
 * project-written minimal facade used instead.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.versionfield;
