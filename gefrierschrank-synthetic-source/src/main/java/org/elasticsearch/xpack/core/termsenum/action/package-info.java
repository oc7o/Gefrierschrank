/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/core/src/main/java/org/elasticsearch/xpack/core/termsenum/action/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - see
 * {@link org.elasticsearch.xpack.constantkeyword.mapper} package-info for the fix this supports.
 *
 * <p>Just one small file ({@code SimpleTermCountEnum.java}, ~120 lines), pulled in purely because
 * {@code ConstantKeywordFieldMapper.ConstantKeywordFieldType} directly constructs one in its
 * {@code termsEnum(...)} override - not the real x-pack "terms enum" transport action feature this
 * lives under in ES's own module structure (this project never touches that action or its
 * transport wiring), just this one small, self-contained value class.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.core.termsenum.action;
