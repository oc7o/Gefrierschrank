/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code x-pack/plugin/wildcard/src/main/java/org/elasticsearch/xpack/wildcard/}.
 *
 * <p><strong>Elastic License 2.0 ONLY</strong> (no AGPL-3.0/SSPL-1.0 alternative) - see
 * {@link org.elasticsearch.xpack.wildcard.mapper} package-info for the full explanation.
 *
 * <p>Just one small file ({@code WildcardDocValuesField.java}), referenced directly by
 * {@code WildcardFieldMapper}'s script doc-values support - compiler-demanded, not speculative.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull the
 * whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.xpack.wildcard;
