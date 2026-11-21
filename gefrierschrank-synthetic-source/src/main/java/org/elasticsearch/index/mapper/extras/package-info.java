/**
 * Vendored verbatim from {@code https://github.com/elastic/elasticsearch}, tag {@code v9.3.0},
 * path {@code modules/mapper-extras/src/main/java/org/elasticsearch/index/mapper/extras/}.
 *
 * <p>Why files from ES's own repository live inside this repository instead of being an ordinary
 * Maven dependency: {@code scaled_float} (plus {@code token_count}, {@code rank_feature},
 * {@code rank_features}, {@code search_as_you_type}, {@code match_only_text}) are implemented in
 * Elasticsearch's {@code mapper-extras} module, not in {@code org.elasticsearch:elasticsearch}
 * core - a real ES node loads it as a bundled plugin module at startup via
 * {@link org.elasticsearch.index.mapper.extras.MapperExtrasPlugin}. That module is NOT published
 * to Maven Central for 9.3.0 (only 6.1.0 and 7.4.0 ever were - verified via
 * {@code repo1.maven.org}), because ES ships its bundled modules as part of its distribution
 * zip/Docker image, not as continuously-published standalone artifacts for every version. Since
 * this module's whole point is reconstructing synthetic {@code _source}, including for indices
 * that map {@code scaled_float} or another of these types, depending on a published jar was not an
 * option, so these 11 files (all under the identical Elastic-License-2.0 / AGPL-3.0 /
 * SSPL-1.0 tri-license as the {@code org.elasticsearch:elasticsearch} core jar this module already
 * depends on and already declares AGPL-3.0 for - see this module's LICENSE file) are vendored
 * here unmodified, in the exact package ES itself declares them under, so
 * {@link org.elasticsearch.indices.IndicesModule} can be handed a real
 * {@link org.elasticsearch.index.mapper.extras.MapperExtrasPlugin} instance the same way a real
 * ES node would.
 *
 * <p>Kept as close to upstream as possible on purpose: do not repackage, rename, or "clean up"
 * these classes when touching this directory - if a newer ES version needs picking up, re-pull
 * the whole set from the matching tag rather than patching in place.
 */
package org.elasticsearch.index.mapper.extras;
