package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;

import java.util.concurrent.ConcurrentHashMap;

/**
 * The nested-parent bitset producer a worker's MapperService hands to Elasticsearch's nested
 * synthetic-source loader. Elasticsearch asks for a producer on every loader it builds, that is
 * once per leaf per batch; one {@link QueryBitSetProducer} per distinct filter keeps its per-core
 * cache, so a segment's parent bitset is built once per worker instead of once per batch. The
 * bitset comes from a scorer, which ignores live docs: deleted roots delimit blocks as in
 * Elasticsearch's own {@code BitsetFilterCache}.
 */
public final class BitSetProducerCache {

    private final ConcurrentHashMap<Query, BitSetProducer> producers = new ConcurrentHashMap<>();

    public BitSetProducer producer(Query query) {
        return producers.computeIfAbsent(query, QueryBitSetProducer::new);
    }
}
