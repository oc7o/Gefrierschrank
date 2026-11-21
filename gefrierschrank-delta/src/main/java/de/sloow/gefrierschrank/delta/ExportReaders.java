package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.lucene.ShardReader;

import java.io.IOException;
import java.nio.file.Path;

/** Shard readers for export sinks outside the Delta pipeline, such as the JSONL dump. */
public final class ExportReaders {

    private ExportReaders() {}

    /**
     * {@code delegate} with the reader stack the Delta export uses: excluded vectors put back
     * through Elasticsearch's loaders, and documents without a stored {@code _source} reconstructed
     * through Elasticsearch's mappers, both from {@code rawIndexMetadata}, the metadata of the
     * snapshot the shard was materialized from. With null metadata, {@code delegate} itself is
     * returned. When this throws, {@code delegate} is already closed.
     */
    public static ShardReader withSyntheticFallback(ShardReader delegate, Path shardDir, String indexName,
                                                    JsonNode rawIndexMetadata) throws IOException {
        return SyntheticSourceFallbackShardReader.wrap(delegate, shardDir, indexName, rawIndexMetadata);
    }
}
