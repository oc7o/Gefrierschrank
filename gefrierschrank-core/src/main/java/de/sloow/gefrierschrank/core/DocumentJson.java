package de.sloow.gefrierschrank.core;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JSON handling for document {@code _source}. Jackson's default read constraints reject strings
 * over 20M characters, but Elasticsearch accepts them: Kibana's {@code .reporting-*} indices store
 * whole generated PDFs/PNGs as base64 strings. Stored source must export unchanged, so only
 * nesting depth keeps Jackson's default limit.
 */
public final class DocumentJson {

    private DocumentJson() {}

    public static ObjectMapper newMapper() {
        JsonFactory factory = JsonFactory.builder()
            .streamReadConstraints(StreamReadConstraints.builder()
                .maxStringLength(Integer.MAX_VALUE)
                .maxNumberLength(Integer.MAX_VALUE)
                .build())
            .build();
        return new ObjectMapper(factory);
    }
}
