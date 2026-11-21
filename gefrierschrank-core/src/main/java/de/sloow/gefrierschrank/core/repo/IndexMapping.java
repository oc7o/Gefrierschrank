package de.sloow.gefrierschrank.core.repo;

import com.fasterxml.jackson.databind.JsonNode;

/** The ES mapping of an index, as stored in the repository metadata. */
public record IndexMapping(JsonNode properties) {}
