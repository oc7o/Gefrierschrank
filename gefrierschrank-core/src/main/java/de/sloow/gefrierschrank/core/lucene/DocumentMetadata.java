package de.sloow.gefrierschrank.core.lucene;

/** Stored identity/routing accompany source; the slot is only local to this snapshot shard. */
public record DocumentMetadata(String id, String routing, String sourceKind, int slot) {}
