package de.sloow.gefrierschrank.core.repo;

import java.util.Optional;

/** Size/type info for a single blob in a snapshot repository. */
public record BlobMetadata(String key, long size) {}
