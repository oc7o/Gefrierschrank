package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.lucene.DocumentMetadata;

/**
 * One archive document row on its way to Parquet: the stored {@code _source} as bytes
 * {@code [0, length)} of {@code source}, which are valid UTF-8 and exactly the bytes the table
 * stores, plus the document's identity. The array is handed to the writer without a copy, so a
 * producer passes a fresh array per row and the writer appends each row before asking for the next.
 */
record ArchiveRow(byte[] source, int length, DocumentMetadata metadata) {}
