package de.sloow.gefrierschrank.core.repo;

import de.sloow.gefrierschrank.core.FailureDiagnostics;

import java.io.IOException;

/**
 * An index structure that a selectively fetched shard did not download was read. The pruned
 * readers throw it instead of answering "no values", so an unplanned access fails loudly and the
 * shard is completed and read again in full instead of silently losing data.
 */
public final class PrunedFormatAccessException extends IOException implements FailureDiagnostics.SafeMessage {
    public PrunedFormatAccessException() {
        super("a pruned index structure was read (selective fetch)");
    }
}
