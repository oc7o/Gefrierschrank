package de.sloow.gefrierschrank.delta;

import java.io.IOException;

/**
 * The Delta target could not store data: disk full, file too large, quota or I/O error. Fatal for
 * the whole run, because every later shard would be downloaded in full only to fail the same way.
 */
public final class TargetStorageException extends IOException {
    public TargetStorageException(String reason, Throwable cause) {
        super("target storage failure (" + reason + ")", cause);
    }
}
