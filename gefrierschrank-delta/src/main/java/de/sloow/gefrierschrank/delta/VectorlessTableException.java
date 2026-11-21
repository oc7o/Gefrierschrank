package de.sloow.gefrierschrank.delta;

import de.sloow.gefrierschrank.core.FailureDiagnostics;

import java.io.IOException;

/**
 * The target table of an index whose vectors are rehydrated holds files written without them
 * (Gefrierschrank 1.0.0 exported such indices with every excluded vector missing). Completing it
 * would mix rows with and without vectors, so the index fails before any shard is read.
 */
public final class VectorlessTableException extends IOException implements FailureDiagnostics.SafeMessage {
    public VectorlessTableException() {
        super("the table holds rows exported without their vectors by Gefrierschrank 1.0.0; "
            + "export the index to a new output root");
    }
}
