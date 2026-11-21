package de.sloow.gefrierschrank.syntheticsource;

import de.sloow.gefrierschrank.core.FailureDiagnostics;

import java.io.IOException;

/**
 * Elasticsearch failed to put a document's excluded vectors back into its {@code _source}. Carries
 * only the failure's class name and its stack locations: neither its message nor a cause, which may
 * hold document content, reaches a report.
 */
public final class VectorRehydrationException extends IOException implements FailureDiagnostics.SafeMessage {
    public VectorRehydrationException(Throwable failure) {
        super("vectors kept out of _source could not be put back (" + failure.getClass().getSimpleName() + ")");
        setStackTrace(failure.getStackTrace());
    }
}
