package de.sloow.gefrierschrank.syntheticsource;

import de.sloow.gefrierschrank.core.FailureDiagnostics;

import java.io.IOException;

/**
 * The shard does not match the vector plan its index was admitted with: Elasticsearch's mapper,
 * built for this shard, names other excluded vector fields than the index-level probe did, offers
 * no loader for them, or sees another number of documents than the export reader. Fixed message:
 * no field name or document content reaches a report.
 */
public final class SourceVectorsMismatchException extends IOException implements FailureDiagnostics.SafeMessage {
    public SourceVectorsMismatchException() {
        super("the shard does not match the index's plan for vectors kept out of _source; "
            + "it is not exported without its vectors");
    }
}
