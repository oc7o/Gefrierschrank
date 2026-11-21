package de.sloow.gefrierschrank.core.source;

import de.sloow.gefrierschrank.core.FailureDiagnostics;

import java.io.IOException;

/**
 * A stored SMILE, CBOR or YAML {@code _source} that could not be converted to JSON. The message
 * names only the format and a fixed reason; the parser's own failure is neither copied nor chained,
 * because its message can quote the document.
 */
public final class SourceConversionException extends IOException implements FailureDiagnostics.SafeMessage {

    SourceConversionException(SourceFormat format, String reason) {
        super("stored _source in " + format + " format could not be converted to JSON (" + reason + ")");
    }
}
