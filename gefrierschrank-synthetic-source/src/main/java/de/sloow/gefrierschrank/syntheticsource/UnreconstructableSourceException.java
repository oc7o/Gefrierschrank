package de.sloow.gefrierschrank.syntheticsource;

import de.sloow.gefrierschrank.core.FailureDiagnostics;

import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;

/**
 * Documents without stored {@code _source} cannot be rebuilt from what the index wrote: a field
 * that holds data has no doc values or stored field Elasticsearch's loaders could read, or the
 * mapping lets values leave no trace at all. The message names only tokens of a closed vocabulary
 * (Elasticsearch mapper type names with fixed qualifiers), never a field name or document text.
 * Ambient, so the isolated worker and the preflight throw the same class.
 */
public final class UnreconstructableSourceException extends IOException implements FailureDiagnostics.SafeMessage {

    static final String MESSAGE =
        "documents without stored _source cannot be reconstructed from this index's doc values and stored fields: ";

    private final String tokens;

    public UnreconstructableSourceException(Collection<String> tokens) {
        this(String.join(" ", new TreeSet<>(tokens)));
    }

    private UnreconstructableSourceException(String tokens) {
        super(MESSAGE + tokens);
        this.tokens = tokens;
    }

    /** The sorted, de-duplicated tokens, space separated. */
    public String tokens() {
        return tokens;
    }
}
