package de.sloow.gefrierschrank.core.smile;

import java.util.Arrays;

/**
 * A decoded ChecksumBlobStoreFormat envelope: the codec metadata from the Lucene
 * header plus the raw SMILE payload bytes.
 */
public record Envelope(String codecName, int codecVersion, byte compressionMode, byte[] content) {
    public Envelope {
        content = content.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Envelope other)) return false;
        return codecVersion == other.codecVersion
            && compressionMode == other.compressionMode
            && codecName.equals(other.codecName)
            && Arrays.equals(content, other.content);
    }

    @Override
    public int hashCode() {
        int result = codecName.hashCode();
        result = 31 * result + codecVersion;
        result = 31 * result + compressionMode;
        result = 31 * result + Arrays.hashCode(content);
        return result;
    }
}
