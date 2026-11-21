package de.sloow.gefrierschrank.lucene10;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

/**
 * Collects {@code _source}, {@code _id} and {@code _routing} of one document in a single stored-field
 * visit, skipping every other field. Reproduces what the previous full-document read resolved through
 * {@code Document.getField}/{@code getBinaryValue}/{@code get}: the first {@code _source} value of any
 * type decides stored vs synthetic; the first binary {@code _id} wins over an earlier string one;
 * {@code _routing} and a non-binary {@code _id} take the first string-like value, numbers rendered
 * with {@code Number.toString} as {@code Field.stringValue} does.
 */
final class SourceIdentityVisitor extends StoredFieldVisitor {

    boolean sawSource;
    /** First _source value when it is binary; null when absent or when the first value is not binary. */
    byte[] source;
    byte[] idBinary;
    String idText;
    String routing;

    void reset() {
        sawSource = false;
        source = null;
        idBinary = null;
        idText = null;
        routing = null;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        return switch (fieldInfo.name) {
            case "_source", "_id", "_routing" -> Status.YES;
            default -> Status.NO;
        };
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
        switch (fieldInfo.name) {
            case "_source" -> {
                if (!sawSource) {
                    sawSource = true;
                    source = value;
                }
            }
            case "_id" -> {
                if (idBinary == null) idBinary = value;
            }
            default -> { /* a binary _routing has no string value */ }
        }
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) {
        stringLike(fieldInfo.name, value);
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) {
        stringLike(fieldInfo.name, Integer.toString(value));
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) {
        stringLike(fieldInfo.name, Long.toString(value));
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) {
        stringLike(fieldInfo.name, Float.toString(value));
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) {
        stringLike(fieldInfo.name, Double.toString(value));
    }

    private void stringLike(String field, String value) {
        switch (field) {
            case "_source" -> sawSource = true;
            case "_id" -> {
                if (idText == null) idText = value;
            }
            case "_routing" -> {
                if (routing == null) routing = value;
            }
            default -> { }
        }
    }
}
