package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.repo.IndexMapping;
import io.delta.standalone.types.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Infers a flat table schema from the first JSON document and converts values
 * into Avro records for the Parquet writer. Mapping rules:
 *
 * <ul>
 *   <li>int → long (safe widening; ES has no int/long overflow distinction in JSON)</li>
 *   <li>number → double</li>
 *   <li>boolean → boolean</li>
 *   <li>string → string</li>
 *   <li>null → union-free nullable long (typed on first non-null value; the
 *       inferrer only sees the whole document set, so null-only fields become
 *       strings to avoid trapping later values)</li>
 *   <li>object/array → JSON string (schema stability across heterogeneous docs)</li>
 * </ul>
 */
final class JsonSchemaMapper {

    /** Column holding fields present in a document but not declared in the ES mapping, as a JSON object. */
    static final String UNMAPPED_FIELDS_FIELD = "_unmapped_fields";

    private static final ObjectMapper JSON = new ObjectMapper();

    private StructType deltaSchema;
    private Schema avroSchema;
    // non-null only when schema came from an ES mapping; enables unmapped-field detection
    private Set<String> mappedFieldNames;
    // Avro (sanitized) field name -> the real JSON/ES field name to read the value from. Only
    // populated for fields that actually needed sanitizing; see sanitizeAvroName().
    private java.util.Map<String, String> avroNameToJsonName = java.util.Map.of();

    boolean notYetInferred() {
        return deltaSchema == null;
    }

    void inferFrom(JsonNode firstDocument) {
        List<StructField> fields = new ArrayList<>();
        List<Schema.Field> avroFields = new ArrayList<>();
        java.util.Map<String, String> renamed = new java.util.LinkedHashMap<>();
        firstDocument.fieldNames().forEachRemaining(name -> {
            JsonNode value = firstDocument.get(name);
            String avroType = avroTypeOf(value);
            String avroName = sanitizeAvroName(name, renamed);
            fields.add(new StructField(avroName,
                deltaTypeOf(avroType), true));
            avroFields.add(new Schema.Field(avroName,
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.valueOf(avroType))),
                null, null));
        });
        this.avroNameToJsonName = renamed;
        this.deltaSchema = new StructType(fields.toArray(new StructField[0]));
        this.avroSchema = Schema.createRecord("gefrierschrank_doc", null, null, false, avroFields);
    }

    /**
     * Types the schema from the ES index mapping's declared field types, instead of guessing from
     * document values. Always adds a trailing {@link #UNMAPPED_FIELDS_FIELD} string column so that
     * fields present in a document but absent from the mapping aren't silently dropped.
     */
    void inferFromEsMapping(IndexMapping mapping) {
        List<StructField> fields = new ArrayList<>();
        List<Schema.Field> avroFields = new ArrayList<>();
        Set<String> names = new java.util.LinkedHashSet<>();
        java.util.Map<String, String> renamed = new java.util.LinkedHashMap<>();

        Iterator<String> fieldNames = mapping.properties().fieldNames();
        while (fieldNames.hasNext()) {
            String name = fieldNames.next();
            names.add(name);
            String esType = mapping.properties().get(name).path("type").asText();
            String avroType = avroTypeOfEsType(esType);
            String avroName = sanitizeAvroName(name, renamed);
            fields.add(new StructField(avroName, deltaTypeOf(avroType), true));
            avroFields.add(new Schema.Field(avroName,
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.valueOf(avroType))),
                null, null));
        }
        fields.add(new StructField(UNMAPPED_FIELDS_FIELD, new StringType(), true));
        avroFields.add(new Schema.Field(UNMAPPED_FIELDS_FIELD,
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)),
            null, null));

        this.mappedFieldNames = names;
        this.avroNameToJsonName = renamed;
        this.deltaSchema = new StructType(fields.toArray(new StructField[0]));
        this.avroSchema = Schema.createRecord("gefrierschrank_doc", null, null, false, avroFields);
    }

    /** The committed schema determines every later Parquet file's physical column types. */
    void useTableSchema(StructType existing) {
        Set<String> existingNames = new java.util.HashSet<>();
        for (StructField field : existing.getFields()) existingNames.add(field.getName());
        for (StructField field : deltaSchema.getFields()) {
            if (!existingNames.contains(field.getName())) {
                throw new IllegalArgumentException("document schema adds columns to an existing table; schema evolution is required");
            }
        }
        List<Schema.Field> fields = new ArrayList<>();
        for (StructField field : existing.getFields()) {
            DataType type = field.getDataType();
            Schema.Type avroType;
            if (type instanceof LongType) avroType = Schema.Type.LONG;
            else if (type instanceof DoubleType) avroType = Schema.Type.DOUBLE;
            else if (type instanceof BooleanType) avroType = Schema.Type.BOOLEAN;
            else if (type instanceof StringType) avroType = Schema.Type.STRING;
            else throw new IllegalArgumentException("existing table has an unsupported column type");
            fields.add(new Schema.Field(field.getName(),
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(avroType)), null, null));
        }
        deltaSchema = existing;
        avroSchema = Schema.createRecord("gefrierschrank_doc", null, null, false, fields);
    }

    /**
     * The real JSON/ES field name to read a document value from for a given Avro field, or the
     * Avro field's own name if it needed no sanitizing (the common case).
     */
    String jsonFieldNameFor(String avroFieldName) {
        return avroNameToJsonName.getOrDefault(avroFieldName, avroFieldName);
    }

    /**
     * Avro record field names must match {@code [A-Za-z_][A-Za-z0-9_]*} - ES field names are
     * under no such constraint (notably {@code @timestamp}, ubiquitous on TSDB/data-stream
     * indices, starts with '@'). Since Parquet's physical columns are derived directly from this
     * Avro schema, the SANITIZED name must be used consistently for both the Avro and Delta
     * schemas (a mismatch between Delta's declared schema and the Parquet file's actual columns
     * would make the table unreadable) - only the JSON lookup uses the original name, via
     * {@link #jsonFieldNameFor}.
     */
    private static String sanitizeAvroName(String name, java.util.Map<String, String> renamed) {
        if (name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return name;
        }
        StringBuilder sanitized = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean valid = Character.isLetterOrDigit(c) || c == '_';
            boolean validFirst = i == 0 && (Character.isLetter(c) || c == '_');
            sanitized.append(i == 0 ? (validFirst ? c : '_') : (valid ? c : '_'));
        }
        String avroName = sanitized.toString();
        renamed.put(avroName, name);
        return avroName;
    }

    /**
     * JSON object of the document's fields that aren't in the ES mapping, or null if there are
     * none (or the schema wasn't built from a mapping). Backs the {@link #UNMAPPED_FIELDS_FIELD} column.
     */
    String unmappedFieldsJson(JsonNode doc) {
        if (mappedFieldNames == null) return null;
        ObjectNode extra = null;
        Iterator<String> names = doc.fieldNames();
        while (names.hasNext()) {
            String name = names.next();
            if (!mappedFieldNames.contains(name)) {
                if (extra == null) extra = JSON.createObjectNode();
                extra.set(name, doc.get(name));
            }
        }
        return extra == null ? null : extra.toString();
    }

    StructType deltaSchema() {
        return deltaSchema;
    }

    Schema avroSchema() {
        return avroSchema;
    }

    private static String avroTypeOfEsType(String esType) {
        return switch (esType) {
            case "keyword", "text", "wildcard", "constant_keyword" -> "STRING";
            case "integer", "short", "byte", "long" -> "LONG";
            case "float", "half_float", "double", "scaled_float" -> "DOUBLE";
            case "boolean" -> "BOOLEAN";
            default -> "STRING";  // date, object, nested, geo_point, geo_shape, unknown: raw string
        };
    }

    private static String avroTypeOf(JsonNode value) {
        if (value == null || value.isNull()) return "STRING";  // null-only field: safest fallback
        if (value.isInt() || value.isLong()) return "LONG";
        if (value.isNumber()) return "DOUBLE";
        if (value.isTextual()) return "STRING";
        if (value.isBoolean()) return "BOOLEAN";
        return "STRING";  // object/array: JSON-encoded
    }

    private static DataType deltaTypeOf(String avroType) {
        return switch (avroType) {
            case "LONG" -> new LongType();
            case "DOUBLE" -> new DoubleType();
            case "BOOLEAN" -> new BooleanType();
            default -> new StringType();
        };
    }

    /** Converts one JSON value to its Avro representation per the field schema. */
    static Object avroValue(JsonNode value, Schema fieldSchema) {
        Schema effective = fieldSchema.getTypes().get(1);  // union [null, T]: pick T
        if (value == null || value.isNull()) return null;
        return switch (effective.getName()) {
            case "long" -> {
                if (!value.isIntegralNumber() || !value.canConvertToLong())
                    throw new IllegalArgumentException("value cannot be represented as a long column");
                yield value.longValue();
            }
            case "double" -> {
                if (!value.isNumber()) throw new IllegalArgumentException("value cannot be represented as a double column");
                yield value.doubleValue();
            }
            case "boolean" -> {
                if (!value.isBoolean()) throw new IllegalArgumentException("value cannot be represented as a boolean column");
                yield value.booleanValue();
            }
            case "string" -> value.isTextual() ? value.asText() : value.toString();
            default -> null;
        };
    }
}
