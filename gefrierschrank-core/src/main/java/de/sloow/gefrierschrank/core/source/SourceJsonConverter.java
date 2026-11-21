package de.sloow.gefrierschrank.core.source;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.StreamWriteFeature;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Renders a stored SMILE, CBOR or YAML {@code _source} as the compact JSON text Elasticsearch
 * returns for it: a token-by-token copy that mirrors x-content 9.3.0's
 * {@code XContentGenerator.copyCurrentEvent}, read with the parser settings of its
 * {@code *XContentImpl} factories and written by a default JSON generator.
 *
 * <p>Deliberate differences from Elasticsearch: duplicate keys are kept (Elasticsearch rejects
 * them when reading, the JSON path keeps them byte-exact); a BigDecimal is written as
 * {@link java.math.BigDecimal#toString()} (Elasticsearch drops it and fails the document); YAML's
 * {@code .nan} and {@code .inf}, which the YAML parser cannot read, become the strings written for
 * non-finite numbers; names are capped at Jackson's default length, as on the JSON path. Thread-safe.
 */
public final class SourceJsonConverter {

    /** Bytes {@code [0, length)} of a fresh array the caller owns. */
    public record Json(byte[] bytes, int length) {}

    private static final StreamReadConstraints CONSTRAINTS = StreamReadConstraints.builder()
        .maxStringLength(Integer.MAX_VALUE)
        .maxNumberLength(Integer.MAX_VALUE)
        .build();

    private static final SmileFactory SMILE = SmileFactory.builder()
        .streamReadConstraints(CONSTRAINTS)
        .disable(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW)
        .disable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
        .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
        .build();
    private static final CBORFactory CBOR = CBORFactory.builder()
        .streamReadConstraints(CONSTRAINTS)
        .disable(JsonFactory.Feature.FAIL_ON_SYMBOL_HASH_OVERFLOW)
        .disable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
        .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
        .build();
    private static final YAMLFactory YAML = YAMLFactory.builder()
        .streamReadConstraints(CONSTRAINTS)
        .disable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
        .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
        .enable(YAMLParser.Feature.EMPTY_STRING_AS_NULL)
        .build();
    // Elasticsearch's JsonXContent generator: Jackson's defaults, set explicitly so a dependency
    // upgrade cannot change them (non-finite numbers as the quoted strings "NaN", "Infinity", ...).
    private static final JsonFactory JSON = JsonFactory.builder()
        .enable(JsonWriteFeature.QUOTE_FIELD_NAMES)
        .enable(JsonWriteFeature.WRITE_NAN_AS_STRINGS)
        .disable(JsonWriteFeature.ESCAPE_NON_ASCII)
        .disable(JsonWriteFeature.WRITE_NUMBERS_AS_STRINGS)
        .disable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN)
        .disable(StreamWriteFeature.USE_FAST_DOUBLE_WRITER)
        .disable(StreamWriteFeature.AUTO_CLOSE_CONTENT)
        .build();

    // The SMILE and CBOR parsers do not check StreamReadConstraints' name length; the JSON path does.
    private static final int MAX_NAME_LENGTH = CONSTRAINTS.getMaxNameLength();

    private SourceJsonConverter() {}

    /**
     * Converts one stored document. The root must be one object with nothing after it, so a
     * multi-document YAML stream fails.
     *
     * @throws SourceConversionException for anything that is not such a document, without its text
     * @throws IllegalArgumentException for a format that is not converted
     */
    public static Json toJson(byte[] bytes, int length, SourceFormat format) throws SourceConversionException {
        JsonFactory factory = switch (format) {
            case SMILE -> SMILE;
            case CBOR -> CBOR;
            case YAML -> YAML;
            default -> throw new IllegalArgumentException("not a converted format: " + format);
        };
        var out = new Output(length);
        try (JsonParser parser = factory.createParser(bytes, 0, length);
             JsonGenerator generator = JSON.createGenerator(out)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) throw new SourceConversionException(format, "root is not an object");
            generator.writeStartObject();
            int depth = 1;
            while (depth > 0) {
                JsonToken token = parser.nextToken();
                if (token == null) throw new SourceConversionException(format, "truncated");
                switch (token) {
                    case START_OBJECT -> {
                        generator.writeStartObject();
                        depth++;
                    }
                    case END_OBJECT -> {
                        generator.writeEndObject();
                        depth--;
                    }
                    case START_ARRAY -> {
                        generator.writeStartArray();
                        depth++;
                    }
                    case END_ARRAY -> {
                        generator.writeEndArray();
                        depth--;
                    }
                    case FIELD_NAME -> {
                        String name = parser.currentName();
                        if (name.length() > MAX_NAME_LENGTH) throw new SourceConversionException(format, "name too long");
                        generator.writeFieldName(name);
                    }
                    case VALUE_STRING -> {
                        if (parser.hasTextCharacters()) {
                            generator.writeString(parser.getTextCharacters(), parser.getTextOffset(), parser.getTextLength());
                        } else {
                            generator.writeString(parser.getText());
                        }
                    }
                    case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> {
                        String nonFinite = format == SourceFormat.YAML && token == JsonToken.VALUE_NUMBER_FLOAT
                            ? yamlNonFinite(parser.getText()) : null;
                        if (nonFinite != null) {
                            generator.writeString(nonFinite);
                        } else {
                            copyNumber(parser, generator);
                        }
                    }
                    case VALUE_TRUE, VALUE_FALSE -> generator.writeBoolean(parser.getBooleanValue());
                    case VALUE_NULL -> generator.writeNull();
                    case VALUE_EMBEDDED_OBJECT -> generator.writeBinary(parser.getBinaryValue());
                    default -> throw new SourceConversionException(format, "unexpected token");
                }
            }
            if (parser.nextToken() != null) throw new SourceConversionException(format, "content after the root object");
            generator.flush();
        } catch (SourceConversionException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            throw new SourceConversionException(format, failure.getClass().getSimpleName());
        }
        return out.json();
    }

    /** By the parser's number type, as copyCurrentEvent does; BIG_DECIMAL, which it drops, as its toString. */
    private static void copyNumber(JsonParser parser, JsonGenerator generator) throws IOException {
        switch (parser.getNumberType()) {
            case INT -> generator.writeNumber(parser.getIntValue());
            case LONG -> generator.writeNumber(parser.getLongValue());
            case FLOAT -> generator.writeNumber(parser.getFloatValue());
            case DOUBLE -> generator.writeNumber(parser.getDoubleValue());
            case BIG_INTEGER -> generator.writeNumber(parser.getBigIntegerValue());
            case BIG_DECIMAL -> generator.writeNumber(parser.getDecimalValue());
        }
    }

    /**
     * YAML 1.1's {@code .nan}, {@code .inf} and {@code -.inf}, which Jackson's YAML parser types as
     * floats but cannot read, as the strings the JSON generator writes for NaN and the infinities.
     */
    private static String yamlNonFinite(String text) {
        return switch (text) {
            case ".nan", ".NaN", ".NAN" -> "NaN";
            case ".inf", ".Inf", ".INF", "+.inf", "+.Inf", "+.INF" -> "Infinity";
            case "-.inf", "-.Inf", "-.INF" -> "-Infinity";
            default -> null;
        };
    }

    /**
     * The memory gate admits a converted document at its length, so the array handed out stays
     * close to that: the buffer itself when little of it is unused, else an exact copy.
     */
    private static final class Output extends ByteArrayOutputStream {
        Output(int inputLength) {
            super((int) Math.min(Integer.MAX_VALUE - 8, 64L + inputLength + inputLength / 2));
        }

        Json json() {
            byte[] bytes = buf.length - count > count / 8 + 64 ? Arrays.copyOf(buf, count) : buf;
            return new Json(bytes, count);
        }
    }
}
