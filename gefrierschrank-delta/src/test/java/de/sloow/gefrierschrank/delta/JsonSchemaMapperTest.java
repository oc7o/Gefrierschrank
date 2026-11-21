package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.IndexMapping;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/** Schema built from a real ES index mapping, instead of guessed from document values. */
class JsonSchemaMapperTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static IndexMapping mapping;

    @BeforeAll
    static void setup() throws IOException {
        var inspector = new RepositoryInspector(
            new FileSystemBlobStore(Path.of("../gefrierschrank-core/src/test/resources/fixtures/demo-repo")));
        mapping = inspector.indexMapping("test_data_8_17_0");
    }

    private static StructField field(StructType schema, String name) {
        return Arrays.stream(schema.getFields())
            .filter(f -> f.getName().equals(name))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no field named " + name));
    }

    @Test
    void typesFieldsFromDeclaredEsMappingTypes() {
        var mapper = new JsonSchemaMapper();
        mapper.inferFromEsMapping(mapping);
        StructType schema = mapper.deltaSchema();

        assertInstanceOf(StringType.class, field(schema, "category").getDataType());
        assertInstanceOf(DoubleType.class, field(schema, "price").getDataType());
        assertInstanceOf(LongType.class, field(schema, "id").getDataType());
        assertInstanceOf(BooleanType.class, field(schema, "in_stock").getDataType());
        assertInstanceOf(StringType.class, field(schema, "description").getDataType());
    }

    @Test
    void schemaIncludesAllMappedColumnsEvenWhenADocumentOmitsSome() throws IOException {
        var mapper = new JsonSchemaMapper();
        mapper.inferFromEsMapping(mapping);
        StructType schema = mapper.deltaSchema();

        // A document that only sets a couple of the mapped fields must not shrink the schema:
        // every mapped column stays present so later, fuller documents remain writable.
        JsonNode sparse = JSON.readTree("{\"id\": 1, \"category\": \"widgets\"}");
        for (String mappedField : new String[] {"category", "price", "id", "in_stock", "description", "created_at", "title"}) {
            assertNotNull(field(schema, mappedField), "missing mapped column: " + mappedField);
        }
        assertNull(mapper.unmappedFieldsJson(sparse), "no unmapped fields in this document");
    }

    @Test
    void fieldsAbsentFromTheMappingAreCapturedNotDropped() throws IOException {
        var mapper = new JsonSchemaMapper();
        mapper.inferFromEsMapping(mapping);
        StructType schema = mapper.deltaSchema();

        assertNotNull(field(schema, JsonSchemaMapper.UNMAPPED_FIELDS_FIELD));

        JsonNode doc = JSON.readTree("{\"id\": 1, \"totally_unmapped_field\": \"surprise\"}");
        String unmapped = mapper.unmappedFieldsJson(doc);
        assertNotNull(unmapped, "field outside the mapping must be captured, not silently dropped");
        JsonNode unmappedNode = JSON.readTree(unmapped);
        assertEquals("surprise", unmappedNode.get("totally_unmapped_field").asText());
        assertFalse(unmappedNode.has("id"), "mapped fields must not be duplicated into _unmapped_fields");
    }

    /**
     * ES's own @timestamp field (ubiquitous on TSDB/data-stream indices, and a top-level JSON
     * field of every reconstructed synthetic source) is not a legal Avro record field name - Avro requires
     * [A-Za-z_][A-Za-z0-9_]*. Since Parquet's physical columns come directly from the Avro
     * schema, the sanitized name must be used consistently in BOTH schemas; only the JSON lookup
     * (jsonFieldNameFor) needs to know the real name.
     */
    @Test
    void semanticTextColumnsStayInSchemaAndReceiveNullWhenReconstructionOmitsThem() throws IOException {
        var mapper = new JsonSchemaMapper();
        IndexMapping semanticMapping = new IndexMapping(JSON.readTree("""
            {
              "message": {"type": "keyword"},
              "summary": {"type": "semantic_text"}
            }
            """));
        mapper.inferFromEsMapping(semanticMapping);

        StructField summary = field(mapper.deltaSchema(), "summary");
        JsonNode reconstructedWithoutSemanticText = JSON.readTree("{\"message\": \"still exported\"}");

        assertInstanceOf(StringType.class, summary.getDataType());
        assertNull(JsonSchemaMapper.avroValue(
            reconstructedWithoutSemanticText.get(mapper.jsonFieldNameFor(summary.getName())),
            mapper.avroSchema().getField(summary.getName()).schema()));
    }

    @Test
    void avroIllegalFieldNamesLikeTimestampAreSanitizedConsistently() throws IOException {
        var mapper = new JsonSchemaMapper();
        JsonNode doc = JSON.readTree(
            "{\"@timestamp\": \"2026-01-01T00:00:00Z\", \"host\": \"host-1\"}");
        mapper.inferFrom(doc);

        // The Avro/Delta schema must use a legal name - never the raw "@timestamp".
        boolean hasIllegalName = java.util.Arrays.stream(mapper.deltaSchema().getFields())
            .anyMatch(f -> f.getName().equals("@timestamp"));
        assertFalse(hasIllegalName, "the raw ES field name must never reach the Avro/Parquet schema");

        // The sanitized field must still resolve back to the correct JSON value.
        String sanitizedName = java.util.Arrays.stream(mapper.deltaSchema().getFields())
            .map(StructField::getName)
            .filter(n -> !n.equals("host"))
            .findFirst()
            .orElseThrow();
        assertEquals("@timestamp", mapper.jsonFieldNameFor(sanitizedName));

        var avroField = mapper.avroSchema().getField(sanitizedName);
        Object value = JsonSchemaMapper.avroValue(doc.get(mapper.jsonFieldNameFor(sanitizedName)), avroField.schema());
        assertEquals("2026-01-01T00:00:00Z", value);
    }
}
