package de.sloow.gefrierschrank.core.repo;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** Reads the index mappings embedded in the repository metadata. */
class IndexMappingTest {

    private static RepositoryInspector inspector;

    @BeforeAll
    static void setup() {
        inspector = new RepositoryInspector(
            new FileSystemBlobStore(Path.of("src/test/resources/fixtures/demo-repo")));
    }

    @Test
    void parsesEs8IndexMapping() throws IOException {
        IndexMapping mapping = inspector.indexMapping("test_data_8_17_0");
        assertEquals("text", mapping.properties().get("description").get("type").asText());
        assertEquals("keyword", mapping.properties().get("category").get("type").asText());
        assertEquals("date", mapping.properties().get("created_at").get("type").asText());
        assertEquals("integer", mapping.properties().get("id").get("type").asText());
        assertEquals("boolean", mapping.properties().get("in_stock").get("type").asText());
        assertEquals("float", mapping.properties().get("price").get("type").asText());
    }
}
