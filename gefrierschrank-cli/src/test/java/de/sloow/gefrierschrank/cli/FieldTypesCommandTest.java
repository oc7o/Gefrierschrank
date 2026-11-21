package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FieldTypesCommandTest {

    private final ObjectMapper json = new ObjectMapper();

    private Map<String, List<String>> collect(String propertiesJson) throws Exception {
        Map<String, List<String>> into = new TreeMap<>();
        FieldTypesCommand.collectTypes(json.readTree(propertiesJson), "", into);
        return into;
    }

    @Test
    void collectsTopLevelFieldTypes() throws Exception {
        var types = collect("""
            {
              "host":  {"type": "keyword"},
              "cpu":   {"type": "scaled_float", "scaling_factor": 100},
              "count": {"type": "long"}
            }
            """);

        assertEquals(List.of("cpu"), types.get("scaled_float"));
        assertEquals(List.of("host"), types.get("keyword"));
        assertEquals(List.of("count"), types.get("long"));
    }

    /**
     * An unregistered type nested inside an object still fails the whole mapping merge, so the
     * walk must recurse rather than only reading the top level.
     */
    @Test
    void recursesIntoObjectProperties() throws Exception {
        var types = collect("""
            {
              "request": {
                "properties": {
                  "latency": {
                    "properties": {
                      "histogram": {"type": "histogram"}
                    }
                  }
                }
              }
            }
            """);

        assertEquals(List.of("request.latency.histogram"), types.get("histogram"));
        assertTrue(types.get("object").contains("request"));
    }

    /** Multi-fields ("fields") are a second nesting mechanism and fail the merge just the same. */
    @Test
    void recursesIntoMultiFields() throws Exception {
        var types = collect("""
            {
              "message": {
                "type": "match_only_text",
                "fields": {
                  "raw": {"type": "wildcard"}
                }
              }
            }
            """);

        assertEquals(List.of("message"), types.get("match_only_text"));
        assertEquals(List.of("message.raw"), types.get("wildcard"));
    }

    /** An object field carries no explicit "type", but should still be reported as one. */
    @Test
    void treatsUntypedObjectAsObjectType() throws Exception {
        var types = collect("""
            {"labels": {"properties": {"env": {"type": "keyword"}}}}
            """);

        assertEquals(List.of("labels"), types.get("object"));
    }

    @Test
    void toleratesEmptyOrMissingProperties() throws Exception {
        assertTrue(collect("{}").isEmpty());

        Map<String, List<String>> into = new TreeMap<>();
        FieldTypesCommand.collectTypes(null, "", into);
        assertTrue(into.isEmpty());
    }

    /**
     * A mapping can hold several types outside Elasticsearch core, such as the
     * aggregate_metric_double / histogram pair a downsampled index can use; all of them must be
     * reported in one pass, before any export starts.
     */
    @Test
    void reportsEveryDistinctTypeInOnePass() throws Exception {
        var types = collect("""
            {
              "svc":     {"type": "keyword"},
              "latency": {"type": "aggregate_metric_double", "metrics": ["min","max"], "default_metric": "max"},
              "sizes":   {"type": "histogram"},
              "ratio":   {"type": "scaled_float", "scaling_factor": 1000},
              "kind":    {"type": "constant_keyword", "value": "metric"}
            }
            """);

        assertEquals(
            new ArrayList<>(List.of("aggregate_metric_double", "constant_keyword", "histogram",
                "keyword", "scaled_float")),
            new ArrayList<>(types.keySet()));
    }
}
