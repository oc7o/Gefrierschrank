package de.sloow.gefrierschrank.delta;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Ordered value equality of JSON documents: Jackson's tree equality ignores the order of object
 * fields, so the field-name sequence is compared at every level as well.
 */
final class OrderedJsonAssert {

    private OrderedJsonAssert() {}

    static void assertOrderedEquals(JsonNode expected, JsonNode actual, String message) {
        assertEquals(expected, actual, message);
        assertEquals(fieldOrder(expected), fieldOrder(actual), message + " (field order)");
    }

    static void assertOrderedEquals(Map<String, JsonNode> expected, Map<String, JsonNode> actual) {
        assertEquals(expected.keySet(), actual.keySet(), "document ids");
        for (Map.Entry<String, JsonNode> entry : expected.entrySet()) {
            assertOrderedEquals(entry.getValue(), actual.get(entry.getKey()), entry.getKey());
        }
    }

    static List<String> fieldOrder(JsonNode node) {
        List<String> order = new ArrayList<>();
        collect(node, "", order);
        return order;
    }

    private static void collect(JsonNode node, String path, List<String> into) {
        if (node.isObject()) {
            node.properties().forEach(e -> {
                into.add(path + "/" + e.getKey());
                collect(e.getValue(), path + "/" + e.getKey(), into);
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) collect(node.get(i), path + "[" + i + "]", into);
        }
    }
}
