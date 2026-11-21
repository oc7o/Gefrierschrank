package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The inputs a worker gets for rebuilding documents without stored {@code _source}. */
class SyntheticSourceReconstructorRewriteTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private String previous;

    @BeforeEach
    void saveKillSwitch() {
        previous = System.getProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY);
    }

    @AfterEach
    void restoreKillSwitch() {
        if (previous == null) System.clearProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY);
        else System.setProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, previous);
    }

    private static JsonNode metadata(String source) throws Exception {
        return JSON.readTree("""
            {"settings":{"index":{"version":{"created":"8521000"},"mapping":{"source":{"mode":"stored"}}}},
             "mappings":[{"_doc":{"_source":%s,"dynamic_templates":[{"x":{"match":"*"}}],
               "runtime":{"r":{"type":"keyword"}},"properties":{"a":{"type":"keyword"},"_source_like":{"type":"long"}}}}]}
            """.formatted(source));
    }

    @Test
    void theRewriteRemovesOnlyTheSourceObject() throws Exception {
        for (String source : new String[]{"{\"enabled\":false}", "{\"mode\":\"disabled\"}",
                "{\"enabled\":false,\"includes\":[\"a\"],\"excludes\":[\"b\"]}"}) {
            var inputs = SyntheticSourceReconstructor.reconstructionInputs(metadata(source));
            ObjectNode expected = (ObjectNode) JSON.readTree(inputs.mappingJson());
            assertTrue(expected.has("_source"));
            expected.remove("_source");
            assertEquals(expected, JSON.readTree(inputs.rewriteMappingJson()), source);
            assertFalse(JSON.readTree(inputs.rewriteMappingJson()).has("dynamic_templates"), "removed for every worker alike");
            assertTrue(JSON.readTree(inputs.rewriteMappingJson()).has("runtime"));
        }
    }

    @Test
    void theSettingsOverrideReplacesStoredAndDisabled() throws Exception {
        var inputs = SyntheticSourceReconstructor.reconstructionInputs(metadata("{\"enabled\":false}"));
        assertEquals("stored", inputs.flatSettings().get("index.mapping.source.mode"), "the recorded settings stay");
        for (String mode : new String[]{"stored", "disabled"}) {
            Map<String, String> rewritten = rewriteSettings(Map.of("index.mapping.source.mode", mode, "index.version.created", "8521000"));
            assertEquals("synthetic", rewritten.get("index.mapping.source.mode"));
            assertEquals("8521000", rewritten.get("index.version.created"));
        }
    }

    @Test
    void theKillSwitchLeavesNoRewrite() throws Exception {
        System.setProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, "false");
        assertNull(SyntheticSourceReconstructor.reconstructionInputs(metadata("{\"enabled\":false}")).rewriteMappingJson());
        assertFalse(SyntheticCoverage.reconstructsDisabledSource());
        System.setProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, "FALSE");
        assertFalse(SyntheticCoverage.reconstructsDisabledSource());
        System.setProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY, "true");
        assertTrue(SyntheticCoverage.reconstructsDisabledSource());
        System.clearProperty(SyntheticSourceReconstructor.RECONSTRUCT_DISABLED_SOURCE_PROPERTY);
        assertTrue(SyntheticCoverage.reconstructsDisabledSource(), "on by default");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> rewriteSettings(Map<String, String> settings) throws Exception {
        IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();
        var method = Class.forName("de.sloow.gefrierschrank.syntheticsource.isolated.ReconstructionCoverage", true, loader)
            .getDeclaredMethod("rewriteSettings", Map.class);
        method.setAccessible(true);
        return (Map<String, String>) method.invoke(null, settings);
    }
}
