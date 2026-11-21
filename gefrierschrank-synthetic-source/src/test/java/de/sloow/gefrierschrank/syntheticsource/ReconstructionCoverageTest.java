package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The end-to-end coverage matrix: every case of {@code coverage-matrix/cases.txt} indexes its
 * documents through Elasticsearch's own DocumentParser into a stored-mode index with {@code _source}
 * disabled and into a native-synthetic twin, then reconstructs the stored-mode index through the
 * reconstruction worker's rewrite. Each document must equal the twin's native rendering (fields the
 * twin's {@code _ignored} lists removed, a {@code constant_keyword} value added where the document
 * lacks it), or differ only in fields its own {@code _ignored} lists while being counted, or the
 * case must refuse with exactly its named tokens. A case that is none of these fails the build.
 */
class ReconstructionCoverageTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path CASES = Path.of("src/test/resources/coverage-matrix/cases.txt");
    private static final String PROBE = "de.sloow.gefrierschrank.syntheticsource.isolated.CoverageMatrixProbe";
    /** CoverageMatrixProbe.UPDATE: a mapping update between documents. */
    private static final String UPDATE = "@update ";
    private static final Pattern TOKEN = Pattern.compile(
        "object:enabled-false|dynamic:false|dynamic:runtime|nested:keep-all|runtime:shadowing|flattened:ignore_above"
            + "|unmapped|mapping-rejected:[A-Za-z]+|[a-z_]+:(not-indexed|diverted|fallback|copy_to|no-native-source)");

    record Case(String name, Map<String, String> settings, String mapping, String expect, List<String> documents, String inject,
                String preflight, String pruned) {}

    @Test
    void everyCaseIsReconstructedCountedOrRefusedByName(@TempDir Path tmp) throws Exception {
        List<String> failures = new ArrayList<>();
        Set<String> provenDiversions = new TreeSet<>();
        int index = 0;
        for (Case c : cases()) {
            int before = failures.size();
            try {
                JsonNode result = check(c, tmp.resolve("case" + index++), failures);
                if (result != null && failures.size() == before) {
                    for (JsonNode doc : result.path("docs")) {
                        if (doc.path("counted").asBoolean()) result.path("acceptedDiversions").forEach(t -> provenDiversions.add(t.asText()));
                    }
                }
            } catch (Exception | AssertionError e) {
                if (c.expect().equals("unbuildable") && !(e instanceof AssertionError)) continue;
                failures.add(c.name() + ": " + e);
            }
        }
        if (!failures.isEmpty()) fail(failures.size() + " case(s):\n" + String.join("\n", failures));
        // A type may divert values to a stored field stored mode never wrote only when a case counted
        // a document in which it did so; an unproven entry would let a value drop silently.
        assertEquals(new TreeSet<>(ignoredRecordedDiversions()), provenDiversions,
            "IGNORED_RECORDED_DIVERSIONS must be exactly the types the matrix proves");
    }

    @SuppressWarnings("unchecked")
    private static Set<String> ignoredRecordedDiversions() throws Exception {
        var field = Class.forName("de.sloow.gefrierschrank.syntheticsource.isolated.ReconstructionCoverage", true,
            IsolatedEsClassLoader.shared()).getDeclaredField("IGNORED_RECORDED_DIVERSIONS");
        field.setAccessible(true);
        return (Set<String>) field.get(null);
    }

    /** Without its accepted set, a type that diverts values to a stored field stored mode never wrote refuses. */
    @Test
    void aDiversionOutsideTheProvenSetRefuses() throws Exception {
        Map<String, String> settings = Map.of("index.number_of_shards", "1", "index.number_of_replicas", "0",
            "index.version.created", "9039000");
        String mapping = "{\"_source\":{\"enabled\":false},\"properties\":{\"k\":{\"type\":\"keyword\",\"ignore_above\":3},"
            + "\"n\":{\"type\":\"long\",\"ignore_malformed\":true},\"t\":{\"type\":\"text\",\"fields\":{\"raw\":{\"type\":\"keyword\"}}}}}";
        assertEquals("keyword:diverted long:diverted text:diverted", mappingRefusals(mapping, settings, Set.of()));
        assertEquals("", mappingRefusals(mapping, settings, Set.of("keyword", "long", "text")));
    }

    private static String mappingRefusals(String mapping, Map<String, String> settings, Set<String> accepted) throws Exception {
        IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            return (String) Class.forName(PROBE, true, loader).getMethod("mappingRefusals", String.class, Map.class, Set.class)
                .invoke(null, mapping, settings, accepted);
        } finally {
            thread.setContextClassLoader(previous);
        }
    }

    /** Every type the isolated MapperService registers appears in at least one case. */
    @Test
    void theMatrixCoversEveryRegisteredType() throws IOException {
        Set<String> used = new TreeSet<>();
        for (Case c : cases()) used.addAll(types(JSON.readTree(c.mapping())));
        Set<String> missing = new TreeSet<>(SYNTHETIC_MAPPER_TYPES);
        missing.removeAll(used);
        assertEquals(Set.of(), missing);
    }

    /** ExportPreflight.SYNTHETIC_MAPPER_TYPES (delta), which ExportPreflightTest keeps equal to the registry. */
    static final Set<String> SYNTHETIC_MAPPER_TYPES = Set.of(
        "aggregate_metric_double", "alias", "binary", "boolean", "byte", "completion", "constant_keyword",
        "counted_keyword", "date", "date_nanos", "date_range", "dense_vector", "double", "double_range",
        "flattened", "float", "float_range", "geo_point", "geo_shape", "half_float", "histogram", "integer",
        "integer_range", "ip", "ip_range", "keyword", "long", "long_range", "match_only_text", "nested",
        "object", "passthrough", "rank_feature", "rank_features", "scaled_float", "search_as_you_type",
        "shape", "short", "sparse_vector", "text", "token_count", "unsigned_long", "version", "wildcard");

    private static JsonNode check(Case c, Path tmp, List<String> failures) throws Exception {
        boolean optIn = c.expect().startsWith("optin");
        String previous = System.getProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY);
        JsonNode result;
        try {
            if (optIn) System.setProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY, SyntheticCoverage.ACCEPT_UNMAPPED_VALUES);
            ObjectNode mapping = (ObjectNode) JSON.readTree(c.mapping());
            ObjectNode withSource = JSON.createObjectNode();
            withSource.putObject("_source").put("enabled", false);
            withSource.setAll(mapping);
            result = JSON.readTree(run(JSON.writeValueAsString(withSource), c.settings(), c.documents(), tmp, c.inject()));
        } finally {
            if (previous == null) System.clearProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY);
            else System.setProperty(SyntheticCoverage.SOURCE_DISABLED_ACCEPT_PROPERTY, previous);
        }
        String report = System.getProperty("gefrierschrank.coverageMatrix.report");
        if (report != null) {
            Files.writeString(Path.of(report), c.name() + "\t" + result + "\n", StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        }
        if (c.expect().equals("unbuildable")) {
            failures.add(c.name() + ": expected the mapping not to build");
            return null;
        }
        if (!result.path("indexFailures").isEmpty()) {
            failures.add(c.name() + ": documents not indexed " + result.path("indexFailures"));
            return null;
        }
        checkPreflight(c, result, failures);
        checkPruned(c, result, failures);
        Set<String> refusals = new TreeSet<>();
        if (result.has("refused")) refusals.add(result.path("refused").asText());
        for (JsonNode doc : result.path("docs")) {
            if (doc.has("refused")) refusals.add(doc.path("refused").asText());
        }
        for (String refusal : refusals) {
            for (String token : refusal.split(" ")) {
                if (!inVocabulary(token)) failures.add(c.name() + ": token outside the vocabulary " + token);
            }
        }
        if (c.expect().startsWith("refused")) {
            String expected = c.expect().substring("refused".length()).strip();
            if (!refusals.equals(Set.of(expected))) failures.add(c.name() + ": expected refusal " + expected + ", got " + refusals
                + " " + result.path("docs"));
            return result;
        }
        if (!refusals.isEmpty()) {
            failures.add(c.name() + ": unexpected refusal " + refusals);
            return result;
        }
        boolean unordered = c.expect().equals("covered-unordered");
        List<String> dropped = optIn ? List.of(c.expect().substring("optin".length()).strip().split(",")) : List.of();
        if (optIn != result.path("losesUnmappedValues").asBoolean()) {
            failures.add(c.name() + ": losesUnmappedValues " + result.path("losesUnmappedValues"));
        }
        Map<String, String> constants = constants(JSON.readTree(c.mapping()), "");
        for (JsonNode doc : result.path("docs")) {
            String where = c.name() + " doc " + doc.path("doc").asInt();
            JsonNode actual = JSON.readTree(doc.path("rewrite").asText());
            JsonNode oracle = oracle(JSON.readTree(doc.path("native").asText()), doc.path("twinIgnored"), constants, dropped,
                JSON.readTree(c.mapping()));
            Set<String> storedIgnored = new TreeSet<>();
            doc.path("storedIgnored").forEach(n -> storedIgnored.add(n.asText()));
            if (doc.path("counted").asBoolean() != !storedIgnored.isEmpty()) {
                failures.add(where + ": counted " + doc.path("counted") + " but _ignored " + storedIgnored);
            }
            if (unordered) {
                oracle = sortedArrays(oracle);
                actual = sortedArrays(actual);
            }
            if (sameOrdered(oracle, actual)) continue;
            Set<String> differing = new TreeSet<>();
            diff(oracle, actual, "", differing);
            boolean onlyIgnored = !differing.isEmpty() && differing.stream().allMatch(path -> listed(path, storedIgnored));
            if (!onlyIgnored) {
                failures.add(where + ": rewrite " + actual + " != oracle " + oracle + " (native " + doc.path("native").asText()
                    + ", _ignored " + storedIgnored + ")");
            }
        }
        return result;
    }

    /**
     * The commit-point preflight's FieldInfos leaf refuses exactly as the real leaf on every segment,
     * except where the case names what the preflight cannot see ({@code preflight: <tokens>} or
     * {@code none}): there the real leaf must refuse and the FieldInfos leaf must give those tokens.
     */
    private static void checkPreflight(Case c, JsonNode result, List<String> failures) {
        boolean diverged = false;
        for (JsonNode leaf : result.path("leaves")) {
            String real = leaf.path("real").asText();
            String stub = leaf.path("stub").asText();
            if (real.equals(stub)) continue;
            diverged = true;
            String expected = c.preflight() == null ? null : c.preflight().equals("none") ? "" : c.preflight();
            if (expected == null || real.isEmpty() || !stub.equals(expected)) {
                failures.add(c.name() + ": preflight leaf [" + stub + "] but real leaf [" + real + "]");
            }
        }
        if (c.preflight() != null && !diverged) failures.add(c.name() + ": preflight line set but the leaves agree");
    }

    /**
     * A leaf whose {@code _field_names} postings are hidden refuses exactly as the real leaf, except
     * where the case names what it gives instead ({@code pruned: <tokens>}): a text field without
     * norms whose keyword multi-field must be counted has postings there but no {@code _field_names}
     * entry, so values without tokens cannot be seen and the leaf must refuse rather than pass.
     */
    private static void checkPruned(Case c, JsonNode result, List<String> failures) {
        boolean diverged = false;
        for (JsonNode leaf : result.path("leaves")) {
            String real = leaf.path("real").asText();
            String pruned = leaf.path("pruned").asText();
            if (real.equals(pruned)) continue;
            diverged = true;
            if (c.pruned() == null || !pruned.equals(c.pruned())) {
                failures.add(c.name() + ": leaf without _field_names [" + pruned + "] but real leaf [" + real + "]");
            }
        }
        if (c.pruned() != null && !diverged) failures.add(c.name() + ": pruned line set but the leaves agree");
    }

    private static JsonNode sortedArrays(JsonNode node) {
        if (node.isObject()) {
            ObjectNode copy = JSON.createObjectNode();
            node.properties().forEach(e -> copy.set(e.getKey(), sortedArrays(e.getValue())));
            return copy;
        }
        if (node.isArray()) {
            List<JsonNode> elements = new ArrayList<>();
            node.forEach(element -> elements.add(sortedArrays(element)));
            elements.sort(java.util.Comparator.comparing(JsonNode::toString));
            ArrayNode copy = JSON.createArrayNode();
            elements.forEach(copy::add);
            return copy;
        }
        return node;
    }

    /** A closed-vocabulary token: its qualifier follows an Elasticsearch type name, never a field name. */
    static boolean inVocabulary(String token) {
        if (!TOKEN.matcher(token).matches()) return false;
        int colon = token.indexOf(':');
        String head = colon < 0 ? token : token.substring(0, colon);
        return Set.of("unmapped", "mapping-rejected", "object", "dynamic", "nested", "runtime", "flattened").contains(head)
            || SYNTHETIC_MAPPER_TYPES.contains(head);
    }

    /** A differing path counts as ignored when _ignored lists it, a field below it, or its parent field (multi-fields). */
    private static boolean listed(String path, Set<String> ignored) {
        for (String field : ignored) {
            if (field.equals(path) || field.startsWith(path + ".") || path.startsWith(field + ".")) return true;
        }
        return false;
    }

    private static boolean sameOrdered(JsonNode a, JsonNode b) {
        return a.equals(b) && order(a).equals(order(b));
    }

    private static List<String> order(JsonNode node) {
        List<String> names = new ArrayList<>();
        collectOrder(node, "", names);
        return names;
    }

    private static void collectOrder(JsonNode node, String path, List<String> into) {
        if (node.isObject()) {
            node.properties().forEach(e -> {
                into.add(path + "/" + e.getKey());
                collectOrder(e.getValue(), path + "/" + e.getKey(), into);
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) collectOrder(node.get(i), path + "[" + i + "]", into);
        }
    }

    /** Dotted paths of the top-most fields where the two documents differ. */
    private static void diff(JsonNode expected, JsonNode actual, String path, Set<String> into) {
        if (expected.isObject() && actual.isObject()) {
            Set<String> names = new TreeSet<>();
            expected.fieldNames().forEachRemaining(names::add);
            actual.fieldNames().forEachRemaining(names::add);
            for (String name : names) {
                String child = path.isEmpty() ? name : path + "." + name;
                JsonNode e = expected.path(name);
                JsonNode a = actual.path(name);
                if (!e.equals(a)) diff(e, a, child, into);
            }
            if (expected.equals(actual) && !order(expected).equals(order(actual))) into.add(path.isEmpty() ? "<order>" : path);
        } else if (!expected.equals(actual)) {
            into.add(path.isEmpty() ? "<root>" : path);
        }
    }

    /**
     * The native-synthetic rendering with what the rewrite cannot know: values of fields the twin's
     * {@code _ignored} lists removed (with the text field whose keyword sub-field is listed), empty
     * objects pruned, values under opt-in objects dropped, constants added where missing.
     */
    private static JsonNode oracle(JsonNode nativeSource, JsonNode twinIgnored, Map<String, String> constants,
                                   List<String> dropped, JsonNode mapping) {
        ObjectNode copy = nativeSource.deepCopy();
        for (JsonNode ignored : twinIgnored) remove(copy, ignored.asText(), mapping);
        for (String path : dropped) remove(copy, path, mapping);
        prune(copy);
        for (Map.Entry<String, String> constant : constants.entrySet()) addConstant(copy, constant.getKey(), constant.getValue());
        return copy;
    }

    private static void remove(JsonNode node, String path, JsonNode mapping) {
        if (node.isArray()) {
            node.forEach(element -> remove(element, path, mapping));
            return;
        }
        if (!(node instanceof ObjectNode object)) return;
        if (object.has(path)) {
            object.remove(path);
            return;
        }
        // A listed multi-field (title.raw) removes its parent field's value.
        for (Iterator<String> names = object.fieldNames(); names.hasNext(); ) {
            String name = names.next();
            if (path.startsWith(name + ".")) {
                JsonNode child = object.get(name);
                if (child.isObject() || (child.isArray() && child.size() > 0 && child.get(0).isObject())) {
                    remove(child, path.substring(name.length() + 1), mapping);
                } else {
                    names.remove();
                }
                return;
            }
        }
    }

    private static boolean prune(JsonNode node) {
        if (node instanceof ObjectNode object) {
            for (Iterator<Map.Entry<String, JsonNode>> it = object.fields(); it.hasNext(); ) {
                JsonNode child = it.next().getValue();
                if (child.isContainerNode() && prune(child)) it.remove();
            }
            return object.isEmpty();
        }
        if (node instanceof ArrayNode array) {
            for (Iterator<JsonNode> it = array.elements(); it.hasNext(); ) {
                JsonNode child = it.next();
                if (child.isContainerNode() && prune(child)) it.remove();
            }
            return array.isEmpty();
        }
        return false;
    }

    /** {@code path} -> value of every constant_keyword with a value; nested paths are marked with '#'. */
    private static Map<String, String> constants(JsonNode mapping, String prefix) {
        Map<String, String> constants = new LinkedHashMap<>();
        mapping.path("properties").properties().forEach(e -> {
            JsonNode field = e.getValue();
            String path = prefix + e.getKey();
            if (field.path("type").asText().equals("constant_keyword") && field.has("value")) {
                constants.put(path, field.path("value").asText());
            } else if (field.has("properties")) {
                constants.putAll(constants(field, path + (field.path("type").asText().equals("nested") ? "#" : ".")));
            }
        });
        return constants;
    }

    private static void addConstant(ObjectNode root, String path, String value) {
        int nested = path.indexOf('#');
        if (nested >= 0) {
            JsonNode objects = root.path(path.substring(0, nested));
            List<ObjectNode> targets = new ArrayList<>();
            if (objects instanceof ObjectNode one) targets.add(one);
            objects.forEach(element -> { if (element instanceof ObjectNode o) targets.add(o); });
            for (ObjectNode target : targets) addConstant(target, path.substring(nested + 1), value);
            return;
        }
        int dot = path.indexOf('.');
        if (dot >= 0) {
            String name = path.substring(0, dot);
            if (!root.has(name)) insertSorted(root, name, JSON.createObjectNode());
            addConstant((ObjectNode) root.get(name), path.substring(dot + 1), value);
            return;
        }
        if (!root.has(path)) insertSorted(root, path, JSON.getNodeFactory().textNode(value));
    }

    private static void insertSorted(ObjectNode object, String name, JsonNode value) {
        Map<String, JsonNode> entries = new LinkedHashMap<>();
        boolean inserted = false;
        for (Iterator<Map.Entry<String, JsonNode>> it = object.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            if (!inserted && name.compareTo(entry.getKey()) < 0) {
                entries.put(name, value);
                inserted = true;
            }
            entries.put(entry.getKey(), entry.getValue());
        }
        if (!inserted) entries.put(name, value);
        object.removeAll();
        entries.forEach(object::set);
    }

    private static Set<String> types(JsonNode mapping) {
        Set<String> types = new TreeSet<>();
        mapping.path("properties").properties().forEach(e -> {
            JsonNode field = e.getValue();
            types.add(field.path("type").asText(field.has("properties") ? "object" : ""));
            types.addAll(types(field));
            field.path("fields").properties().forEach(f -> types.add(f.getValue().path("type").asText()));
        });
        types.remove("");
        return types;
    }

    static List<Case> cases() throws IOException {
        List<Case> cases = new ArrayList<>();
        String name = null;
        Map<String, String> settings = null;
        String mapping = null;
        String expect = null;
        List<String> documents = null;
        String inject = null;
        String preflight = null;
        String pruned = null;
        for (String line : Files.readAllLines(CASES, StandardCharsets.UTF_8)) {
            if (line.startsWith("== ")) {
                if (name != null) cases.add(new Case(name, settings, mapping, expect, documents, inject, preflight, pruned));
                name = line.substring(3).strip();
                inject = null;
                preflight = null;
                pruned = null;
                settings = new LinkedHashMap<>();
                settings.put("index.number_of_shards", "1");
                settings.put("index.number_of_replicas", "0");
                settings.put("index.version.created", "9039000");
                mapping = null;
                expect = null;
                documents = new ArrayList<>();
            } else if (line.startsWith("settings: ")) {
                for (String pair : line.substring(10).split(";")) {
                    String[] kv = pair.split("=", 2);
                    settings.put(kv[0].strip(), kv[1].strip());
                }
            } else if (line.startsWith("mapping: ")) {
                mapping = line.substring(9).strip();
            } else if (line.startsWith("expect: ")) {
                expect = line.substring(8).strip();
            } else if (line.startsWith("inject: ")) {
                inject = line.substring(8).strip();
            } else if (line.startsWith("preflight: ")) {
                preflight = line.substring(11).strip();
            } else if (line.startsWith("pruned: ")) {
                pruned = line.substring(8).strip();
            } else if (line.startsWith("update: ")) {
                documents.add(UPDATE + line.substring(8).strip());
            } else if (line.startsWith("doc: ")) {
                documents.add(line.substring(5).strip());
            }
        }
        if (name != null) cases.add(new Case(name, settings, mapping, expect, documents, inject, preflight, pruned));
        return cases;
    }

    private static String run(String mapping, Map<String, String> settings, List<String> documents, Path tmp, String inject)
            throws Exception {
        IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            Method run = Class.forName(PROBE, true, loader).getMethod("run", String.class, Map.class, List.class, String.class,
                String.class);
            return (String) run.invoke(null, mapping, settings, documents, tmp.toString(), inject);
        } catch (InvocationTargetException e) {
            throw e.getTargetException() instanceof Exception cause ? cause : e;
        } finally {
            thread.setContextClassLoader(previous);
        }
    }
}
