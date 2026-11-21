package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.delta.ExportPreflight;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

/**
 * Reports which ES field <em>types</em> a repository's mappings use, so a "No mapper found for
 * type [x]" quarantine can be diagnosed without re-running the export.
 *
 * <p>Deliberately emits type names, field-name depth counts and index settings only - never field
 * names' values and never document content - so its output is safe to share. Field names
 * themselves are also withheld by default (they can encode business meaning);
 * {@code --show-field-names} opts in when the user judges that acceptable.
 *
 * <p>Motivation: ES's mapper implementations are split across core, bundled modules
 * (mapper-extras) and x-pack plugins. Only core + whatever this project vendors is registered in
 * the standalone MapperService used for synthetic-source reconstruction, so an unregistered type
 * fails the whole shard. Knowing the full type inventory up front turns "fix one type, re-run, hit
 * the next type" into a single pass.
 */
@Command(name = "field-types",
    description = "Report which ES field types a repository's index mappings use (safe to share: "
        + "no document content, field names withheld unless --show-field-names)")
public class FieldTypesCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Repository: local dir or s3://bucket/prefix")
    private String repoUri;

    @CommandLine.Option(names = {"--source-endpoint", "--endpoint"},
        description = "S3 endpoint for s3:// repositories")
    private String sourceEndpoint;

    @CommandLine.Option(names = "--source-credentials", paramLabel = "env|ambient", converter = CredentialSources.ModeConverter.class,
        description = "env (default): AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY (optional AWS_SESSION_TOKEN); ambient: " +
            "AWS_PROFILE, web identity, container or instance role, never environment keys.")
    private CredentialSources.Mode sourceCredentials = CredentialSources.Mode.ENV;

    @CommandLine.Option(names = "--index",
        description = "Only inspect this index (default: every index in the repository)")
    private String singleIndex;

    @CommandLine.Option(names = "--show-field-names",
        description = "Also print the field name for each occurrence. Off by default because "
            + "field names can encode business meaning; type names alone cannot.")
    private boolean showFieldNames;

    @Override
    public Integer call() {
        try (BlobStore store = openStore(repoUri)) {
            RepositoryInspector inspector = new RepositoryInspector(store);

            List<String> indices = new ArrayList<>();
            if (singleIndex != null) {
                indices.add(singleIndex);
            } else {
                inspector.overview().indices().forEach(i -> indices.add(i.name()));
            }

            // type name -> how many indices use it (not how many fields: an index with many
            // fields of one unregistered type is still one index that fails, which is the unit
            // that matters)
            Map<String, Integer> indicesPerType = new TreeMap<>();
            Map<String, String> firstIndexPerType = new TreeMap<>();
            Map<String, List<String>> fieldNamesPerType = new TreeMap<>();
            Map<String, Integer> indexModes = new TreeMap<>();
            int inspected = 0;
            int failed = 0;

            for (String index : indices) {
                try {
                    JsonNode metadata = inspector.indexMetadata(index);
                    JsonNode properties = inspector.indexMapping(index).properties();

                    Map<String, List<String>> typesHere = new TreeMap<>();
                    collectTypes(properties, "", typesHere);

                    for (var entry : typesHere.entrySet()) {
                        String type = entry.getKey();
                        indicesPerType.merge(type, 1, Integer::sum);
                        firstIndexPerType.putIfAbsent(type, index);
                        if (showFieldNames) {
                            fieldNamesPerType
                                .computeIfAbsent(type, k -> new ArrayList<>())
                                .addAll(entry.getValue());
                        }
                    }

                    // settings are flattened ("index.mode") in some versions and nested in others
                    String mode = metadata.path("settings").path("index.mode").asText("");
                    if (mode.isEmpty()) {
                        mode = metadata.path("settings").path("index").path("mode").asText("standard");
                    }
                    indexModes.merge(mode, 1, Integer::sum);

                    inspected++;
                } catch (Exception e) {
                    failed++;
                }
                if (inspected % 50 == 0 && inspected > 0) {
                    System.err.printf("  ...inspected %d/%d indices%n", inspected, indices.size());
                }
            }

            print(inspected, failed, indices.size(), indicesPerType, firstIndexPerType,
                fieldNamesPerType, indexModes);
            return 0;
        } catch (Exception e) {
            System.err.println("error: " + e.getMessage());
            return 1;
        }
    }

    /**
     * Walks a mapping's {@code properties} tree collecting every declared {@code type}. Recurses
     * into object fields and {@code fields} (multi-fields), because an unregistered type nested
     * under either still fails the whole mapping merge.
     */
    static void collectTypes(JsonNode properties, String pathPrefix, Map<String, List<String>> into) {
        if (properties == null || !properties.isObject()) {
            return;
        }
        Iterator<Map.Entry<String, JsonNode>> it = properties.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> field = it.next();
            String name = field.getKey();
            JsonNode def = field.getValue();
            String path = pathPrefix.isEmpty() ? name : pathPrefix + "." + name;

            String type = def.path("type").asText("");
            if (!type.isEmpty()) {
                into.computeIfAbsent(type, k -> new ArrayList<>()).add(path);
            } else if (def.has("properties")) {
                // an object field with no explicit type is implicitly "object"
                into.computeIfAbsent("object", k -> new ArrayList<>()).add(path);
            }

            if (def.has("properties")) {
                collectTypes(def.path("properties"), path, into);
            }
            if (def.has("fields")) {
                collectTypes(def.path("fields"), path, into);
            }
        }
    }

    private void print(int inspected, int failed, int total,
                       Map<String, Integer> indicesPerType,
                       Map<String, String> firstIndexPerType,
                       Map<String, List<String>> fieldNamesPerType,
                       Map<String, Integer> indexModes) {
        System.out.printf("Indices inspected : %d of %d%n", inspected, total);
        if (failed > 0) {
            System.out.printf("Indices unreadable: %d (metadata could not be read)%n", failed);
        }
        System.out.println();

        System.out.println("Index modes:");
        for (var e : indexModes.entrySet()) {
            System.out.printf("  %-24s %d indices%n", e.getKey(), e.getValue());
        }
        System.out.println();

        System.out.printf("%-34s %-10s %-10s %s%n", "FIELD TYPE", "INDICES", "SYNTHETIC", "FIRST SEEN IN");
        for (var e : indicesPerType.entrySet()) {
            System.out.printf("%-34s %-10d %-10s %s%n", e.getKey(), e.getValue(),
                ExportPreflight.syntheticExportable(e.getKey()) ? "yes" : "no",
                firstIndexPerType.get(e.getKey()));
        }

        if (showFieldNames) {
            System.out.println();
            System.out.println("Field names per type:");
            for (var e : fieldNamesPerType.entrySet()) {
                System.out.printf("  %s:%n", e.getKey());
                e.getValue().stream().distinct().limit(40)
                    .forEach(f -> System.out.printf("    %s%n", f));
            }
        }

        System.out.println();
        System.out.println("SYNTHETIC no: a synthetic-source index using the type does not export:");
        System.out.println("gefrierschrank's standalone MapperService has no mapper and shards fail with");
        System.out.println("\"No mapper found for type [x]\" (survey: not_exportable(unsupported_synthetic_type), or");
        System.out.println("semantic_text_synthetic). Stored-source indices are unaffected.");
        System.out.println("Run with --index <name> against a quarantined index to see exactly what it needs.");
    }

    private BlobStore openStore(String uri) {
        return CredentialSources.openSourceStore(uri, sourceEndpoint, sourceCredentials, null, System::getenv);
    }
}
