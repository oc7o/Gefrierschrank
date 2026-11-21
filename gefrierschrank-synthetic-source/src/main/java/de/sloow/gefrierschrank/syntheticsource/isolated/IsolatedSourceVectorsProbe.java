package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Asks Elasticsearch's own mapper code which vector fields an index left out of its stored
 * {@code _source}. Must be defined by {@code IsolatedEsClassLoader} (hence this package) so the ES
 * types resolve in the isolated world; only JDK types cross the boundary.
 */
public final class IsolatedSourceVectorsProbe {

    private IsolatedSourceVectorsProbe() {}

    /**
     * Returns {@code ["false"]} when {@code index.mapping.exclude_source_vectors} evaluates to false
     * (ES derives its default from {@code index.version.created}), otherwise {@code "true"} followed
     * by {@code MappingLookup.syntheticVectorFields()}: the vector fields ES stripped from the stored
     * source, already accounting for {@code index: false}, sparse {@code store: false} and synthetic
     * source. The mapping is built by the same builder reconstruction uses.
     */
    public static List<String> probe(String indexName, String mappingJson, Map<String, String> flatSettings)
            throws IOException {
        IsolatedEsBootstrap.ensure();
        Settings.Builder settings = Settings.builder();
        flatSettings.forEach(settings::put);
        List<String> result = new ArrayList<>();
        if (!IndexSettings.INDEX_MAPPING_EXCLUDE_SOURCE_VECTORS_SETTING.get(settings.build())) {
            result.add("false");
            return result;
        }
        result.add("true");
        try (MapperService mapperService = IsolatedReconstructionWorkerImpl.buildMapperService(indexName, mappingJson, flatSettings)) {
            result.addAll(new TreeSet<>(mapperService.mappingLookup().syntheticVectorFields()));
        }
        return result;
    }
}
