package de.sloow.gefrierschrank.syntheticsource.isolated;

import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateMetricDoubleFieldMapper;
import org.elasticsearch.xpack.analytics.mapper.HistogramFieldMapper;
import org.elasticsearch.xpack.constantkeyword.mapper.ConstantKeywordFieldMapper;
import org.elasticsearch.xpack.countedkeyword.CountedKeywordFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongFieldMapper;
import org.elasticsearch.xpack.versionfield.VersionStringFieldMapper;
import org.elasticsearch.xpack.wildcard.mapper.WildcardFieldMapper;

import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * THIS CLASS IS PROJECT-WRITTEN, NOT VENDORED ES CODE - deliberately placed next to the genuinely
 * vendored {@code org.elasticsearch.xpack.*} mapper classes it registers so the two are obviously
 * distinguishable at a glance (see those packages' {@code package-info.java} for the real
 * vendoring, EL2.0-only license note, and provenance).
 *
 * <p>Registers nine {@link Mapper.TypeParser}s in total:
 *
 * <p>Four types - {@code aggregate_metric_double}
 * ({@link AggregateMetricDoubleFieldMapper}), {@code histogram} ({@link HistogramFieldMapper}),
 * {@code constant_keyword} ({@link ConstantKeywordFieldMapper}), and {@code wildcard}
 * ({@link WildcardFieldMapper}) - each implemented in a real ES node as part of a bundled x-pack
 * plugin ({@code AggregateMetricMapperPlugin}, {@code AnalyticsPlugin},
 * {@code ConstantKeywordMapperPlugin}, {@code WildcardPlugin} respectively).
 *
 * <p>Five more types - {@code version} ({@link VersionStringFieldMapper}, from
 * x-pack's {@code mapper-version} plugin), {@code counted_keyword}
 * ({@link CountedKeywordFieldMapper}, from {@code mapper-counted-keyword}), {@code unsigned_long}
 * ({@link UnsignedLongFieldMapper}, from {@code mapper-unsigned-long}), {@code shape}
 * ({@link ShapeFieldMapper}, Cartesian shapes) and {@code geo_shape}
 * ({@link GeoShapeWithDocValuesFieldMapper}, the modern doc-values-backed implementation - see
 * {@code org.elasticsearch.xpack.spatial.index.mapper} package-info for why the older
 * {@code LegacyGeoShapeFieldMapper} path was deliberately NOT vendored) - both from x-pack's
 * {@code spatial} plugin.
 *
 * <p>None of the nine real plugin classes above ({@code AggregateMetricMapperPlugin} etc.,
 * including {@code SpatialPlugin} and the two mapper-version/mapper-counted-keyword/
 * mapper-unsigned-long plugin classes) are vendored or extended by this class. Each one pulls in
 * unrelated aggregation-registrar, transport-action, and usage-reporting machinery (cluster
 * actions, {@code ValuesSourceRegistry} aggregation wiring, license/usage stats collectors) that
 * this project's standalone, non-search, non-aggregation {@code MapperService.merge(...)} path
 * never calls and would otherwise need to be fully resolved or stubbed out for no benefit.
 * Implementing the single {@link MapperPlugin#getMappers()} method needed to make
 * {@code IndicesModule} accept these nine types is sufficient and keeps this project's own code
 * (as opposed to the vendored ES sources sitting next to it) minimal and auditable.
 *
 * <p>One wrinkle for {@code geo_shape}: the real {@code GeoShapeWithDocValuesFieldMapper.PARSER}
 * (unlike the other eight types' plain {@code TypeParser} constant) is an instance of the real ES
 * {@code GeoShapeWithDocValuesFieldMapper.TypeParser} class, which needs a
 * {@link GeoFormatterFactory} (used by the real plugin only to support vector-tile-format output
 * extensions this project never registers). An empty {@code GeoFormatterFactory} - built from an
 * empty formatter-factory list, exactly the state a real node has before any
 * {@code GeometryFormatterExtension} loads - is constructed here rather than in the vendored
 * mapper class itself, since standard GeoJSON/WKT/WKB formatting (this project's synthetic-source
 * reconstruction path) does not go through this extension point at all.
 *
 * <p>Not a {@code Plugin} subclass on purpose (unlike the real ES plugin classes, and unlike
 * {@code MapperExtrasPlugin} which extends {@code Plugin} mainly because it ALSO implements
 * {@code SearchPlugin} for query registration this project doesn't need) - {@code IndicesModule}'s
 * constructor only requires a {@code List<MapperPlugin>}, and {@link MapperPlugin} itself carries
 * no abstract requirements beyond default no-op methods, so a bare {@code MapperPlugin}
 * implementation is the minimal correct facade here.
 */
public final class GefrierschrankXPackMapperPlugin implements MapperPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(AggregateMetricDoubleFieldMapper.CONTENT_TYPE, AggregateMetricDoubleFieldMapper.PARSER);
        mappers.put(HistogramFieldMapper.CONTENT_TYPE, HistogramFieldMapper.PARSER);
        mappers.put(ConstantKeywordFieldMapper.CONTENT_TYPE, ConstantKeywordFieldMapper.PARSER);
        mappers.put(WildcardFieldMapper.CONTENT_TYPE, WildcardFieldMapper.PARSER);
        mappers.put(VersionStringFieldMapper.CONTENT_TYPE, VersionStringFieldMapper.PARSER);
        mappers.put(CountedKeywordFieldMapper.CONTENT_TYPE, CountedKeywordFieldMapper.PARSER);
        mappers.put(UnsignedLongFieldMapper.CONTENT_TYPE, UnsignedLongFieldMapper.PARSER);
        mappers.put(ShapeFieldMapper.CONTENT_TYPE, ShapeFieldMapper.PARSER);
        mappers.put(
            GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE,
            new GeoShapeWithDocValuesFieldMapper.TypeParser(new GeoFormatterFactory<Geometry>(List.of()))
        );
        return Collections.unmodifiableMap(mappers);
    }
}
