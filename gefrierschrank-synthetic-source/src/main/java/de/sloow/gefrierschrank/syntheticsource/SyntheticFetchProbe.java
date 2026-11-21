package de.sloow.gefrierschrank.syntheticsource;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Which index structures (postings, points, norms, term vectors, vectors) Elasticsearch's own
 * reconstruction reads on a shard whose directory selective fetch left them out of. Runs the
 * worker's setup and leaf entry over a sample of every leaf's live roots in the isolated world, with
 * the missing structures answered "no values" and recorded. Selective fetch downloads what was
 * touched and asks again until nothing new is touched; see {@code ShardOpening}. Content-free: only
 * structure names leave the isolated world.
 */
public final class SyntheticFetchProbe {

    private static final String IMPL = "de.sloow.gefrierschrank.syntheticsource.isolated.IsolatedSyntheticFetchProbeImpl";

    private SyntheticFetchProbe() {}

    /**
     * {@code touched}: names of {@code SelectiveFetch.Structure}s the reconstruction asked for and the
     * directory lacks. {@code failed}: the setup or a reconstruction threw (a refusal, a mapping that
     * cannot be built, a stub's empty answer); conclusive only when nothing was touched.
     */
    public record Result(Set<String> touched, boolean failed) {
        public Result {
            touched = Set.copyOf(touched);
        }
    }

    /** Never throws for what the reconstruction does; a failure is {@link Result#failed()}. */
    public static Result probe(Path shardDir, String indexName, JsonNode rawIndexMetadata) {
        SyntheticSourceReconstructor.Inputs inputs;
        try {
            inputs = SyntheticSourceReconstructor.reconstructionInputs(rawIndexMetadata);
        } catch (IOException | RuntimeException unreadable) {
            return new Result(Set.of(), true);
        }
        IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();
        Thread currentThread = Thread.currentThread();
        ClassLoader previousContextLoader = currentThread.getContextClassLoader();
        try {
            currentThread.setContextClassLoader(loader);
            @SuppressWarnings("unchecked")
            List<String> answer = (List<String>) Class.forName(IMPL, true, loader)
                .getMethod("probe", Path.class, String.class, String.class, Map.class, String.class)
                .invoke(null, shardDir, indexName, inputs.mappingJson(), inputs.flatSettings(), inputs.rewriteMappingJson());
            return new Result(Set.copyOf(answer.subList(1, answer.size())), !"ok".equals(answer.get(0)));
        } catch (ReflectiveOperationException | RuntimeException e) {
            if (SyntheticSourceReconstructor.hasLinkageError(e)) IsolatedEsClassLoader.discard(loader);
            return new Result(Set.of(), true);
        } catch (LinkageError e) {
            IsolatedEsClassLoader.discard(loader);
            return new Result(Set.of(), true);
        } finally {
            currentThread.setContextClassLoader(previousContextLoader);
        }
    }
}
