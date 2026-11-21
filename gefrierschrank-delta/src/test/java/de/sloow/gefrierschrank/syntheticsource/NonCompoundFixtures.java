package de.sloow.gefrierschrank.syntheticsource;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;

/**
 * Test helper of the delta module, in this package for the isolated loader: rewrites a shard
 * directory with non-compound segments through the isolated world's Elasticsearch codecs (see
 * {@code isolated.NonCompoundShards}), without the writer's lock file.
 */
public final class NonCompoundFixtures {

    private NonCompoundFixtures() {}

    public static void rewrite(Path from, Path to) throws Exception {
        IsolatedEsClassLoader loader = IsolatedEsClassLoader.shared();
        Thread thread = Thread.currentThread();
        ClassLoader previous = thread.getContextClassLoader();
        thread.setContextClassLoader(loader);
        try {
            Class.forName("de.sloow.gefrierschrank.syntheticsource.isolated.NonCompoundShards", true, loader)
                .getMethod("rewrite", String.class, String.class).invoke(null, from.toString(), to.toString());
        } catch (InvocationTargetException e) {
            throw e.getTargetException() instanceof Exception cause ? cause : e;
        } finally {
            thread.setContextClassLoader(previous);
        }
        java.nio.file.Files.deleteIfExists(to.resolve("write.lock"));
    }
}
