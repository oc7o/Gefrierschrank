package de.sloow.gefrierschrank.syntheticsource;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Isolates Elasticsearch's/Lucene's SPI-registered classes (codecs, doc-values formats, postings
 * formats) from {@code gefrierschrank-lucene10}'s own no-op stub classes of the <em>same SPI
 * name</em>.
 *
 * <p><strong>The trap this exists to route around</strong> (found empirically while integrating
 * this module, not documented anywhere else): {@code org.elasticsearch:elasticsearch:9.3.0}'s own
 * jar registers Lucene SPI providers under
 * {@code META-INF/services/org.apache.lucene.codecs.{Codec,DocValuesFormat,PostingsFormat}} for
 * names like {@code ES819TSDB} and {@code ES93TSDBDefaultCompressionLucene103Codec} - the EXACT
 * same SPI names {@code gefrierschrank-lucene10} already registers its own safe read-only stub
 * classes under (see {@code Es819TsdbDocValuesFormat}, {@code ES93TSDBDefaultCompressionLucene103Codec}
 * in that module), for a completely different reason: those stubs let a shard's segment files be
 * <em>opened</em> at all when full ES isn't on the classpath. When both jars are visible to one
 * {@link java.util.ServiceLoader} scan (exactly the case once this module and
 * {@code gefrierschrank-lucene10} are both dependencies of the same runtime classpath, e.g. the
 * CLI's shaded fat jar), which class wins for a given SPI name becomes classpath-order-dependent
 * and, worse, is decided once per JVM and cached ({@code Codec$Holder}/{@code DocValuesFormat$Holder}
 * are lazily-initialized singletons) - so whichever resolves first "wins" for the rest of the
 * process, silently. Verified empirically: with the stub classes earlier on the classpath, the
 * ambient/ordinary {@code Codec.forName(...)}/{@code DocValuesFormat.forName(...)} resolve to the
 * SAFE STUBS, not the real codec - which is exactly the behavior every other Lucene-reading
 * codepath (the existing {@code Lucene10ShardReader} fast path) correctly depends on and must keep
 * getting.
 *
 * <p>This reconstruction path is the one place in the codebase that specifically needs the REAL
 * ES819TSDB doc-values decoder (to actually read the time-series doc values it produced) instead
 * of the stub (which reports every field as having no doc values at all - correct for a plain
 * stored-{@code _source} dump, useless for synthetic-source reconstruction). Loading ES's mapper
 * classes ({@link org.elasticsearch.index.mapper.MapperService} et al.) through an isolated,
 * child-first {@link URLClassLoader} whose {@link ClassLoader#getResources(String)} override
 * merges every real provider-config line for the relevant SPI keys while dropping only this
 * project's own {@code de.sloow.gefrierschrank.*} package gives this one reconstruction path its
 * own private view of "which class implements ES819TSDB" - genuinely the real ES class - without
 * ever touching the ambient/ordinary classloader's cached SPI state that the rest of the process
 * (including {@code Lucene10ShardReader} reading non-synthetic-source shards in the very same
 * process) continues to rely on.
 *
 * <p>Deliberately NOT solved by excluding jars/paths from a classpath list: in a shaded fat jar
 * (the CLI's actual packaging), the stub classes and the real ES classes are merged into ONE jar
 * with no separable path/URL boundary between them - only overriding resource resolution for the
 * specific SPI provider-config file names (not classes in general) can distinguish "the safe stub
 * gefrierschrank-lucene10 wants" from "the real decoder this module needs", because both are
 * present on literally the same classpath entries.
 *
 * <p><strong>A second thing worth spelling out once this module started vendoring ES's own
 * {@code mapper-extras} source under {@code org.elasticsearch.index.mapper.extras} (see that
 * package's {@code package-info.java}):</strong> this project now defines classes inside the
 * {@code org.elasticsearch} namespace itself, not just consumes ES's. Verified this creates no
 * new ambiguity for {@link #isIsolatedClass(String)}'s existing {@code org.elasticsearch.} prefix
 * check, for two independent reasons - so no change to that check was needed: (1) the vendored
 * {@code mapper-extras} package does not exist anywhere in the real
 * {@code org.elasticsearch:elasticsearch:9.3.0} jar (confirmed via {@code unzip -l} across the
 * whole jar), so there is no same-named-class collision the way there is for Lucene SPI keys -
 * whichever URL entry (this module's own compiled output vs. the real ES jar) actually contains a
 * given {@code .class} file is simply the one {@link #findClass} resolves it from; and (2) unlike
 * the Lucene codecs above, ES's mapper plugins are never discovered via {@link java.util.ServiceLoader}
 * - {@code MapperExtrasPlugin} is only ever directly {@code new}'d, by
 * {@code IsolatedReconstructionWorkerImpl} (itself an isolated class per
 * {@link #ISOLATED_OWN_PACKAGE_PREFIX}), so the SPI-resource-merging machinery this class exists
 * for is simply not in play for the vendored mapper classes at all - they just need to be defined
 * by this loader (not delegated to the ambient one) the same way any other real ES class is,
 * which the existing prefix check already does correctly.
 *
 * <p><strong>One class is never loaded from any jar:</strong>
 * {@code org.elasticsearch.nativeaccess.NativeAccessHolder}. ES's holder binds its bundled libzstd,
 * which a standalone process does not have, and silently falls back to a {@code NativeAccess}
 * without zstd, so reading ES 9.3's ZSTD-compressed ES819 binary doc values would throw a
 * NullPointerException. This loader defines that exact name from {@link IsolatedNativeAccessHolder}'s
 * bytes instead, whose {@code INSTANCE} is {@code IsolatedJniNativeAccess} (zstd through
 * {@code ZstdFrameDecoder}, everything else no-op). Only the isolated world sees the substitute; the
 * ambient class path, and the shaded jar, still hold only ES's own holder.
 */
final class IsolatedEsClassLoader extends URLClassLoader {

    private static final Set<String> ISOLATED_SPI_RESOURCE_KEYS = Set.of(
        "META-INF/services/org.apache.lucene.codecs.Codec",
        "META-INF/services/org.apache.lucene.codecs.DocValuesFormat",
        "META-INF/services/org.apache.lucene.codecs.PostingsFormat",
        "META-INF/services/org.apache.lucene.codecs.KnnVectorsFormat");

    /** This project's own stub classes/providers - the only thing ever dropped from the merge. */
    private static final String EXCLUDED_PACKAGE_PREFIX = "de.sloow.gefrierschrank.";

    /**
     * The one package of THIS module's own code that must also be defined by this loader, not
     * delegated to the parent: {@code SyntheticSourceWorker} and friends reference
     * {@code org.apache.lucene.*}/{@code org.elasticsearch.*} types directly, and constant-pool
     * type references resolve via a class's OWN defining classloader, not the classloader that
     * happened to initiate the {@code loadClass} call. If the worker class were left to the
     * parent (ambient) loader like every other non-isolated class, its Lucene/ES type references
     * would resolve through the AMBIENT classloader's SPI state - exactly the state this whole
     * class exists to route around - silently defeating the isolation.
     */
    private static final String ISOLATED_OWN_PACKAGE_PREFIX = "de.sloow.gefrierschrank.syntheticsource.isolated.";

    /**
     * The process-wide loader. Loading ES's mapper and codec classes costs about 1.5 s and 14 MB
     * of Metaspace per loader, and nothing ever unloads a used one, so one loader per synthetic
     * shard would grow without bound as more shards are read. MapperServices stay per shard; only
     * classes are shared.
     */
    private static final AtomicReference<IsolatedEsClassLoader> SHARED = new AtomicReference<>();

    IsolatedEsClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    /**
     * The shared loader, built by {@link #create()} on first use (so from the first caller's
     * context class loader) and never closed: open reconstructors on other threads may still use
     * it, and it lives until the JVM exits.
     */
    static IsolatedEsClassLoader shared() {
        while (true) {
            IsolatedEsClassLoader current = SHARED.get();
            if (current != null) {
                return current;
            }
            IsolatedEsClassLoader created = create();
            if (SHARED.compareAndSet(null, created)) {
                return created;
            }
            try {
                created.close();
            } catch (IOException ignored) {
                // never used: nothing was loaded through it
            }
        }
    }

    /**
     * Stops handing out {@code poisoned} (a class failed to link or initialize in it, which would
     * fail every later shard the same way); the next {@link #shared()} builds a fresh loader.
     * No effect when {@code poisoned} was already replaced.
     */
    static void discard(IsolatedEsClassLoader poisoned) {
        SHARED.compareAndSet(poisoned, null);
    }

    /** True for classes this loader must define itself rather than delegate to the parent. */
    private static boolean isIsolatedClass(String name) {
        return name.startsWith("org.apache.lucene.") || name.startsWith("org.elasticsearch.")
            || name.startsWith(ISOLATED_OWN_PACKAGE_PREFIX);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> c = findLoadedClass(name);
            if (c == null) {
                if (IsolatedNativeAccessHolder.CLASS_NAME.equals(name)) {
                    byte[] holder = IsolatedNativeAccessHolder.bytes();
                    c = defineClass(name, holder, 0, holder.length);
                } else if (isIsolatedClass(name)) {
                    try {
                        c = findClass(name);
                    } catch (ClassNotFoundException e) {
                        c = super.loadClass(name, false);
                    }
                } else {
                    c = super.loadClass(name, false);
                }
            }
            if (resolve) {
                resolveClass(c);
            }
            return c;
        }
    }

    @Override
    public URL getResource(String name) {
        if (ISOLATED_SPI_RESOURCE_KEYS.contains(name)) {
            return syntheticFilteredUrl(name);
        }
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        if (ISOLATED_SPI_RESOURCE_KEYS.contains(name)) {
            return Collections.enumeration(List.of(syntheticFilteredUrl(name)));
        }
        return super.getResources(name);
    }

    /**
     * Merges every real occurrence of an SPI provider-config resource visible to the ordinary
     * classpath scan, dropping only lines naming this project's own stub package. Built
     * dynamically (not a hardcoded class-name list) so it keeps working across Elasticsearch/
     * Lucene version bumps without needing to be updated here.
     */
    private URL syntheticFilteredUrl(String name) {
        LinkedHashSet<String> keep = new LinkedHashSet<>();
        try {
            Enumeration<URL> all = super.getResources(name);
            while (all.hasMoreElements()) {
                URL u = all.nextElement();
                try (InputStream in = u.openStream();
                     BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = r.readLine()) != null) {
                        String trimmed = line.strip();
                        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                            continue;
                        }
                        if (trimmed.startsWith(EXCLUDED_PACKAGE_PREFIX)) {
                            continue;
                        }
                        keep.add(trimmed);
                    }
                }
            }
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        byte[] body = (String.join("\n", keep) + "\n").getBytes(StandardCharsets.UTF_8);
        try {
            return new URL(null, "gefrierschrank-isolated-spi:" + name, new StaticContentUrlStreamHandler(body));
        } catch (java.net.MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Serves fixed in-memory bytes for the synthetic resource URLs created above. */
    private static final class StaticContentUrlStreamHandler extends URLStreamHandler {
        private final byte[] content;

        StaticContentUrlStreamHandler(byte[] content) {
            this.content = content;
        }

        @Override
        protected URLConnection openConnection(URL u) {
            return new URLConnection(u) {
                @Override
                public void connect() {
                }

                @Override
                public InputStream getInputStream() {
                    return new ByteArrayInputStream(content);
                }
            };
        }
    }

    /**
     * Builds an isolated loader from the current thread's context classloader's URLs (works both
     * on a plain reactor classpath and inside a shaded fat jar, where
     * {@link URLClassLoader#getURLs()} may not reflect the actual jar; falls back to the
     * {@code java.class.path} system property in that case).
     */
    static IsolatedEsClassLoader create() {
        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        URL[] urls;
        if (parent instanceof URLClassLoader ucl && ucl.getURLs().length > 0) {
            urls = ucl.getURLs();
        } else {
            String cp = System.getProperty("java.class.path", "");
            String[] entries = cp.split(java.io.File.pathSeparator);
            urls = new URL[entries.length];
            for (int i = 0; i < entries.length; i++) {
                try {
                    urls[i] = java.nio.file.Path.of(entries[i]).toUri().toURL();
                } catch (Exception e) {
                    throw new IllegalStateException("cannot build isolated classloader from classpath entry: " + entries[i], e);
                }
            }
        }
        return new IsolatedEsClassLoader(urls, parent);
    }
}
