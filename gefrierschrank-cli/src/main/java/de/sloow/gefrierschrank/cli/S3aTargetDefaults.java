package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.TempRoot;
import org.apache.hadoop.conf.Configuration;

import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;

/**
 * S3A upload settings for s3a:// target tables, applied only where Hadoop's shipped default is
 * the sole source of a key, so nothing the user configured (core-site.xml, a programmatic value,
 * -Dhadoop.tmp.dir) is ever overridden.
 *
 * <ul>
 *   <li>16 MiB multipart parts and 8 active blocks per output stream, instead of 64 MiB and 4:
 *   more of a file's parts are already uploaded when its writer closes, which shortens the serial
 *   close at the end of every shard, and the buffered blocks of an open file shrink from up to
 *   320 MiB to 144 MiB. At 16 MiB the 10,000-part limit still allows 156 GiB per object, far
 *   above the 256 MiB file target.</li>
 *   <li>The upload blocks go to {@code <run root>/s3a} instead of {@code /tmp/hadoop-$USER/s3a}:
 *   on the temp filesystem the export budgets, owned by this process, and removed with the run
 *   root on exit or by the next start's sweep.</li>
 * </ul>
 */
final class S3aTargetDefaults {

    static final String MULTIPART_SIZE = "16M";
    static final String ACTIVE_BLOCKS = "8";
    static final String SOURCE = "gefrierschrank target default";
    private static final String SHIPPED_DEFAULTS = "core-default.xml";

    private S3aTargetDefaults() {}

    static void apply(Configuration conf) {
        apply(conf, TempRoot.runRoot(), System::getProperty, System::getenv);
    }

    static void apply(Configuration conf, Optional<Path> runRoot, Function<String, String> systemProperties,
                      Function<String, String> env) {
        setIfUnconfigured(conf, "fs.s3a.multipart.size", MULTIPART_SIZE);
        setIfUnconfigured(conf, "fs.s3a.fast.upload.active.blocks", ACTIVE_BLOCKS);
        // The shipped default is ${env.LOCAL_DIRS:-${hadoop.tmp.dir}}/s3a; either of those set by
        // the user is a chosen buffer location too.
        if (runRoot.isPresent() && systemProperties.apply("hadoop.tmp.dir") == null && env.apply("LOCAL_DIRS") == null
                && unconfigured(conf, "hadoop.tmp.dir")) {
            setIfUnconfigured(conf, "fs.s3a.buffer.dir", runRoot.get().resolve("s3a").toString());
        }
    }

    private static void setIfUnconfigured(Configuration conf, String key, String value) {
        if (unconfigured(conf, key)) conf.set(key, value, SOURCE);
    }

    private static boolean unconfigured(Configuration conf, String key) {
        String[] sources = conf.getPropertySources(key);
        return sources != null && sources.length == 1 && SHIPPED_DEFAULTS.equals(sources[0]);
    }
}
