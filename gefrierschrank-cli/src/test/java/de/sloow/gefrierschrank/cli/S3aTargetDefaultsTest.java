package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.TempRoot;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/** S3A upload defaults apply only where the user configured nothing. */
class S3aTargetDefaultsTest {

    private static final String DEFAULT_BUFFER_DIR = "${env.LOCAL_DIRS:-${hadoop.tmp.dir}}/s3a";

    @TempDir
    Path root;

    private static String source(Configuration conf, String key) {
        return String.join(",", conf.getPropertySources(key));
    }

    @Test
    void unconfiguredKeysGetSmallerPartsMoreActiveBlocksAndABufferInTheRunRoot() {
        var conf = new Configuration();
        assertEquals("64M", conf.get("fs.s3a.multipart.size"), "Hadoop's shipped default");
        S3aTargetDefaults.apply(conf, Optional.of(root), k -> null, k -> null);
        assertEquals("16M", conf.get("fs.s3a.multipart.size"));
        assertEquals(16L << 20, conf.getLongBytes("fs.s3a.multipart.size", 0));
        assertEquals("8", conf.get("fs.s3a.fast.upload.active.blocks"));
        assertEquals(root.resolve("s3a").toString(), conf.get("fs.s3a.buffer.dir"));
        assertEquals(S3aTargetDefaults.SOURCE, source(conf, "fs.s3a.buffer.dir"));
    }

    @Test
    void keysTheUserSetAreKept() {
        var conf = new Configuration();
        conf.set("fs.s3a.multipart.size", "128M");
        conf.set("fs.s3a.fast.upload.active.blocks", "2");
        conf.set("fs.s3a.buffer.dir", "/data/s3a-blocks");
        S3aTargetDefaults.apply(conf, Optional.of(root), k -> null, k -> null);
        assertEquals("128M", conf.get("fs.s3a.multipart.size"));
        assertEquals("2", conf.get("fs.s3a.fast.upload.active.blocks"));
        assertEquals("/data/s3a-blocks", conf.get("fs.s3a.buffer.dir"));
    }

    @Test
    void aChosenHadoopTmpDirOrLocalDirsKeepsTheBufferDirectory() {
        var byProperty = new Configuration();
        S3aTargetDefaults.apply(byProperty, Optional.of(root), Map.of("hadoop.tmp.dir", "/big/tmp")::get, k -> null);
        assertEquals(DEFAULT_BUFFER_DIR, byProperty.getRaw("fs.s3a.buffer.dir"));
        assertEquals("16M", byProperty.get("fs.s3a.multipart.size"), "the part size is still applied");

        var byEnv = new Configuration();
        S3aTargetDefaults.apply(byEnv, Optional.of(root), k -> null, Map.of("LOCAL_DIRS", "/yarn/local")::get);
        assertEquals(DEFAULT_BUFFER_DIR, byEnv.getRaw("fs.s3a.buffer.dir"));

        var bySite = new Configuration();
        bySite.set("hadoop.tmp.dir", "/site/tmp");
        S3aTargetDefaults.apply(bySite, Optional.of(root), k -> null, k -> null);
        assertEquals(DEFAULT_BUFFER_DIR, bySite.getRaw("fs.s3a.buffer.dir"));
    }

    @Test
    void aRealHadoopTmpDirSystemPropertyIsHonoured() {
        String previous = System.getProperty("hadoop.tmp.dir");
        System.setProperty("hadoop.tmp.dir", root.resolve("chosen").toString());
        try {
            var conf = new Configuration();
            S3aTargetDefaults.apply(conf, Optional.of(root), System::getProperty, k -> null);
            assertEquals(root.resolve("chosen").resolve("s3a").toString(), conf.get("fs.s3a.buffer.dir"));
        } finally {
            if (previous == null) System.clearProperty("hadoop.tmp.dir");
            else System.setProperty("hadoop.tmp.dir", previous);
        }
    }

    @Test
    void withoutARunRootTheBufferDirectoryStaysHadoops() {
        var conf = new Configuration();
        S3aTargetDefaults.apply(conf, Optional.empty(), k -> null, k -> null);
        assertEquals(DEFAULT_BUFFER_DIR, conf.getRaw("fs.s3a.buffer.dir"));
        assertEquals("8", conf.get("fs.s3a.fast.upload.active.blocks"));
    }

    @Test
    void s3aTargetsGetTheDefaultsAndLocalTablesStayUntouched() {
        var env = Map.of("TARGET_AWS_ACCESS_KEY_ID", "tk", "TARGET_AWS_SECRET_ACCESS_KEY", "ts");
        var local = DumpToDeltaCommand.s3aConf("/tmp/table", null, env::get);
        for (String key : new String[]{"fs.s3a.multipart.size", "fs.s3a.fast.upload.active.blocks", "fs.s3a.buffer.dir"}) {
            assertEquals("core-default.xml", source(local, key), key);
        }
        var s3a = DumpToDeltaCommand.s3aConf("s3a://bucket/table", "http://target:9000", env::get);
        assertEquals("16M", s3a.get("fs.s3a.multipart.size"));
        assertEquals("8", s3a.get("fs.s3a.fast.upload.active.blocks"));
        assertTrue(TempRoot.runRoot().isEmpty(), "in-process, without Main, there is no run root");
        assertEquals(DEFAULT_BUFFER_DIR, s3a.getRaw("fs.s3a.buffer.dir"), "so the buffer stays Hadoop's (RunRootCleanupTest covers the root)");
    }
}
