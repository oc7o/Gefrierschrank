package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.TempRoot;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.delta.DumpToDeltaPipeline;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * The child JVM of {@link RunRootCleanupTest}, started with its own {@code java.io.tmpdir} so the
 * sweep never touches the machine's shared temp directory. Installs a run root the way
 * {@link Main} does and prints where the pipeline's shard directory and the S3A upload buffer of
 * an s3a target go: {@code ROOT}, {@code SHARD_DIR}, {@code S3A_BUFFER_DIR}, {@code QUARANTINED}.
 */
public final class RunRootProbe {

    public static void main(String[] args) throws Exception {
        Path repo = Path.of(args[0]);
        Path table = Path.of(args[1]);
        try (var session = TempRoot.install()) {
            System.out.println("ROOT " + TempRoot.runRoot().orElseThrow());
            var blobs = new FileSystemBlobStore(repo);
            var pipeline = new DumpToDeltaPipeline(new RepositoryInspector(blobs), blobs, table.toString(),
                new Configuration(), (shard, shardDir) -> {
                    System.out.println("SHARD_DIR " + shardDir + " " + Files.isDirectory(shardDir));
                    throw new IOException("stop after materialize");
                });
            System.out.println("QUARANTINED " + pipeline.run(args[2]).quarantinedCount());
            var env = Map.of("TARGET_AWS_ACCESS_KEY_ID", "tk", "TARGET_AWS_SECRET_ACCESS_KEY", "ts");
            var conf = DumpToDeltaCommand.s3aConf("s3a://bucket/table", "http://127.0.0.1:1", env::get);
            System.out.println("S3A_BUFFER_DIR " + conf.get("fs.s3a.buffer.dir"));
        }
    }
}
