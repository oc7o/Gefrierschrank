package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.FailureDiagnostics;
import de.sloow.gefrierschrank.core.TempRoot;
import de.sloow.gefrierschrank.core.lucene.ShardReader;
import de.sloow.gefrierschrank.core.repo.BlobStore;
import de.sloow.gefrierschrank.core.repo.RepositorySources;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.delta.ExportPreflight;
import de.sloow.gefrierschrank.delta.ExportReaders;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * JSONL export of one index on dump-to-delta's per-index plan: the latest snapshot of the name,
 * mounted searchable-snapshot references resolved to their backing data, the export preflight
 * (created-version floor, source retention, excluded vectors), metadata and segments from one
 * snapshot, and synthetic-source reconstruction. Each
 * line is one live document's {@code _source}; a file output appears only once the whole index is
 * written (see {@link Destination}).
 */
@Command(name = "dump", description = "Dump all _source documents of an index as JSONL")
public class DumpCommand implements Callable<Integer> {

    @CommandLine.Spec CommandLine.Model.CommandSpec spec;

    /** Opens the source repository and every --repository; a seam for tests. */
    interface StoreOpener {
        BlobStore open(String uri, String endpoint, String repositoryPrefix);
    }

    @Parameters(index = "0", description = "Repository: local dir or s3://bucket/prefix")
    private String repoUri;

    @CommandLine.Option(names = {"--index"}, required = true,
        description = "Index to dump: its latest snapshot, or for a mounted searchable snapshot the data it mounts")
    private String indexName;

    @CommandLine.Option(names = {"-o", "--output"}, required = true,
        description = "Output JSONL file, written beside it as a partial file and moved into place once the whole index is dumped; "
            + "/dev/stdout, a named pipe or another non-regular file is written directly")
    private Path output;

    // "--endpoint" stays as an alias so existing scripts keep working.
    @CommandLine.Option(names = {"--source-endpoint", "--endpoint"},
        description = "S3 endpoint for the source s3:// repository (e.g. http://localhost:9000)")
    private String sourceEndpoint;

    @CommandLine.Option(names = "--source-credentials", paramLabel = "env|ambient", converter = CredentialSources.ModeConverter.class,
        description = "Source repository credentials: env (default) reads AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY " +
            "(optional AWS_SESSION_TOKEN); ambient uses AWS_PROFILE (role_arn, credential_process, SSO), web " +
            "identity (IRSA), container or instance role and must not be combined with AWS_ACCESS_KEY_ID. A " +
            "backing repository with --repository-env NAME=PREFIX uses PREFIX_AWS_* keys or PREFIX_AWS_PROFILE instead.")
    private CredentialSources.Mode sourceCredentials = CredentialSources.Mode.ENV;

    @CommandLine.Option(names = "--repository", paramLabel = "NAME=URI",
        description = "Additional backing repository of mounted searchable snapshots, keyed by its ES repository name. Repeatable. Uses source credentials.")
    private Map<String, String> repositories = new LinkedHashMap<>();

    @CommandLine.Option(names = "--repository-endpoint", paramLabel = "NAME=URL",
        description = "S3 endpoint override for an additional --repository; otherwise uses --source-endpoint.")
    private Map<String, String> repositoryEndpoints = new LinkedHashMap<>();

    @CommandLine.Option(names = "--repository-env", paramLabel = "NAME=PREFIX",
        description = "Credentials for a backing repository from PREFIX_AWS_ACCESS_KEY_ID / PREFIX_AWS_SECRET_ACCESS_KEY (optional SESSION_TOKEN and REGION).")
    private Map<String, String> repositoryEnvs = new LinkedHashMap<>();

    @CommandLine.Option(names = "--download-concurrency", paramLabel = "N", converter = DumpToDeltaCommand.DownloadConcurrency.class,
        description = "Concurrent download streams per shard, 1-32 (default: 8). 1 downloads a shard's files one after another.")
    private Integer downloadConcurrency;

    StoreOpener stores = (uri, endpoint, repositoryPrefix) ->
        CredentialSources.openSourceStore(uri, endpoint, sourceCredentials, repositoryPrefix, System::getenv);

    @Override
    public Integer call() {
        LuceneRuntimeSelector.requireInProcessRuntime(spec);
        List<BlobStore> opened = new ArrayList<>();
        try {
            String directory = IntraShardOptions.invalidReconstructionDirectory();
            if (directory != null) throw new InvalidDumpRequestException(directory);
            if (downloadConcurrency != null) System.setProperty(ShardMaterializer.CONCURRENCY_PROPERTY, downloadConcurrency.toString());
            var destination = Destination.of(output);
            // A direct destination may be stdout itself, where a status line would become a row.
            PrintStream status = destination.direct() ? System.err : System.out;
            BlobStore primary = stores.open(repoUri, sourceEndpoint, null);
            opened.add(primary);
            var sources = new RepositorySources(new RepositorySources.Source("primary", primary));
            for (var entry : repositories.entrySet()) {
                BlobStore backing = stores.open(entry.getValue(),
                    repositoryEndpoints.getOrDefault(entry.getKey(), sourceEndpoint), repositoryEnvs.get(entry.getKey()));
                opened.add(backing);
                sources.add(new RepositorySources.Source(entry.getKey(), backing));
            }
            long total = dumpIndex(sources, destination, status);
            status.printf("Dumped %d documents to %s%n", total, output);
            return 0;
        } catch (Exception e) {
            System.err.println("error: " + safeFailure(e));
            return 1;
        } finally {
            for (BlobStore store : opened) {
                try {
                    store.close();
                } catch (Exception e) {
                    System.err.println("warning: cannot close a repository store (" + e.getClass().getSimpleName() + ")");
                }
            }
        }
    }

    /** Everything that can refuse the index runs before the destination is opened. */
    private long dumpIndex(RepositorySources sources, Destination destination, PrintStream status) throws IOException {
        var inspector = sources.primary().inspector();
        if (inspector.overview().indices().stream().noneMatch(i -> i.name().equals(indexName)))
            throw new InvalidDumpRequestException("the index is not in the repository; the info command lists its indices");
        String snapshotUuid = inspector.latestSnapshotUuid(indexName);
        var resolved = sources.resolve(indexName, snapshotUuid);
        ExportPreflight.require(indexName, resolved);
        int shardCount = resolved.shardCount();
        if (resolved.referenceChain().size() > 1) {
            status.println("Dumping " + indexName + " from snapshot " + inspector.requireSnapshot(indexName, snapshotUuid).name()
                + " via " + String.join(" -> ", resolved.referenceChain()));
        }

        var materializer = new ShardMaterializer(resolved.source().blobs());
        return destination.write(out -> {
            long total = 0;
            for (int shardId = 0; shardId < shardCount; shardId++) {
                ShardSnapshot shard = resolved.source().inspector()
                    .shardSnapshot(resolved.indexName(), shardId, resolved.snapshotUuid());
                total += dumpShard(shard, materializer, resolved, out);
            }
            return total;
        });
    }

    private static long dumpShard(ShardSnapshot shard, ShardMaterializer materializer,
                                  RepositorySources.Resolved resolved, OutputStream out) throws IOException {
        Path shardDir = TempRoot.newShardDirectory();
        long count;
        try {
            materializer.materialize(shard, shardDir);
            try (ShardReader reader = ExportReaders.withSyntheticFallback(LuceneRuntimeSelector.openLucene10(shard, shardDir),
                     shardDir, resolved.indexName(), resolved.metadata())) {
                count = writeLines(reader, out);
            }
        } catch (Exception | Error failure) {
            try {
                deleteRecursively(shardDir);
            } catch (IOException cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        deleteRecursively(shardDir);
        return count;
    }

    /**
     * Writes each live document's source bytes as one JSONL line, byte-exact except raw CR/LF,
     * with no charset decoding in between. Documents whose excluded vectors were put back are not
     * byte-exact: their vectors are Elasticsearch's rendering, appended to the stored bytes or, for
     * vectors below the root, in Elasticsearch's rendering of the whole document.
     */
    static long writeLines(ShardReader reader, OutputStream out) throws IOException {
        ShardReader.SourceConsumer line = (bytes, length) -> {
            try {
                out.write(singleLine(bytes, length), 0, length);
                out.write('\n');
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        long count = 0;
        try {
            for (int docId = 0; docId < reader.docCount(); docId++) {
                if (!reader.isLive(docId)) continue;
                reader.readSource(docId, line);
                count++;
            }
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        return count;
    }

    /**
     * JSON forbids unescaped control characters inside strings, so a raw CR or LF in valid JSON is
     * insignificant whitespace and becomes a space. UTF-8 multi-byte sequences never contain these
     * bytes. Copies only when there is something to replace.
     */
    static byte[] singleLine(byte[] bytes, int length) {
        byte[] result = bytes;
        for (int i = 0; i < length; i++) {
            if (bytes[i] == '\n' || bytes[i] == '\r') {
                if (result == bytes) result = java.util.Arrays.copyOf(bytes, length);
                result[i] = ' ';
            }
        }
        return result;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        try (var walk = Files.walk(dir)) {
            for (Path path : (Iterable<Path>) walk.sorted(Comparator.reverseOrder())::iterator) Files.delete(path);
        }
    }

    /** The error line: fixed messages of known exception types, otherwise only class and stack locations. */
    static String safeFailure(Throwable failure) {
        Throwable root = FailureDiagnostics.rootCause(failure);
        if (root instanceof FailureDiagnostics.SafeMessage || root instanceof RepositorySources.UnresolvedSnapshotException)
            return root.getClass().getSimpleName() + ": " + root.getMessage();
        var credentialFailure = CredentialSources.configurationFailure(failure);
        if (credentialFailure != null) return CredentialSources.describe(credentialFailure);
        return FailureDiagnostics.describe(failure);
    }

    /**
     * Where the JSONL goes. A regular file, or a name that does not exist yet, is written to a
     * partial file of its own beside it (beside a symbolic link's target, so the link stays), which
     * gets the replaced file's permission bits and is moved over it only when every shard is
     * written; any failure, a JVM error or a SIGTERM/SIGINT deletes it. Concurrent runs never share
     * a partial file. A name that cannot be replaced by a rename (a device such as {@code /dev/null},
     * a named pipe, a process substitution, or a descriptor path such as {@code /dev/stdout} even
     * when it is redirected to a file) is written directly, as it always was: a failure then leaves
     * what was already streamed.
     */
    record Destination(Path file, boolean direct) {

        private static final int BUFFER_BYTES = 1 << 16;

        /** Refuses, with fixed messages, what would only fail after the download. */
        static Destination of(Path output) throws IOException {
            if (Files.isDirectory(output)) throw new InvalidDumpRequestException("the output is a directory; -o names the JSONL file");
            if (namesFileDescriptor(output) || Files.exists(output) && !Files.isRegularFile(output)) return new Destination(output, true);
            Path file;
            if (Files.exists(output)) {
                file = output.toRealPath();
                if (!Files.isWritable(file)) throw new InvalidDumpRequestException("the output file is not writable");
            } else if (Files.isSymbolicLink(output)) {
                throw new InvalidDumpRequestException("the output is a symbolic link to a missing file");
            } else {
                file = output.toAbsolutePath();
            }
            Path directory = file.getParent();
            if (!Files.isDirectory(directory)) throw new InvalidDumpRequestException("the output's directory does not exist");
            if (!Files.isWritable(directory)) {
                throw new InvalidDumpRequestException("the output's directory is not writable; dump writes a partial file beside the output");
            }
            return new Destination(file, false);
        }

        /** {@code /dev/stdout}, {@code /dev/stderr}, {@code /dev/fd/N} and {@code /proc/<pid>/fd/N} name an open descriptor. */
        static boolean namesFileDescriptor(Path output) {
            Path path = output.toAbsolutePath().normalize();
            return path.equals(Path.of("/dev/stdout")) || path.equals(Path.of("/dev/stderr")) || path.startsWith("/dev/fd")
                || path.startsWith("/proc") && path.getNameCount() == 4 && path.getName(2).toString().equals("fd");
        }

        interface Body {
            long writeTo(OutputStream out) throws IOException;
        }

        /** Runs body against the destination and returns its document count. */
        long write(Body body) throws IOException {
            if (direct) {
                try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(file), BUFFER_BYTES)) {
                    return body.writeTo(out);
                }
            }
            Path partial = file.resolveSibling(file.getFileName() + "."
                + Long.toUnsignedString(ThreadLocalRandom.current().nextLong(), 36) + ".partial");
            Thread onSignal = new Thread(() -> {
                try {
                    Files.deleteIfExists(partial);
                } catch (IOException ignored) {
                    // best effort while the JVM exits
                }
            }, "gefrierschrank-dump-partial-cleanup");
            Runtime.getRuntime().addShutdownHook(onSignal);
            try {
                long total;
                try (OutputStream out = new BufferedOutputStream(
                         Files.newOutputStream(partial, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), BUFFER_BYTES)) {
                    keepPermissions(partial);
                    total = body.writeTo(out);
                }
                try {
                    Files.move(partial, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(partial, file, StandardCopyOption.REPLACE_EXISTING);
                }
                return total;
            } catch (Exception | Error failure) {
                try {
                    Files.deleteIfExists(partial);
                } catch (IOException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(onSignal);
                } catch (IllegalStateException exiting) {
                    // the hook deletes the partial file
                }
            }
        }

        /** The rename replaces the inode, so an output the operator restricted must not become world-readable. */
        private void keepPermissions(Path partial) throws IOException {
            var view = Files.getFileAttributeView(partial, PosixFileAttributeView.class);
            if (view != null && Files.exists(file)) view.setPermissions(Files.getPosixFilePermissions(file));
        }
    }

    /** A request dump refuses before reading any shard; fixed messages only. */
    static final class InvalidDumpRequestException extends IOException implements FailureDiagnostics.SafeMessage {
        InvalidDumpRequestException(String message) {
            super(message);
        }
    }
}
