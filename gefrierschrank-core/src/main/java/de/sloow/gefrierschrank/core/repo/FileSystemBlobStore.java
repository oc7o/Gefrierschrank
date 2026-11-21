package de.sloow.gefrierschrank.core.repo;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** BlobStore over a local snapshot-repository directory. */
public final class FileSystemBlobStore implements BlobStore {

    private final Path root;

    public FileSystemBlobStore(Path root) {
        this.root = root;
    }

    @Override
    public Optional<BlobMetadata> stat(String key) {
        Path file = resolve(key);
        try {
            return Files.isRegularFile(file)
                ? Optional.of(new BlobMetadata(key, Files.size(file)))
                : Optional.empty();
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    @Override
    public InputStream read(String key) throws IOException {
        return Files.newInputStream(resolve(key));
    }

    /**
     * The version is size, modification time and file key, so an overwrite or replacement is
     * detected. The path is read before and after the channel opens: a file replaced in between
     * could otherwise get the new file's version for the old file's bytes.
     */
    @Override
    public RangeRead openRange(String key, long offset, long length, String expectedVersion) throws IOException {
        BlobStore.requireValidRange(offset, length);
        Path file = resolve(key);
        String version = version(file);
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        try {
            long size = channel.size();
            if (!version.equals(version(file)) || !version.startsWith(size + ":")) throw new IOException(BLOB_CHANGED);
            if (expectedVersion != null && !expectedVersion.equals(version)) throw new IOException(BLOB_CHANGED);
            if (offset + length > size) throw new IOException(RANGE_OUTSIDE_BLOB);
            channel.position(offset);
            return new RangeRead(new ExactLengthInputStream(Channels.newInputStream(channel), length), version, size);
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    private static String version(Path file) throws IOException {
        var attributes = Files.readAttributes(file, BasicFileAttributes.class);
        return attributes.size() + ":" + attributes.lastModifiedTime().to(TimeUnit.NANOSECONDS) + ":" + attributes.fileKey();
    }

    @Override
    public List<String> list(String prefix) {
        Path dir = resolve(prefix);
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.walk(dir)) {
            return entries
                .filter(Files::isRegularFile)
                .sorted()
                .map(p -> root.relativize(p).toString())
                .toList();
        } catch (IOException e) {
            return List.of();
        }
    }

    @Override
    public List<BlobMetadata> listWithSizes(String prefix) {
        Path dir = resolve(prefix);
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        try (Stream<Path> entries = Files.walk(dir)) {
            return entries
                .filter(Files::isRegularFile)
                .sorted()
                .map(p -> {
                    try {
                        return new BlobMetadata(root.relativize(p).toString(), Files.size(p));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .toList();
        } catch (IOException e) {
            return List.of();
        }
    }

    @Override
    public void put(String key, byte[] content) {
        try {
            Path file = resolve(key);
            Files.createDirectories(file.getParent());
            Files.write(file, content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path resolve(String key) {
        return root.resolve(key);
    }
}
