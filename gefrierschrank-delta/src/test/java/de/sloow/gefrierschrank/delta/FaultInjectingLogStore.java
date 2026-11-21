package de.sloow.gefrierschrank.delta;

import io.delta.storage.CloseableIterator;
import io.delta.storage.LocalLogStore;
import io.delta.storage.LogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Local log store that can fail commit writes and log listings on demand and records which log
 * files were read.
 * Enabled per table through the Hadoop conf ({@link #enable}); its switches are static because
 * delta-standalone instantiates the store itself.
 */
public final class FaultInjectingLogStore extends LogStore {

    /** The FS_ERROR modes throw what Hadoop's local filesystem throws on ENOSPC or EIO. */
    enum Mode { THROW_BEFORE_WRITE, THROW_AFTER_WRITE, FS_ERROR_BEFORE_WRITE, FS_ERROR_AFTER_WRITE }

    private static volatile Mode mode = Mode.THROW_BEFORE_WRITE;
    private static final AtomicInteger failuresLeft = new AtomicInteger();
    private static volatile boolean failListings;
    static final List<String> reads = new CopyOnWriteArrayList<>();

    private final LogStore delegate;

    public FaultInjectingLogStore(Configuration conf) {
        super(conf);
        delegate = new LocalLogStore(conf);
    }

    static Configuration enable(Configuration conf) {
        conf.set("delta.logStore.class", FaultInjectingLogStore.class.getName());
        return conf;
    }

    /** The next {@code count} commit (delta JSON) writes fail as {@code failure} describes. */
    static void failNextCommits(Mode failure, int count) {
        mode = failure;
        failuresLeft.set(count);
    }

    /** Every log listing, and so every DeltaLog.update(), fails until {@link #reset}. */
    static void failListings() {
        failListings = true;
    }

    static void reset() {
        failuresLeft.set(0);
        failListings = false;
        reads.clear();
    }

    /** Commit versions whose delta JSON was read since the last reset. */
    static List<Long> deltaVersionsRead() {
        return reads.stream().filter(name -> name.matches("\\d{20}\\.json"))
            .map(name -> Long.parseLong(name.substring(0, 20))).toList();
    }

    @Override
    public CloseableIterator<String> read(Path path, Configuration conf) throws IOException {
        reads.add(path.getName());
        return delegate.read(path, conf);
    }

    @Override
    public void write(Path path, Iterator<String> actions, Boolean overwrite, Configuration conf) throws IOException {
        boolean commit = path.getName().matches("\\d{20}\\.json");
        boolean fail = commit && failuresLeft.getAndUpdate(left -> Math.max(0, left - 1)) > 0;
        if (fail && mode == Mode.THROW_BEFORE_WRITE) throw new IOException("injected failure before the commit write");
        if (fail && mode == Mode.FS_ERROR_BEFORE_WRITE) throw fsError("No space left on device");
        delegate.write(path, actions, overwrite, conf);
        if (fail && mode == Mode.FS_ERROR_AFTER_WRITE) throw fsError("No space left on device");
        if (fail) throw new IOException("injected failure after the commit write");
    }

    /** Hadoop's FSError has a package-private constructor only. */
    static org.apache.hadoop.fs.FSError fsError(String errno) {
        try {
            var constructor = org.apache.hadoop.fs.FSError.class.getDeclaredConstructor(Throwable.class);
            constructor.setAccessible(true);
            return constructor.newInstance(new IOException(errno));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public Iterator<FileStatus> listFrom(Path path, Configuration conf) throws IOException {
        if (failListings) throw new IOException("injected listing failure");
        return delegate.listFrom(path, conf);
    }

    @Override
    public Path resolvePathOnPhysicalStorage(Path path, Configuration conf) throws IOException {
        return delegate.resolvePathOnPhysicalStorage(path, conf);
    }

    @Override
    public Boolean isPartialWriteVisible(Path path, Configuration conf) throws IOException {
        return delegate.isPartialWriteVisible(path, conf);
    }
}
