package de.sloow.gefrierschrank.cli;

import de.sloow.gefrierschrank.core.repo.RepoFileInfo;
import de.sloow.gefrierschrank.core.repo.ShardSnapshot;
import de.sloow.gefrierschrank.lucene10.Lucene10ShardReader;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Every shard is read in-process on Lucene 10. Lucene 10 opens indices created up to two majors
 * back, read-only, and {@link Lucene10ShardReader} opens every commit with a created-major floor
 * of 8, so shards written by Lucene 8 (ES 7), 9 (ES 8) and 10 (ES 9) are all read in-process,
 * including ES 7-created indices whose segments were all rewritten under ES 8. Shards with older
 * files (Lucene 7, ES 6-created indices) fail loudly in-process.
 *
 * <p>The Lucene 9 worker JVM was removed in 1.2.0. Its opt-out
 * {@code -Dgefrierschrank.lucene8.runtime=worker} is still recognised, only to refuse the run as
 * a usage error before any repository read rather than to export what the setting no longer
 * selects.
 */
public final class LuceneRuntimeSelector {

    static final String LUCENE8_RUNTIME_PROPERTY = "gefrierschrank.lucene8.runtime";
    static final String WORKER_REMOVED = "the Lucene 9 worker was removed in 1.2.0";
    static final String INVALID_RUNTIME = LUCENE8_RUNTIME_PROPERTY + " must be 'in-process' (" + WORKER_REMOVED + ")";

    private LuceneRuntimeSelector() {}

    /** Shard files without any {@code written_by} version fail the shard, as they did when it picked a runtime. */
    static void requireWrittenBy(ShardSnapshot shard) {
        if (shard.files().stream().map(RepoFileInfo::writtenBy).allMatch(v -> v == null)) {
            throw new IllegalArgumentException(
                "Shard files carry no written_by version; cannot pick a Lucene runtime");
        }
    }

    /**
     * The fixed refusal for a set {@code gefrierschrank.lucene8.runtime}, or null when it is unset,
     * blank or {@code in-process}. The value itself is never echoed.
     */
    static String lucene8RuntimeRefusal() {
        String value = System.getProperty(LUCENE8_RUNTIME_PROPERTY);
        if (value == null || value.isBlank() || value.equals("in-process")) return null;
        return value.equals("worker") ? WORKER_REMOVED : INVALID_RUNTIME;
    }

    /** Commands call this before any repository access, so a set opt-out exits 2 with its fixed message. */
    static void requireInProcessRuntime(CommandLine.Model.CommandSpec spec) {
        String refusal = lucene8RuntimeRefusal();
        if (refusal != null) throw new CommandLine.ParameterException(spec.commandLine(), refusal);
    }

    public static Lucene10ShardReader openLucene10(ShardSnapshot shard, Path shardDir) throws IOException {
        requireWrittenBy(shard);
        return new Lucene10ShardReader(shardDir);
    }
}
