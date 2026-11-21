package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import de.sloow.gefrierschrank.delta.RunReport;
import de.sloow.gefrierschrank.delta.RunReportListener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Writes the run report to a local JSON file after every shard, so a report reflecting progress up
 * to the last completed shard exists while a run is still going and after a hard crash, rather
 * than nothing at all. A local path is used
 * deliberately (not S3): reports must be readable without the source/target S3 credentials.
 *
 * <p>Atomic write: each update is written to a sibling temp file, then moved over the real path
 * with {@code ATOMIC_MOVE}. A reader opening {@code reportPath} therefore only ever sees either
 * the previous complete report or the new complete one - never a half-written file.
 */
final class JsonReportWriter implements RunReportListener {

    private final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private final Path reportPath;
    private boolean warned;

    JsonReportWriter(Path reportPath) {
        this.reportPath = reportPath.toAbsolutePath();
    }

    /**
     * Never propagates an I/O failure: reporting is observability, and a report that can't be
     * written must not destroy the extraction it is reporting on: the shards' data is the valuable
     * part and the report is not ({@code DumpToDeltaPipeline} also catches listener exceptions, as
     * defense in depth). Warns once per writer rather than per shard, so a persistently
     * unwritable report directory doesn't drown the real progress output.
     */
    @Override
    public void onShardProcessed(RunReport reportSoFar) {
        try {
            // The report directory may not exist yet: with --all-indices this listener fires on
            // the first shard of the first index, before any manifest write has created it.
            Files.createDirectories(reportPath.getParent());
            Path tmp = Files.createTempFile(reportPath.getParent(),
                reportPath.getFileName().toString(), ".tmp");
            mapper.writeValue(tmp.toFile(), reportSoFar);
            Files.move(tmp, reportPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException | RuntimeException e) {
            if (!warned) {
                warned = true;
                System.err.println("warning: cannot write run report to " + reportPath
                    + " (" + e.getClass().getSimpleName() + ": " + e.getMessage()
                    + ") - the dump continues; further report-write failures for this index are not repeated");
            }
        }
    }
}
