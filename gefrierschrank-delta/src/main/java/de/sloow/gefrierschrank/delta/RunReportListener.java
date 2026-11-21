package de.sloow.gefrierschrank.delta;

/**
 * Callback the pipeline invokes after every shard's outcome is decided (committed, skipped, or
 * quarantined), so a long-running invocation's progress can be persisted incrementally rather
 * than only once {@code run()} returns. Deliberately just a callback: this module stays decoupled
 * from "where reports get written" (a file, stdout, wherever) - that's the caller's concern.
 */
public interface RunReportListener {

    RunReportListener NO_OP = reportSoFar -> { };

    void onShardProcessed(RunReport reportSoFar);
}
