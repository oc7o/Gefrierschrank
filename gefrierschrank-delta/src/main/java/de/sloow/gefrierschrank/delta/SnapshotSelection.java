package de.sloow.gefrierschrank.delta;

import java.util.Objects;

/**
 * Which retained snapshot of an index {@link FullIndexExporter} exports.
 *
 * <ul>
 *   <li>{@code AUTO}: a table that already holds committed shards (or only its
 *       {@code _export_metadata} row) of one snapshot that is still retained is completed from
 *       that snapshot; otherwise the latest retained snapshot is used. A rerun after the next
 *       SLM snapshot therefore resumes instead of conflicting, and the table stays as of the
 *       snapshot it started from ({@link RunReport.SnapshotInfo#newerSnapshotAvailable}). A
 *       mounted index's table whose shell snapshot retention has since deleted is completed as of
 *       that snapshot too, from the latest snapshot's unchanged backing data
 *       ({@link FullIndexExporter#RESUME_RETIRED_SHELLS}).</li>
 *   <li>{@code LATEST}: always the latest retained snapshot; a table started from an older
 *       snapshot is refused with {@link DeltaTableWriter.ArchiveTargetConflictException}.</li>
 *   <li>{@code EXPLICIT}: the snapshot whose UUID or name equals {@code token}.</li>
 * </ul>
 */
public record SnapshotSelection(Mode mode, String token) {

    public enum Mode { AUTO, LATEST, EXPLICIT }

    public static final SnapshotSelection AUTO = new SnapshotSelection(Mode.AUTO, null);
    public static final SnapshotSelection LATEST = new SnapshotSelection(Mode.LATEST, null);

    /** The one place the default lives; changing it changes what a plain rerun does. */
    public static final SnapshotSelection DEFAULT = AUTO;

    public SnapshotSelection {
        Objects.requireNonNull(mode, "mode");
        if ((mode == Mode.EXPLICIT) != (token != null))
            throw new IllegalArgumentException("Only an explicit snapshot selection carries a token");
        if (token != null && token.isBlank()) throw new IllegalArgumentException("Snapshot name or UUID is blank");
    }

    public static SnapshotSelection explicit(String nameOrUuid) {
        return new SnapshotSelection(Mode.EXPLICIT, nameOrUuid);
    }

    /** {@code auto} and {@code latest} are keywords; anything else names a snapshot by name or UUID. */
    public static SnapshotSelection parse(String value) {
        return switch (value) {
            case "auto" -> AUTO;
            case "latest" -> LATEST;
            default -> explicit(value);
        };
    }

    /** Report label; never the token, which already appears as the resolved snapshot. */
    public String label() {
        return mode.name().toLowerCase(java.util.Locale.ROOT);
    }
}
