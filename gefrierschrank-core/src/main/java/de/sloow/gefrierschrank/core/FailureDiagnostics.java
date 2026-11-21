package de.sloow.gefrierschrank.core;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/** Diagnostic locations only: exception messages may contain source values or credentials. */
public final class FailureDiagnostics {
    private FailureDiagnostics() {}

    /**
     * Marks an exception whose message is fixed text naming only settings, options and allowed
     * values, never document content, paths or credentials, so {@link #describe} may print it.
     */
    public interface SafeMessage {}

    /**
     * The first {@code type} among {@code failure}, its causes and everything suppressed along the
     * way, breadth first, or null. A failure that ended a shard or index can carry another one
     * that decides how the run goes on: a second shard's failure is suppressed into the first,
     * and several ranges of one shard fail together.
     */
    public static <T extends Throwable> T find(Throwable failure, Class<T> type) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        java.util.ArrayDeque<Throwable> pending = new java.util.ArrayDeque<>();
        pending.add(failure);
        while (!pending.isEmpty()) {
            Throwable next = pending.poll();
            if (!seen.add(next)) continue;
            if (type.isInstance(next)) return type.cast(next);
            if (next.getCause() != null) pending.add(next.getCause());
            for (Throwable suppressed : next.getSuppressed()) pending.add(suppressed);
        }
        return null;
    }

    public static Throwable rootCause(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        seen.add(failure);
        while (failure.getCause() != null && seen.add(failure.getCause())) {
            failure = failure.getCause();
        }
        return failure;
    }

    public static String describe(Throwable failure) {
        Throwable root = rootCause(failure);
        StringBuilder result = new StringBuilder(root.getClass().getName());
        if (root instanceof SafeMessage && root.getMessage() != null) result.append(": ").append(root.getMessage());
        StackTraceElement[] stack = root.getStackTrace();
        for (int i = 0; i < Math.min(stack.length, 12); i++) {
            result.append(i == 0 ? " at " : " <- ").append(stack[i]);
        }
        if (stack.length > 12) result.append(" <- ...");
        return result.toString();
    }
}
