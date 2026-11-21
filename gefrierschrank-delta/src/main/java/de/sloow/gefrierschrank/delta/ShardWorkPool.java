package de.sloow.gefrierschrank.delta;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Bounds concurrent shard tasks: at most {@code workers} run at once, and their estimated
 * temp-disk use (materialized shard files) stays within a budget counted in MiB. One pool serves
 * a whole CLI run. Each index's dispatcher thread requests admission in shard order, and with
 * several indices in flight their requests are admitted first come, first served across
 * dispatchers (see {@link Lease#tryAcquire}).
 *
 * <p>Each task gets a fresh platform daemon thread named {@code gefrierschrank-shard-N} whose
 * context class loader is the submitting thread's. Deliberately no ForkJoin or other shared
 * executor: the shared {@code IsolatedEsClassLoader} derives its classpath from the context class
 * loader of whichever thread builds it first, and thread-locals that Elasticsearch code sets
 * during a shard's reconstruction must die with their shard. Daemon
 * threads keep a fatal {@link Error} equivalent to a crash: the JVM exits without waiting for
 * in-flight shards, whose uncommitted files are orphans exactly as after {@code kill -9}.
 */
public final class ShardWorkPool {

    static final long MIB = 1024L * 1024L;

    private final int workers;
    private final int budgetMiB;
    private final Semaphore permits;
    private final Semaphore budget;
    private final AtomicInteger threadIds = new AtomicInteger();
    private final Object idleMonitor = new Object();
    private int running;
    private volatile Error fatal;
    // Leases that asked for admission and are neither admitted nor released, by arrival.
    private final Object admissionQueue = new Object();
    private final TreeSet<Long> waiting = new TreeSet<>();
    private long nextTicket;

    /**
     * @param budgetMiB temp-disk budget; 0 or less means unbounded. Capped at {@link Integer#MAX_VALUE} MiB.
     */
    ShardWorkPool(int workers, long budgetMiB) {
        if (workers < 1) throw new IllegalArgumentException("workers must be positive");
        this.workers = workers;
        this.budgetMiB = budgetMiB <= 0 ? 0 : (int) Math.min(budgetMiB, Integer.MAX_VALUE);
        this.permits = new Semaphore(workers, true);
        this.budget = this.budgetMiB == 0 ? null : new Semaphore(this.budgetMiB, true);
    }

    /**
     * A pool for {@code workers} concurrent shards. With one worker exactly one shard is on disk at
     * a time, so no budget applies. Otherwise {@code tempBudgetMiB}, or when that is 0, 80% of the
     * usable space of {@code java.io.tmpdir} now; unbounded if that cannot be determined.
     */
    public static ShardWorkPool create(int workers, long tempBudgetMiB) {
        if (workers == 1) return new ShardWorkPool(1, 0);
        return new ShardWorkPool(workers, tempBudgetMiB > 0 ? tempBudgetMiB : defaultBudgetMiB());
    }

    static long defaultBudgetMiB() {
        try {
            Path tmp = Path.of(System.getProperty("java.io.tmpdir"));
            return Math.max(1, Files.getFileStore(tmp).getUsableSpace() / 10 * 8 / MIB);
        } catch (IOException | RuntimeException e) {
            System.err.println("warning: cannot determine free temp space (" + e.getClass().getSimpleName()
                + "); concurrent shards are bounded by --parallelism only, set --temp-budget");
            return 0;
        }
    }

    public int workers() {
        return workers;
    }

    /** The temp-disk budget in MiB, or 0 when unbounded. */
    public long budgetMiB() {
        return budgetMiB;
    }

    /** An admission request for one task expected to occupy {@code estimateBytes} of temp disk. */
    Lease lease(long estimateBytes) {
        long estimateMiB = Math.max(1, (estimateBytes + MIB - 1) / MIB);
        // A shard larger than the whole budget takes all of it and so runs alone rather than never.
        return new Lease(budget == null ? 0 : (int) Math.min(estimateMiB, budgetMiB));
    }

    /**
     * Runs {@code body} on a new daemon thread and hands its result to {@code onDone}, then
     * releases the lease: whoever acquires the freed permit or budget can already see the result,
     * so a dispatcher that checks for finished tasks after each admission never admits past a
     * failure it could have seen. An {@link Error} from a body stops all further admission, and so
     * does one that keeps the result from reaching {@code onDone}'s receiver (an OutOfMemoryError
     * while allocating it): a dispatcher waiting for that result learns of it through
     * {@link #rethrowFatal()} instead.
     */
    <T> void start(Lease lease, Callable<T> body, Consumer<Result<T>> onDone) {
        if (!lease.admitted()) throw new IllegalStateException("lease not admitted");
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        Thread thread = new Thread(() -> {
            try {
                Result<T> result;
                try {
                    result = new Result<>(body.call(), null);
                } catch (Throwable failure) {
                    if (failure instanceof Error error) stop(error);
                    result = new Result<>(null, failure);
                }
                onDone.accept(result);
            } catch (Throwable lost) {
                stop(lost instanceof Error error ? error : new AssertionError("a shard task's result was lost", lost));
            } finally {
                lease.release();
                synchronized (idleMonitor) {
                    running--;
                    idleMonitor.notifyAll();
                }
            }
        }, "gefrierschrank-shard-" + threadIds.getAndIncrement());
        thread.setDaemon(true);
        thread.setContextClassLoader(contextLoader);
        synchronized (idleMonitor) {
            running++;
        }
        try {
            thread.start();
        } catch (Throwable failure) {
            lease.release();
            synchronized (idleMonitor) {
                running--;
                idleMonitor.notifyAll();
            }
            throw failure;
        }
    }

    /** Stops all further admission. */
    private void stop(Error error) {
        if (fatal == null) fatal = error;
    }

    /** Rethrows the Error that stopped admission, if any. */
    void rethrowFatal() {
        Error stopped = fatal;
        if (stopped != null) throw stopped;
    }

    /** Waits until no started task is still running. */
    boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (idleMonitor) {
            while (running > 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) return false;
                TimeUnit.NANOSECONDS.timedWait(idleMonitor, remaining);
            }
            return true;
        }
    }

    int availablePermits() {
        return permits.availablePermits();
    }

    /** Free budget in MiB, or -1 when unbounded. */
    int availableBudgetMiB() {
        return budget == null ? -1 : budget.availablePermits();
    }

    /** A body's return value or its failure. */
    record Result<T>(T value, Throwable failure) {}

    /**
     * One task's claim on a worker permit and its share of the budget. Acquired first the permit,
     * then the budget, possibly over several {@link #tryAcquire} calls; the dispatcher keeps a
     * partially acquired lease while it handles finished tasks in between.
     */
    final class Lease {
        private final int budgetUnits;
        private boolean permitHeld;
        private boolean budgetHeld;
        private boolean released;
        private long ticket = -1; // guarded by admissionQueue
        private boolean queued; // guarded by admissionQueue

        private Lease(int budgetUnits) {
            this.budgetUnits = budgetUnits;
        }

        /**
         * True once both the permit and the budget share are held. Rethrows a task's fatal Error
         * instead, also when that task's freed permit is what this call just acquired.
         *
         * <p>The first call queues the lease, and only the oldest queued lease of the whole pool
         * acquires anything, so admission is first come, first served across all dispatchers even
         * though each one polls with timeouts: a large lease cannot be overtaken indefinitely by
         * another index's small ones taking every freed MiB. Worker threads release leases without
         * touching the queue's order, so a waiting dispatcher never blocks a release.
         */
        boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
            stopIfFatal();
            if (released) throw new IllegalStateException("lease already released");
            if (permitHeld && budgetHeld) return true;
            long deadline = System.nanoTime() + unit.toNanos(timeout);
            if (awaitTurn(deadline)) {
                if (!permitHeld) permitHeld = permits.tryAcquire(remaining(deadline), TimeUnit.NANOSECONDS);
                if (permitHeld && !budgetHeld) budgetHeld = budgetUnits == 0
                    || budget.tryAcquire(budgetUnits, remaining(deadline), TimeUnit.NANOSECONDS);
                if (permitHeld && budgetHeld) leaveQueue();
            }
            stopIfFatal();
            return permitHeld && budgetHeld;
        }

        /** Queues this lease on its first call; true once it is the oldest queued lease. */
        private boolean awaitTurn(long deadline) throws InterruptedException {
            synchronized (admissionQueue) {
                if (ticket < 0) {
                    ticket = nextTicket++;
                    waiting.add(ticket);
                    queued = true;
                }
                while (waiting.first() != ticket) {
                    long remaining = remaining(deadline);
                    if (remaining == 0) return false;
                    TimeUnit.NANOSECONDS.timedWait(admissionQueue, remaining);
                }
                return true;
            }
        }

        /**
         * Allocates nothing once the lease has left the queue, as when a task's thread releases it:
         * an OutOfMemoryError boxing the ticket there would keep the permit and budget forever.
         */
        private void leaveQueue() {
            synchronized (admissionQueue) {
                if (!queued) return;
                queued = false;
                waiting.remove(ticket);
                admissionQueue.notifyAll();
            }
        }

        private static long remaining(long deadline) {
            return Math.max(0, deadline - System.nanoTime());
        }

        private void stopIfFatal() {
            Error stopped = fatal;
            if (stopped == null) return;
            release();
            throw stopped;
        }

        boolean admitted() {
            return permitHeld && budgetHeld && !released;
        }

        int budgetUnits() {
            return budgetUnits;
        }

        /** Leaves the admission queue, then releases the budget, then the permit; idempotent. */
        synchronized void release() {
            if (released) return;
            released = true;
            leaveQueue();
            if (budgetHeld && budgetUnits > 0) budget.release(budgetUnits);
            if (permitHeld) permits.release();
        }
    }
}
