package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@Tag("concurrency")
@Timeout(60)
class ShardWorkPoolTest {

    private static final long MIB = ShardWorkPool.MIB;

    /** Admits and starts tasks in order, the way the pipeline's dispatcher does. */
    private static <T> void admitAndStart(ShardWorkPool pool, long estimateBytes, java.util.concurrent.Callable<T> body,
                                          LinkedBlockingQueue<ShardWorkPool.Result<T>> done) throws InterruptedException {
        var lease = pool.lease(estimateBytes);
        while (!lease.tryAcquire(20, TimeUnit.MILLISECONDS)) { /* the dispatcher would drain here */ }
        pool.start(lease, body, done::add);
    }

    @Test
    void concurrencyNeverExceedsTheWorkerCount() throws Exception {
        var pool = new ShardWorkPool(3, 0);
        var running = new AtomicInteger();
        var highWater = new AtomicInteger();
        var done = new LinkedBlockingQueue<ShardWorkPool.Result<Integer>>();
        for (int i = 0; i < 12; i++) {
            admitAndStart(pool, MIB, () -> {
                highWater.accumulateAndGet(running.incrementAndGet(), Math::max);
                Thread.sleep(15);
                running.decrementAndGet();
                return 1;
            }, done);
        }
        assertTrue(pool.awaitIdle(30, TimeUnit.SECONDS));
        assertEquals(12, done.size());
        assertEquals(3, highWater.get(), "all workers used, never more");
        assertEquals(3, pool.availablePermits());
    }

    @Test
    void theBudgetIsNeverExceeded() throws Exception {
        var pool = new ShardWorkPool(8, 100);
        var inUse = new AtomicLong();
        var highWater = new AtomicLong();
        var done = new LinkedBlockingQueue<ShardWorkPool.Result<Long>>();
        long[] estimatesMiB = {40, 30, 50, 20, 60, 10, 90, 35, 25, 45};
        for (long estimate : estimatesMiB) {
            admitAndStart(pool, estimate * MIB, () -> {
                highWater.accumulateAndGet(inUse.addAndGet(estimate), Math::max);
                Thread.sleep(10);
                inUse.addAndGet(-estimate);
                return estimate;
            }, done);
        }
        assertTrue(pool.awaitIdle(30, TimeUnit.SECONDS));
        assertEquals(estimatesMiB.length, done.size());
        assertTrue(highWater.get() <= 100, "peak " + highWater.get() + " MiB");
        assertEquals(100, pool.availableBudgetMiB());
    }

    @Test
    void anEstimateLargerThanTheBudgetRunsAlone() throws Exception {
        var pool = new ShardWorkPool(4, 100);
        var running = new AtomicInteger();
        var concurrentWithGiant = new AtomicInteger();
        var giantRunning = new java.util.concurrent.atomic.AtomicBoolean();
        var done = new LinkedBlockingQueue<ShardWorkPool.Result<Integer>>();
        long[] estimatesMiB = {10, 10, 500, 10, 10};
        for (long estimate : estimatesMiB) {
            boolean giant = estimate > 100;
            assertEquals(giant ? 100 : estimate, pool.lease(estimate * MIB).budgetUnits());
            admitAndStart(pool, estimate * MIB, () -> {
                running.incrementAndGet();
                if (giant) giantRunning.set(true);
                Thread.sleep(20);
                if (giant ? running.get() > 1 : giantRunning.get()) concurrentWithGiant.incrementAndGet();
                if (giant) giantRunning.set(false);
                running.decrementAndGet();
                return 1;
            }, done);
        }
        assertTrue(pool.awaitIdle(30, TimeUnit.SECONDS));
        assertEquals(0, concurrentWithGiant.get(), "nothing overlaps the shard that takes the whole budget");
        assertEquals(100, pool.availableBudgetMiB());
    }

    @Test
    void oneWorkerAdmitsStrictlyInOrderWithoutABudget() throws Exception {
        var pool = ShardWorkPool.create(1, 0);
        assertEquals(0, pool.budgetMiB(), "one shard on disk at a time needs no budget");
        var started = Collections.synchronizedList(new ArrayList<Integer>());
        var done = new LinkedBlockingQueue<ShardWorkPool.Result<Integer>>();
        for (int i = 0; i < 8; i++) {
            int id = i;
            admitAndStart(pool, Long.MAX_VALUE / 2, () -> {
                started.add(id);
                Thread.sleep(2);
                return id;
            }, done);
        }
        assertTrue(pool.awaitIdle(30, TimeUnit.SECONDS));
        assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7), started);
    }

    @Test
    void threadsAreNamedDaemonThreadsCarryingTheSubmittersContextClassLoader() throws Exception {
        var pool = new ShardWorkPool(2, 0);
        var marker = new URLClassLoader(new URL[0], getClass().getClassLoader());
        var observed = new LinkedBlockingQueue<Object[]>();
        var done = new LinkedBlockingQueue<ShardWorkPool.Result<Integer>>();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(marker);
        try {
            for (int i = 0; i < 2; i++) {
                admitAndStart(pool, MIB, () -> {
                    Thread current = Thread.currentThread();
                    observed.add(new Object[]{current.isDaemon(), current.getContextClassLoader(), current.getName()});
                    return 1;
                }, done);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
        assertTrue(pool.awaitIdle(30, TimeUnit.SECONDS));
        for (Object[] thread : observed) {
            assertEquals(true, thread[0]);
            assertSame(marker, thread[1]);
            assertTrue(((String) thread[2]).startsWith("gefrierschrank-shard-"), (String) thread[2]);
        }
        assertEquals(2, observed.size());
    }

    @Test
    void anErrorInATaskStopsAdmissionAndReachesTheDispatcher() throws Exception {
        var pool = new ShardWorkPool(2, 100);
        var done = new LinkedBlockingQueue<ShardWorkPool.Result<Integer>>();
        var crash = new AssertionError("simulated VM-level failure");
        admitAndStart(pool, MIB, () -> { throw crash; }, done);
        var result = done.poll(30, TimeUnit.SECONDS);
        assertNotNull(result);
        assertSame(crash, result.failure());
        var next = pool.lease(MIB);
        assertSame(crash, assertThrows(AssertionError.class, () -> next.tryAcquire(1, TimeUnit.MILLISECONDS)));
        assertTrue(pool.awaitIdle(30, TimeUnit.SECONDS));
        assertEquals(2, pool.availablePermits(), "the refused lease holds nothing");
        assertEquals(100, pool.availableBudgetMiB());
    }

    /**
     * Handing a result over allocates, so an OutOfMemoryError can lose it; a dispatcher waiting for
     * that result must learn of the Error from the pool instead, or it waits forever.
     */
    @Test
    void anErrorHandingOverAResultStopsAdmissionAndReachesTheDispatcher() throws Exception {
        var pool = new ShardWorkPool(2, 100);
        var lost = new OutOfMemoryError("injected: the result could not be queued");
        var lease = pool.lease(MIB);
        assertTrue(lease.tryAcquire(1, TimeUnit.SECONDS));
        pool.start(lease, () -> 1, result -> { throw lost; });
        assertTrue(pool.awaitIdle(30, TimeUnit.SECONDS));
        assertSame(lost, assertThrows(OutOfMemoryError.class, pool::rethrowFatal));
        var next = pool.lease(MIB);
        assertSame(lost, assertThrows(OutOfMemoryError.class, () -> next.tryAcquire(1, TimeUnit.MILLISECONDS)));
        assertEquals(2, pool.availablePermits(), "the lost task's lease is released");
        assertEquals(100, pool.availableBudgetMiB());
    }

    @Test
    void noPermitOrBudgetLeaksAfterFailures() throws Exception {
        var pool = new ShardWorkPool(3, 50);
        var done = new LinkedBlockingQueue<ShardWorkPool.Result<Integer>>();
        var release = new CountDownLatch(1);
        for (int i = 0; i < 9; i++) {
            int id = i;
            admitAndStart(pool, 10 * MIB, () -> {
                if (id < 3) release.await();
                if (id % 2 == 0) throw new IllegalStateException("shard " + id);
                return id;
            }, done);
            if (id == 2) release.countDown();
        }
        assertTrue(pool.awaitIdle(30, TimeUnit.SECONDS));
        assertEquals(9, done.size());
        assertEquals(5, done.stream().filter(r -> r.failure() != null).count());
        assertEquals(3, pool.availablePermits());
        assertEquals(50, pool.availableBudgetMiB());
    }

    @Test
    void aPartiallyAcquiredLeaseReleasesWhatItHolds() throws Exception {
        var pool = new ShardWorkPool(2, 10);
        var hog = pool.lease(10 * MIB);
        assertTrue(hog.tryAcquire(1, TimeUnit.SECONDS));
        var waiting = pool.lease(5 * MIB);
        assertFalse(waiting.tryAcquire(20, TimeUnit.MILLISECONDS), "permit free, budget not");
        assertEquals(0, pool.availablePermits());
        waiting.release();
        hog.release();
        hog.release();
        assertEquals(2, pool.availablePermits());
        assertEquals(10, pool.availableBudgetMiB());
    }

    @Test
    void createDerivesADefaultBudgetOnlyForSeveralWorkers() {
        assertEquals(0, ShardWorkPool.create(1, 123).budgetMiB());
        assertEquals(123, ShardWorkPool.create(4, 123).budgetMiB());
        assertTrue(ShardWorkPool.create(4, 0).budgetMiB() > 0, "80% of the temp dir's usable space");
    }

    @Test
    void resourceWarningsFlagHeapAndConnectionPoolPressure() {
        // One thread per shard, so the counts do not depend on the machine's cores (the default is auto).
        String previous = System.setProperty(ShardRanges.SHARD_THREADS_KEY, "1");
        try {
            var settings = new ExportSettings(SnapshotSelection.AUTO, 8, 0);
            assertEquals(2, settings.resourceWarnings(4L * 1024 * MIB, 8, 50).size());
            assertEquals(List.of(), settings.resourceWarnings(16L * 1024 * MIB, 8, 128));
            assertEquals(List.of(), ExportSettings.DEFAULT.resourceWarnings(MIB, 8, 1));
            assertEquals(List.of(), settings.resourceWarnings(Long.MAX_VALUE, 16, 128));
        } finally {
            if (previous == null) System.clearProperty(ShardRanges.SHARD_THREADS_KEY);
            else System.setProperty(ShardRanges.SHARD_THREADS_KEY, previous);
        }
    }
}
