package de.sloow.gefrierschrank.delta;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Several indices in flight means several dispatcher threads polling one {@link ShardWorkPool}
 * with timed tryAcquire calls. Admission must stay first come, first served across them.
 */
@Tag("concurrency")
@Timeout(60)
class FifoAdmissionTest {

    private static final long MIB = ShardWorkPool.MIB;

    @Test
    void aLargeLeaseIsNotOvertakenByAnotherDispatchersSmallOnes() throws Exception {
        var pool = new ShardWorkPool(3, 100);
        var hog = pool.lease(60 * MIB);
        assertTrue(hog.tryAcquire(1, TimeUnit.SECONDS));
        var admitted = Collections.synchronizedList(new ArrayList<String>());

        var big = pool.lease(100 * MIB);
        var bigQueued = new CountDownLatch(1);
        Thread bigDispatcher = dispatcher(() -> {
            boolean first = true;
            while (!big.tryAcquire(20, TimeUnit.MILLISECONDS)) {
                if (first) bigQueued.countDown();
                first = false;
            }
            admitted.add("big");
        });
        assertTrue(bigQueued.await(10, TimeUnit.SECONDS));

        var small = pool.lease(10 * MIB);
        Thread smallDispatcher = dispatcher(() -> {
            while (!small.tryAcquire(20, TimeUnit.MILLISECONDS)) { /* collect finished tasks here */ }
            admitted.add("small");
        });

        Thread.sleep(300);
        assertEquals(List.of(), admitted, "40 MiB and a permit are free, but the older large lease comes first");
        hog.release();
        bigDispatcher.join(10_000);
        assertEquals(List.of("big"), admitted);
        Thread.sleep(100);
        assertEquals(List.of("big"), admitted, "the whole budget is taken");
        big.release();
        smallDispatcher.join(10_000);
        assertEquals(List.of("big", "small"), admitted);
        small.release();
        assertEquals(3, pool.availablePermits());
        assertEquals(100, pool.availableBudgetMiB());
    }

    @Test
    void theBudgetAndWorkerBoundHoldAcrossDispatchers() throws Exception {
        var pool = new ShardWorkPool(4, 100);
        var inUse = new AtomicLong();
        var budgetHighWater = new AtomicLong();
        var running = new AtomicLong();
        var workerHighWater = new AtomicLong();
        var done = new LinkedBlockingQueue<ShardWorkPool.Result<Long>>();
        var failure = new AtomicReference<Throwable>();
        var dispatchers = new ArrayList<Thread>();
        for (int d = 0; d < 3; d++) {
            var random = new Random(d);
            dispatchers.add(dispatcher(() -> {
                for (int i = 0; i < 25; i++) {
                    long estimate = 5 + random.nextInt(60);
                    var lease = pool.lease(estimate * MIB);
                    while (!lease.tryAcquire(5, TimeUnit.MILLISECONDS)) { /* collect */ }
                    pool.start(lease, () -> {
                        workerHighWater.accumulateAndGet(running.incrementAndGet(), Math::max);
                        budgetHighWater.accumulateAndGet(inUse.addAndGet(estimate), Math::max);
                        Thread.sleep(3);
                        inUse.addAndGet(-estimate);
                        running.decrementAndGet();
                        return estimate;
                    }, done::add);
                }
            }, failure));
        }
        for (Thread dispatcher : dispatchers) dispatcher.join(30_000);
        assertNull(failure.get());
        assertTrue(pool.awaitIdle(30, TimeUnit.SECONDS));
        assertEquals(75, done.size());
        assertTrue(budgetHighWater.get() <= 100, "peak " + budgetHighWater.get() + " MiB");
        assertTrue(workerHighWater.get() <= 4, "peak " + workerHighWater.get() + " workers");
        assertEquals(4, pool.availablePermits());
        assertEquals(100, pool.availableBudgetMiB());
    }

    @Test
    void anAbandonedLeaseLeavesTheQueue() throws Exception {
        var pool = new ShardWorkPool(2, 10);
        var hog = pool.lease(10 * MIB);
        assertTrue(hog.tryAcquire(1, TimeUnit.SECONDS));
        var head = pool.lease(5 * MIB);
        assertFalse(head.tryAcquire(20, TimeUnit.MILLISECONDS), "queued first: holds the free permit, waits for budget");
        var behind = pool.lease(MIB);
        assertFalse(behind.tryAcquire(20, TimeUnit.MILLISECONDS), "queued behind the head");
        assertEquals(0, pool.availablePermits());

        behind.release();
        head.release();
        assertEquals(1, pool.availablePermits(), "the abandoned head gave its permit back");
        hog.release();
        var next = pool.lease(5 * MIB);
        assertTrue(next.tryAcquire(1, TimeUnit.SECONDS), "neither abandoned lease still blocks the queue");
        next.release();
        assertEquals(2, pool.availablePermits());
        assertEquals(10, pool.availableBudgetMiB());
    }

    @Test
    void aFatalErrorReleasesQueuedLeasesOfEveryDispatcher() throws Exception {
        var pool = new ShardWorkPool(1, 0);
        var crash = new AssertionError("simulated VM-level failure");
        var release = new CountDownLatch(1);
        var lease = pool.lease(MIB);
        assertTrue(lease.tryAcquire(1, TimeUnit.SECONDS));
        var done = new LinkedBlockingQueue<ShardWorkPool.Result<Integer>>();
        pool.start(lease, () -> {
            release.await();
            throw crash;
        }, done::add);
        var errors = Collections.synchronizedList(new ArrayList<Throwable>());
        var dispatchers = new ArrayList<Thread>();
        for (int d = 0; d < 3; d++) {
            dispatchers.add(dispatcher(() -> {
                var waiting = pool.lease(MIB);
                try {
                    while (!waiting.tryAcquire(20, TimeUnit.MILLISECONDS)) { /* collect */ }
                } catch (AssertionError stopped) {
                    errors.add(stopped);
                }
            }));
        }
        Thread.sleep(100);
        release.countDown();
        for (Thread dispatcher : dispatchers) dispatcher.join(10_000);
        assertEquals(3, errors.size());
        errors.forEach(error -> assertSame(crash, error));
        assertTrue(pool.awaitIdle(10, TimeUnit.SECONDS));
        assertEquals(1, pool.availablePermits());
    }

    private interface Body {
        void run() throws Exception;
    }

    private static Thread dispatcher(Body body) {
        return dispatcher(body, new AtomicReference<>());
    }

    private static Thread dispatcher(Body body, AtomicReference<Throwable> failure) {
        Thread thread = new Thread(() -> {
            try {
                body.run();
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
        });
        thread.setDaemon(true);
        thread.start();
        return thread;
    }
}
