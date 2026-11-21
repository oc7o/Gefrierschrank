package de.sloow.gefrierschrank.syntheticsource;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lucene's {@code TermsEnum} and {@code BaseTermsEnum} initialize each other, so two shard threads
 * whose first contact is one and the other (a synthetic leaf's empty postings iterator, a coverage
 * check's sorted-set terms dictionary) deadlocked forever in the isolated loader. The bootstrap,
 * which every isolated entry point runs before it opens a reader, initializes the pair up front.
 */
class IsolatedClassInitializationTest {

    private static final String TERMS_ENUM = "org.apache.lucene.index.TermsEnum";
    private static final String BASE_TERMS_ENUM = "org.apache.lucene.index.BaseTermsEnum";
    private static final String TERMS_DICT = "org.apache.lucene.codecs.lucene90.Lucene90DocValuesProducer$TermsDict";

    @Test
    void bootstrapInitializesTheTermsEnumPair() throws Exception {
        try (IsolatedEsClassLoader loader = IsolatedEsClassLoader.create()) {
            Class<?> termsEnum = Class.forName(TERMS_ENUM, false, loader);
            Class<?> baseTermsEnum = Class.forName(BASE_TERMS_ENUM, false, loader);
            assertTrue(shouldBeInitialized(termsEnum), "a fresh loader has not initialized TermsEnum yet");
            IsolatedNativeAccessTest.call(loader, "bootstrap");
            assertFalse(shouldBeInitialized(termsEnum));
            assertFalse(shouldBeInitialized(baseTermsEnum));
        }
    }

    /**
     * The race itself, after the bootstrap. The window is narrow in a warm JVM: without the fix this
     * hangs only now and then here, but in about one round in eight of a fresh process.
     */
    @Test
    void concurrentFirstContactsAfterBootstrapNeverDeadlock() throws Exception {
        String[] firstContacts = {TERMS_ENUM, TERMS_DICT};
        for (int round = 0; round < 20; round++) {
            try (IsolatedEsClassLoader loader = IsolatedEsClassLoader.create()) {
                IsolatedNativeAccessTest.call(loader, "bootstrap");
                for (String name : firstContacts) Class.forName(name, false, loader);
                CyclicBarrier barrier = new CyclicBarrier(firstContacts.length);
                AtomicReference<Throwable> failure = new AtomicReference<>();
                long termsEnumDelayNanos = ThreadLocalRandom.current().nextLong(3_000_000);
                Thread[] threads = new Thread[firstContacts.length];
                for (int i = 0; i < threads.length; i++) {
                    String name = firstContacts[i];
                    long delay = i == 0 ? termsEnumDelayNanos : 0;
                    threads[i] = new Thread(() -> {
                        try {
                            barrier.await();
                            long until = System.nanoTime() + delay;
                            while (System.nanoTime() < until) Thread.onSpinWait();
                            Class.forName(name, true, loader);
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                    });
                    // A deadlocked pair can never be stopped; daemon threads at least let the JVM exit.
                    threads[i].setDaemon(true);
                    threads[i].start();
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                for (Thread thread : threads)
                    thread.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
                for (Thread thread : threads)
                    assertFalse(thread.isAlive(), "round " + round + ": class initialization deadlocked");
                assertNull(failure.get(), "round " + round);
            }
        }
    }

    /** Asks without initializing; needs {@code --add-exports java.base/jdk.internal.misc} (see the pom). */
    private static boolean shouldBeInitialized(Class<?> type) throws Exception {
        Class<?> unsafeClass = Class.forName("jdk.internal.misc.Unsafe");
        Object unsafe = unsafeClass.getMethod("getUnsafe").invoke(null);
        Method shouldBeInitialized = unsafeClass.getMethod("shouldBeInitialized", Class.class);
        return (boolean) shouldBeInitialized.invoke(unsafe, type);
    }
}
