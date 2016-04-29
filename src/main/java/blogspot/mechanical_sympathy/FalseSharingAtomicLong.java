package blogspot.mechanical_sympathy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class FalseSharingAtomicLong
{
    public final static int NUM_THREADS = 4; // change
    public final static long ITERATIONS = 500L * 1000L * 1000L;

    private static AtomicLong[] longs = new AtomicLong[NUM_THREADS];
    private static PaddedAtomicLong[] paddedLongs = new PaddedAtomicLong[NUM_THREADS];

    private static Thread[] threads = new Thread[NUM_THREADS];
    private static Thread[] paddedThreads = new Thread[NUM_THREADS];

    static
    {
        for (int i = 0; i < longs.length; i++)
        {
            longs[i] = new AtomicLong();
        }
        for (int i = 0; i < paddedLongs.length; i++)
        {
            paddedLongs[i] = new PaddedAtomicLong();
        }

        for (int i = 0; i < threads.length; i++)
        {
            threads[i] = new Thread(new AtomicLongTest(i));
        }
        for (int i = 0; i < paddedThreads.length; i++)
        {
            paddedThreads[i] = new Thread(new PaddedAtomicLongTest(i));
        }
    }

    public static void main(final String[] args) throws Exception
    {
        System.out.println("FalseSharingAtomicLong:");

        final long start1 = System.nanoTime();
        runAtomicLongTest();
        final long duration1 = System.nanoTime() - start1;
        System.out.println("w/o padding = " + TimeUnit.NANOSECONDS.toMillis(duration1) + " ms [" + duration1 + " ns]");

        final long start2 = System.nanoTime();
        runPaddedAtomicLongTest();
        final long duration2 = System.nanoTime() - start2;
        System.out.println("w/ padding  = " + TimeUnit.NANOSECONDS.toMillis(duration2) + " ms [" + duration2 + " ns]");
    }

    private static void runAtomicLongTest() throws InterruptedException
    {
        for (Thread t : threads)
        {
            t.start();
        }

        for (Thread t : threads)
        {
            t.join();
        }
    }

    private static void runPaddedAtomicLongTest() throws InterruptedException
    {
        for (Thread t : paddedThreads)
        {
            t.start();
        }

        for (Thread t : paddedThreads)
        {
            t.join();
        }
    }

    public static final class AtomicLongTest implements Runnable
    {
        private final int arrayIndex;

        public AtomicLongTest(final int arrayIndex)
        {
            this.arrayIndex = arrayIndex;
        }

        public void run()
        {
            long i = ITERATIONS + 1;
            while (0 != --i)
            {
                longs[arrayIndex].set(i);
            }
        }
    }

    public static final class PaddedAtomicLongTest implements Runnable
    {
        private final int arrayIndex;

        public PaddedAtomicLongTest(final int arrayIndex)
        {
            this.arrayIndex = arrayIndex;
        }

        public void run()
        {
            long i = ITERATIONS + 1;
            while (0 != --i)
            {
                paddedLongs[arrayIndex].set(i);
            }
        }
    }

    public static long sumPaddingToPreventOptimisation(final int index)
    {
        PaddedAtomicLong v = paddedLongs[index];
        return v.p1 + v.p2 + v.p3 + v.p4 + v.p5 + v.p6;
    }

    public static class PaddedAtomicLong extends AtomicLong
    {
        public volatile long p1, p2, p3, p4, p5, p6 = 7L;
    }
}
