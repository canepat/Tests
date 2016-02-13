package it.blogspot.mechanical_sympathy;

import java.util.concurrent.TimeUnit;

public final class FalseSharing
{
    public final static int NUM_THREADS = 4; // change
    public final static long ITERATIONS = 500L * 1000L * 1000L;

    private static VolatileLong[] longs = new VolatileLong[NUM_THREADS];
    private static PaddedVolatileLong[] paddedLongs = new PaddedVolatileLong[NUM_THREADS];

    static
    {
        for (int i = 0; i < longs.length; i++)
        {
            longs[i] = new VolatileLong();
        }

        for (int i = 0; i < paddedLongs.length; i++)
        {
            paddedLongs[i] = new PaddedVolatileLong();
        }
    }

    public static void main(final String[] args) throws Exception
    {
        System.out.println("FalseSharing:");

        final long start1 = System.nanoTime();
        runVolatileLongTest();
        final long duration1 = System.nanoTime() - start1;
        System.out.println("w/o padding = " + TimeUnit.NANOSECONDS.toMillis(duration1) + " ms [" + duration1 + " ns]");

        final long start2 = System.nanoTime();
        runPaddedVolatileLongTest();
        final long duration2 = System.nanoTime() - start2;
        System.out.println("w/ padding  = " + TimeUnit.NANOSECONDS.toMillis(duration2) + " ms [" + duration2 + " ns]");
    }

    private static void runVolatileLongTest() throws InterruptedException
    {
        Thread[] threads = new Thread[NUM_THREADS];

        for (int i = 0; i < threads.length; i++)
        {
            threads[i] = new Thread(new VolatileLongTest(i));
        }

        for (Thread t : threads)
        {
            t.start();
        }

        for (Thread t : threads)
        {
            t.join();
        }
    }


    private static void runPaddedVolatileLongTest() throws InterruptedException
    {
        Thread[] threads = new Thread[NUM_THREADS];

        for (int i = 0; i < threads.length; i++)
        {
            threads[i] = new Thread(new PaddedVolatileLongTest(i));
        }

        for (Thread t : threads)
        {
            t.start();
        }

        for (Thread t : threads)
        {
            t.join();
        }
    }

    public final static class VolatileLongTest implements Runnable
    {
        private final int arrayIndex;

        public VolatileLongTest(final int arrayIndex)
        {
            this.arrayIndex = arrayIndex;
        }

        public void run()
        {
            long i = ITERATIONS + 1;
            while (0 != --i)
            {
                longs[arrayIndex].value = i;
            }
        }
    }

    public final static class PaddedVolatileLongTest implements Runnable
    {
        private final int arrayIndex;

        public PaddedVolatileLongTest(final int arrayIndex)
        {
            this.arrayIndex = arrayIndex;
        }

        public void run()
        {
            long i = ITERATIONS + 1;
            while (0 != --i)
            {
                paddedLongs[arrayIndex].value = i;
            }
        }
    }

    public final static class VolatileLong
    {
        public volatile long value = 0L;
    }

    public final static class PaddedVolatileLong
    {
        public volatile long value = 0L;
        public long p1, p2, p3, p4, p5, p6;
    }
}

