package it.blogspot.mechanical_sympathy;

public final class InterThreadLatency implements Runnable
{
    public static final long ITERATIONS = 500L * 1000L * 1000L;

    public static volatile long s1;
    public static volatile long s2;

    public static void main(final String[] args)
    {
        final Thread t = new Thread(new InterThreadLatency());
        t.setDaemon(true);
        t.start();

        final long start = System.nanoTime();

        long value = s1;
        while (s1 < ITERATIONS)
        {
            while (s2 != value)
            {
                // busy spin
            }
            value = ++s1;
        }

        final long duration = System.nanoTime() - start;

        System.out.println("InterThreadLatency:");
        System.out.println("duration = " + duration);
        System.out.println("ns per op = " + duration / (ITERATIONS * 2));
        System.out.println("op/sec = " + (ITERATIONS * 2L * 1000L * 1000L * 1000L) / duration);
        System.out.println("s1 = " + s1 + ", s2 = " + s2);
    }

    public void run()
    {
        long value = s2;
        while (true)
        {
            while (value == s1)
            {
                // busy spin
            }
            value = ++s2;
        }
    }
}