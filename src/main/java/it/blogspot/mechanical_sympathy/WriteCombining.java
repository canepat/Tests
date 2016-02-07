package it.blogspot.mechanical_sympathy;

public final class WriteCombining
{
    private static final int ITERATIONS = Integer.MAX_VALUE;
    private static final int ITEMS = 1 << 24;
    private static final int MASK = ITEMS - 1;

    private static final byte[] ARRAY_A = new byte[ITEMS];
    private static final byte[] ARRAY_B = new byte[ITEMS];
    private static final byte[] ARRAY_C = new byte[ITEMS];
    private static final byte[] ARRAY_D = new byte[ITEMS];
    private static final byte[] ARRAY_E = new byte[ITEMS];
    private static final byte[] ARRAY_F = new byte[ITEMS];
    private static final byte[] ARRAY_G = new byte[ITEMS];
    private static final byte[] ARRAY_H = new byte[ITEMS];
    private static final byte[] ARRAY_I = new byte[ITEMS];
    private static final byte[] ARRAY_J = new byte[ITEMS];
    private static final byte[] ARRAY_K = new byte[ITEMS];
    private static final byte[] ARRAY_L = new byte[ITEMS];

    public static void main(final String[] args)
    {
        System.out.println("WriteCombining:");
        for (int i = 1; i <= 3; i++)
        {
            System.out.println(i + " SingleLoop duration (ns) = " + runCaseOne());
            System.out.println(i + " SplitLoop  duration (ns) = " + runCaseTwo());
        }

        int sum = 1+2+3+4+5+6+7+8+9+10+11+12;
        int result = ARRAY_A[1] + ARRAY_B[2] + ARRAY_C[3] + ARRAY_D[4] + ARRAY_E[5] + ARRAY_F[6] +
            ARRAY_G[7] + ARRAY_H[8] + ARRAY_I[9] + ARRAY_J[10] + ARRAY_K[11] + ARRAY_L[12];

        if (sum != result)
        {
            throw new IllegalStateException();
        }
    }

    public static long runCaseOne()
    {
        long start = System.nanoTime();

        int i = ITERATIONS;
        while (--i != 0)
        {
            int slot = i & MASK;
            byte b = (byte)i;
            ARRAY_A[slot] = b;
            ARRAY_B[slot] = b;
            ARRAY_C[slot] = b;
            ARRAY_D[slot] = b;
            ARRAY_E[slot] = b;
            ARRAY_F[slot] = b;
            ARRAY_G[slot] = b;
            ARRAY_H[slot] = b;
            ARRAY_I[slot] = b;
            ARRAY_J[slot] = b;
            ARRAY_K[slot] = b;
            ARRAY_L[slot] = b;
        }

        return System.nanoTime() - start;
    }

    public static long runCaseTwo()
    {
        long start = System.nanoTime();

        int i = ITERATIONS;
        while (--i != 0)
        {
            int slot = i & MASK;
            byte b = (byte)i;
            ARRAY_A[slot] = b;
            ARRAY_B[slot] = b;
            ARRAY_C[slot] = b;
            ARRAY_D[slot] = b;
            ARRAY_E[slot] = b;
            ARRAY_F[slot] = b;
        }

        i = ITERATIONS;
        while (--i != 0)
        {
            int slot = i & MASK;
            byte b = (byte)i;
            ARRAY_G[slot] = b;
            ARRAY_H[slot] = b;
            ARRAY_I[slot] = b;
            ARRAY_J[slot] = b;
            ARRAY_K[slot] = b;
            ARRAY_L[slot] = b;
        }

        return System.nanoTime() - start;
    }
}
