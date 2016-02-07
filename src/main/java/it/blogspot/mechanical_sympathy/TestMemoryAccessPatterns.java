package it.blogspot.mechanical_sympathy;

public class TestMemoryAccessPatterns
{
    private static final int LONG_SIZE = 8;
    private static final int PAGE_SIZE = 2 * 1024 * 1024;
    private static final int ONE_GIG = 1024 * 1024 * 1024;
    private static final long TWO_GIG = 2L * ONE_GIG;

    private static final int ARRAY_SIZE = (int)(TWO_GIG / LONG_SIZE);
    private static final int WORDS_PER_PAGE = PAGE_SIZE / LONG_SIZE;

    private static final int ARRAY_MASK = ARRAY_SIZE - 1;
    private static final int PAGE_MASK = WORDS_PER_PAGE - 1;

    private static final int PRIME_INC = 514229;

    private static final long[] MEMORY = new long[ARRAY_SIZE];

    static
    {
        for (int i = 0; i < ARRAY_SIZE; i++)
        {
            MEMORY[i] = 777;
        }
    }

    public enum StrideType
    {
        LINEAR_WALK
        {
            public int next(final int pageOffset, final int wordOffset, final int pos)
            {
                return (pos + 1) & ARRAY_MASK;
            }
        },

        RANDOM_PAGE_WALK
        {
            public int next(final int pageOffset, final int wordOffset, final int pos)
            {
                return pageOffset + ((pos + PRIME_INC) & PAGE_MASK);
            }
        },

        RANDOM_HEAP_WALK
        {
            public int next(final int pageOffset, final int wordOffset, final int pos)
            {
                return (pos + PRIME_INC) & ARRAY_MASK;
            }
        };

        public abstract int next(int pageOffset, int wordOffset, int pos);
    }

    public static void main(final String[] args) throws Exception
    {
        System.out.println("TestMemoryAccessPatterns:");
        System.out.println("OS = " + System.getProperty("os.name") + " " +
            System.getProperty("os.arch") + " " + System.getProperty("os.version") + "\n");

        run(StrideType.LINEAR_WALK);
        run(StrideType.RANDOM_PAGE_WALK);
        run(StrideType.RANDOM_HEAP_WALK);
    }

    private static void run(final StrideType strideType)
    {
        for (int i = 0; i < 5; i++)
        {
            perfTest(i, strideType);
        }
        System.out.println("");
    }

    private static void perfTest(final int runNumber, final StrideType strideType)
    {
        final long start = System.nanoTime();

        int pos = -1;
        long result = 0;
        for (int pageOffset = 0; pageOffset < ARRAY_SIZE; pageOffset += WORDS_PER_PAGE)
        {
            for (int wordOffset = pageOffset, limit = pageOffset + WORDS_PER_PAGE;
                 wordOffset < limit;
                 wordOffset++)
            {
                pos = strideType.next(pageOffset, wordOffset, pos);
                result += MEMORY[pos];
            }
        }

        final long duration = System.nanoTime() - start;
        final double nsOp = duration / (double)ARRAY_SIZE;

        if (208574349312L != result)
        {
            throw new IllegalStateException();
        }

        System.out.format("%d - %.2fns %s\n", runNumber, nsOp, strideType);
    }
}
