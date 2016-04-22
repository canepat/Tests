package misc.aeron;

import org.HdrHistogram.Histogram;
import uk.co.real_logic.aeron.*;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.samples.SampleConfiguration;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.*;
import uk.co.real_logic.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import uk.co.real_logic.agrona.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.agrona.console.ContinueBarrier;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static uk.co.real_logic.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class AeronEmbeddedIpcPingPong
{
    private static final boolean useIPC = Boolean.parseBoolean(System.getProperty("useIPC", "true"));
    private static final int PING_STREAM_ID = useIPC ? SampleConfiguration.STREAM_ID : SampleConfiguration.PING_STREAM_ID;
    private static final int PONG_STREAM_ID = useIPC ? SampleConfiguration.STREAM_ID+1 : SampleConfiguration.PONG_STREAM_ID;
    private static final String PING_CHANNEL = useIPC ? CommonContext.IPC_CHANNEL : SampleConfiguration.PING_CHANNEL;
    private static final String PONG_CHANNEL = useIPC ? CommonContext.IPC_CHANNEL : SampleConfiguration.PONG_CHANNEL;
    private static final int NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final int NUMBER_OF_ITERATIONS = SampleConfiguration.NUMBER_OF_ITERATIONS;
    private static final int WARMUP_NUMBER_OF_MESSAGES = SampleConfiguration.WARMUP_NUMBER_OF_MESSAGES;
    private static final int WARMUP_NUMBER_OF_ITERATIONS = SampleConfiguration.WARMUP_NUMBER_OF_ITERATIONS;

    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final int FRAME_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
    private static final CountDownLatch PONG_IMAGE_LATCH = new CountDownLatch(1);
    private static final IdleStrategy READ_IDLE_STRATEGY = new BusySpinIdleStrategy();
    private static final IdleStrategy WRITE_IDLE_STRATEGY = new BusySpinIdleStrategy();
    private static final IdleStrategy OFFER_IDLE_STRATEGY = new BusySpinIdleStrategy();
    private static final IdleStrategy POLL_IDLE_STRATEGY = new BusySpinIdleStrategy();
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    private static final int MSG_TYPE_ID = 7;

    public static void main(final String[] args) throws Exception
    {
        if (1 == args.length)
        {
            MediaDriver.loadPropertiesFile(args[0]);
        }

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .warnIfDirectoriesExist(false)
            .threadingMode(ThreadingMode.SHARED)
            .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
            .receiverIdleStrategy(new NoOpIdleStrategy())
            .senderIdleStrategy(new NoOpIdleStrategy());

        try (final MediaDriver ignored = MediaDriver.launch(ctx))
        {
            final Thread pongThread = createPong(ignored.aeronDirectoryName());
            pongThread.start();

            runPing(ignored.aeronDirectoryName());
            RUNNING.set(false);
            pongThread.join();

            System.out.println("Shutdown Driver...");
        }
    }

    private static void runPing(final String embeddedDirName) throws Exception
    {
        final Aeron.Context ctx = new Aeron.Context()
            .availableImageHandler(AeronEmbeddedIpcPingPong::availablePongImageHandler);
        ctx.aeronDirectoryName(embeddedDirName);

        System.out.println("Publishing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
        System.out.println("Subscribing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);
        System.out.println("Message size of " + MESSAGE_LENGTH + " bytes");

        final FragmentAssembler dataHandler = new FragmentAssembler(AeronEmbeddedIpcPingPong::pongHandler);

        try (final Aeron aeron = Aeron.connect(ctx);
             final Publication pingPublisher = aeron.addPublication(PING_CHANNEL, PING_STREAM_ID);
             final Subscription pongSubscriber = aeron.addSubscription(PONG_CHANNEL, PONG_STREAM_ID))
        {
            System.out.println("Waiting for new image from Pong...");

            PONG_IMAGE_LATCH.await();

            System.out.println(
                "Warming up... " + WARMUP_NUMBER_OF_ITERATIONS + " iterations of " + WARMUP_NUMBER_OF_MESSAGES + " messages");

            for (int i = 0; i < WARMUP_NUMBER_OF_ITERATIONS; i++)
            {
                sendPingAndReceivePong(dataHandler, pingPublisher, pongSubscriber, WARMUP_NUMBER_OF_MESSAGES);
            }

            final ContinueBarrier barrier = new ContinueBarrier("Execute again?");

            int iteration = 0;
            do
            {
                iteration++;

                HISTOGRAM.reset();
                System.out.println("Pinging " + NUMBER_OF_MESSAGES + " messages");

                final long elapsedTime = sendPingAndReceivePong(dataHandler, pingPublisher, pongSubscriber, NUMBER_OF_MESSAGES);

                System.out.println(
                    String.format("Iteration %d: %d ops, %d ns, %d ms, rate %.02g ops/s",
                        iteration,
                        NUMBER_OF_MESSAGES,
                        elapsedTime,
                        TimeUnit.NANOSECONDS.toMillis(elapsedTime),
                        ((double)NUMBER_OF_MESSAGES/(double)elapsedTime) * 1_000_000_000));

                if (NUMBER_OF_ITERATIONS <= 0)
                {
                    System.out.println("Histogram of RTT latencies in microseconds");
                    HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);
                }
            }
            while ((NUMBER_OF_ITERATIONS > 0 && iteration < NUMBER_OF_ITERATIONS) || barrier.await());
        }
    }

    private static Thread createPong(final String embeddedDirName)
    {
        return new Thread()
        {
            @SuppressWarnings("unchecked")
            public void run()
            {
                System.out.println("Subscribing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
                System.out.println("Publishing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);

                final Aeron.Context ctx = new Aeron.Context();
                ctx.aeronDirectoryName(embeddedDirName);

                try (final Aeron aeron = Aeron.connect(ctx);
                     final Publication pongPublisher = aeron.addPublication(PONG_CHANNEL, PONG_STREAM_ID);
                     final Subscription pingSubscriber = aeron.addSubscription(PING_CHANNEL, PING_STREAM_ID))
                {
                    final ByteBuffer outputBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH);
                    final RingBuffer outputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(outputBuffer));

                    final MessageHandler outputMsgHandler = new OutputMessageHandler(pongPublisher);
                    new OutputMessageProcessor(outputRingBuffer, outputMsgHandler).start();

                    final ByteBuffer serviceBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH);
                    final RingBuffer serviceRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(serviceBuffer));

                    final MessageHandler serviceHandler = new ServiceHandler(outputRingBuffer);
                    new InputMessageProcessor(serviceRingBuffer, serviceHandler).start();

                    final ByteBuffer inputBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH);
                    final RingBuffer inputRingBuffer = new OneToOneRingBuffer(new UnsafeBuffer(inputBuffer));

                    final MessageHandler journalHandler = new JournalHandler(serviceRingBuffer);
                    new JournalProcessor(inputRingBuffer, journalHandler).start();

                    final FragmentHandler inputHandler = new FragmentMessageHandler(inputRingBuffer);
                    final FragmentAssembler dataHandler = new FragmentAssembler(inputHandler);

                    while (RUNNING.get())
                    {
                        final int fragmentsRead = pingSubscriber.poll(dataHandler, FRAME_COUNT_LIMIT);
                        POLL_IDLE_STRATEGY.idle(fragmentsRead);
                    }

                    System.out.println("Shutting down...");
                }
                catch (Exception ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
            }
        };
    }

    private static long sendPingAndReceivePong(
        final FragmentHandler fragmentHandler,
        final Publication pingPublisher,
        final Subscription pongSubscriber,
        final int numMessages)
    {
        final IdleStrategy idleStrategy = new BusySpinIdleStrategy();
        final UnsafeBuffer messageBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));

        final long start = System.nanoTime();

        for (int i = 0; i < numMessages; i++)
        {
            do
            {
                messageBuffer.putLong(0, System.nanoTime());
            }
            while (pingPublisher.offer(messageBuffer, 0, 8) < 0L);

            while (pongSubscriber.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT) <= 0)
            {
                idleStrategy.idle(0);
            }
        }

        final long end = System.nanoTime();

        return (end - start);
    }

    private static void pongHandler(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final long pingTimestamp = buffer.getLong(offset);
        final long rttNs = System.nanoTime() - pingTimestamp;

        HISTOGRAM.recordValue(rttNs);
    }

    private static void availablePongImageHandler(final Image image)
    {
        if (PONG_STREAM_ID == image.subscription().streamId() && PONG_CHANNEL.equals(image.subscription().channel()))
        {
            PONG_IMAGE_LATCH.countDown();
        }
    }

    private static class OutputMessageHandler implements MessageHandler
    {
        private final Publication pongPublisher;

        OutputMessageHandler(final Publication pongPublisher)
        {
            this.pongPublisher = pongPublisher;
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            while (pongPublisher.offer(buffer, index, length) < 0L)
            {
                OFFER_IDLE_STRATEGY.idle(0);
            }
        }
    }

    private static class OutputMessageProcessor extends Thread
    {
        private final RingBuffer outputRingBuffer;
        private final MessageHandler outputMsgHandler;

        OutputMessageProcessor(final RingBuffer outputRingBuffer, final MessageHandler outputMsgHandler)
        {
            this.outputRingBuffer = outputRingBuffer;
            this.outputMsgHandler = outputMsgHandler;
        }

        @Override
        public void run()
        {
            while (RUNNING.get())
            {
                final int readCount = outputRingBuffer.read(outputMsgHandler);
                if (0 == readCount)
                {
                    READ_IDLE_STRATEGY.idle(0);
                }
            }
        }
    }

    private static class ServiceHandler implements MessageHandler
    {
        private final RingBuffer outputRingBuffer;

        ServiceHandler(final RingBuffer outputRingBuffer)
        {
            this.outputRingBuffer = outputRingBuffer;
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            while (!outputRingBuffer.write(MSG_TYPE_ID, buffer, index, length))
            {
                WRITE_IDLE_STRATEGY.idle(0);
            }
        }
    }

    private static class InputMessageProcessor extends Thread
    {
        private final RingBuffer serviceRingBuffer;
        private final MessageHandler serviceHandler;

        InputMessageProcessor(final RingBuffer serviceRingBuffer, final MessageHandler serviceHandler)
        {
            this.serviceRingBuffer = serviceRingBuffer;
            this.serviceHandler = serviceHandler;
        }

        @Override
        public void run()
        {
            while (RUNNING.get())
            {
                final int readCount = serviceRingBuffer.read(serviceHandler);
                if (0 == readCount)
                {
                    READ_IDLE_STRATEGY.idle(0);
                }
            }
        }
    }

    private static class JournalHandler implements MessageHandler
    {
        private final RingBuffer serviceRingBuffer;

        JournalHandler(final RingBuffer serviceRingBuffer)
        {
            this.serviceRingBuffer = serviceRingBuffer;
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            // TODO: journal message

            while (!serviceRingBuffer.write(MSG_TYPE_ID, buffer, index, length))
            {
                WRITE_IDLE_STRATEGY.idle(0);
            }
        }
    }

    private static class JournalProcessor extends Thread
    {
        private final RingBuffer inputRingBuffer;
        private final MessageHandler journalHandler;

        JournalProcessor(final RingBuffer inputRingBuffer, final MessageHandler journalHandler)
        {
            this.inputRingBuffer = inputRingBuffer;
            this.journalHandler = journalHandler;
        }

        @Override
        public void run()
        {
            while (RUNNING.get())
            {
                final int readCount = inputRingBuffer.read(journalHandler);
                if (0 == readCount)
                {
                    READ_IDLE_STRATEGY.idle(0);
                }
            }
        }
    }

    private static class FragmentMessageHandler implements FragmentHandler
    {
        private final RingBuffer inputRingBuffer;

        FragmentMessageHandler(final RingBuffer inputRingBuffer)
        {
            this.inputRingBuffer = inputRingBuffer;
        }

        @Override
        public void onFragment(DirectBuffer buffer, int offset, int length, Header header)
        {
            while (!inputRingBuffer.write(MSG_TYPE_ID, buffer, offset, length))
            {
                WRITE_IDLE_STRATEGY.idle(0);
            }
        }
    }
}
