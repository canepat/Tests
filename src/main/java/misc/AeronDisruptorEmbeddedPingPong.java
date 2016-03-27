package misc;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.HdrHistogram.Histogram;
import uk.co.real_logic.aeron.*;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.driver.ThreadingMode;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.samples.SampleConfiguration;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.concurrent.*;
import uk.co.real_logic.agrona.console.ContinueBarrier;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class AeronDisruptorEmbeddedPingPong
{
    private static final boolean useIPC = Boolean.parseBoolean(System.getProperty("useIPC", "true"));
    private static final int PING_STREAM_ID = useIPC ? SampleConfiguration.STREAM_ID : SampleConfiguration.PING_STREAM_ID;
    private static final int PONG_STREAM_ID = useIPC ? SampleConfiguration.STREAM_ID+1 : SampleConfiguration.PONG_STREAM_ID;
    private static final String PING_CHANNEL = useIPC ? CommonContext.IPC_CHANNEL : SampleConfiguration.PING_CHANNEL;
    private static final String PONG_CHANNEL = useIPC ? CommonContext.IPC_CHANNEL : SampleConfiguration.PONG_CHANNEL;
    private static final int NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final int WARMUP_NUMBER_OF_MESSAGES = SampleConfiguration.WARMUP_NUMBER_OF_MESSAGES;
    private static final int WARMUP_NUMBER_OF_ITERATIONS = SampleConfiguration.WARMUP_NUMBER_OF_ITERATIONS;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final int FRAME_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    private static final UnsafeBuffer ATOMIC_BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));
    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
    private static final CountDownLatch PONG_IMAGE_LATCH = new CountDownLatch(1);
    private static final BusySpinIdleStrategy PING_HANDLER_IDLE_STRATEGY = new BusySpinIdleStrategy();
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);

    public static void main(final String[] args) throws Exception
    {
        if (1 == args.length)
        {
            MediaDriver.loadPropertiesFile(args[0]);
        }

        final MediaDriver.Context ctx = new MediaDriver.Context()
            .warnIfDirectoriesExist(false)
            .threadingMode(ThreadingMode.DEDICATED)
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
            .availableImageHandler(AeronDisruptorEmbeddedPingPong::availablePongImageHandler);
        ctx.aeronDirectoryName(embeddedDirName);

        System.out.println("Publishing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
        System.out.println("Subscribing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);
        System.out.println("Message size of " + MESSAGE_LENGTH + " bytes");

        final FragmentAssembler dataHandler = new FragmentAssembler(AeronDisruptorEmbeddedPingPong::pongHandler);

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

            do
            {
                HISTOGRAM.reset();
                System.out.println("Pinging " + NUMBER_OF_MESSAGES + " messages");

                final long elapsedTime = sendPingAndReceivePong(dataHandler, pingPublisher, pongSubscriber, NUMBER_OF_MESSAGES);

                System.out.println(
                    String.format("%d ops, %d ns, %d ms, rate %.02g ops/s", NUMBER_OF_MESSAGES, elapsedTime, TimeUnit.NANOSECONDS.toMillis(elapsedTime),
                        ((double)NUMBER_OF_MESSAGES/(double)elapsedTime) * 1_000_000_000));

                System.out.println("Histogram of RTT latencies in microseconds");
                HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);
            }
            while (barrier.await());
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
                    final ExecutorService inputExecutor = Executors.newCachedThreadPool();
                    final ExecutorService outputExecutor = Executors.newCachedThreadPool();

                    EventHandler<OutputBufferEvent<Object>> outputHandler = (event, sequence, endOfBatch) -> {
                        final DirectBuffer buffer = event.getBuffer();

                        while (pongPublisher.offer(buffer, 0, buffer.byteBuffer().remaining()) < 0L)
                        {
                            PING_HANDLER_IDLE_STRATEGY.idle(0);
                        }

                        if (buffer.capacity() > OutputBufferEvent.DEFAULT_BUFFER_SIZE)
                        {
                            event.resetBuffer();
                        }
                    };
                    EventFactory<OutputBufferEvent<Object>> outputFactory = () -> {
                        ByteBuffer buffer = ByteBuffer.allocateDirect(OutputBufferEvent.DEFAULT_BUFFER_SIZE);
                        return new OutputBufferEvent<>(new Object(), buffer);
                    };
                    final Disruptor<OutputBufferEvent<Object>> outputDisruptor = new Disruptor<>(
                        outputFactory,
                        512*1024,
                        outputExecutor,
                        ProducerType.SINGLE,
                        new BusySpinWaitStrategy());
                    outputDisruptor.handleEventsWith(outputHandler);

                    EventHandler<InputBufferEvent<Object>> serviceHandler = (event, sequence, endOfBatch) -> {
                        final ByteBuffer buffer = event.getBuffer().byteBuffer();
                        final int size = buffer.remaining();

                        final RingBuffer<OutputBufferEvent<Object>> ringBuffer = outputDisruptor.getRingBuffer();

                        final long outputSequence = ringBuffer.next();
                        try
                        {
                            final OutputBufferEvent<Object> outputEvent = ringBuffer.get(outputSequence);

                            outputEvent.setEventTime(event.getEventTime());

                            final ByteBuffer outputBuffer = outputEvent.getBuffer().byteBuffer();
                            for (int i=0; i<size; i++)
                            {
                                outputBuffer.put(buffer.get());
                            }
                            outputBuffer.flip();
                        }
                        finally
                        {
                            ringBuffer.publish(outputSequence);
                        }
                    };

                    /*JournalStrategy journalStrategy = new PositionalWriteJournalStrategy(Paths.get("./"), 1024 * 1024 * 1024, 1);
                    SequenceReportingEventHandler<InputBufferEvent<?>> inputJournaller = new Journaller(journalStrategy, false);*/
                    EventFactory<InputBufferEvent<Object>> inputFactory = () -> new InputBufferEvent<>(new Object());
                    final Disruptor<InputBufferEvent<Object>> inputDisruptor = new Disruptor<>(
                        inputFactory,
                        512*1024,
                        inputExecutor,
                        ProducerType.SINGLE,
                        new BusySpinWaitStrategy());
                    //inputDisruptor.handleEventsWith(inputJournaller).then(serviceHandler);
                    inputDisruptor.handleEventsWith(serviceHandler);

                    outputDisruptor.start();
                    inputDisruptor.start();

                    final FragmentHandler inputHandler = (buffer, offset, length, header) -> {
                        final long requestTime = System.nanoTime();

                        final RingBuffer<InputBufferEvent<Object>> ringBuffer = inputDisruptor.getRingBuffer();

                        final long sequence = ringBuffer.next();
                        try
                        {
                            InputBufferEvent<Object> event = ringBuffer.get(sequence);
                            event.setEventTime(requestTime);

                            if (length > InputBufferEvent.DEFAULT_BUFFER_SIZE)
                            {
                                event.growBuffer(length);
                            }

                            ByteBuffer eventBuffer = event.getBuffer().byteBuffer();
                            eventBuffer.clear();
                            buffer.getBytes(offset, eventBuffer, length);
                            eventBuffer.flip();
                        }
                        finally
                        {
                            ringBuffer.publish(sequence);
                        }
                    };

                    final FragmentAssembler dataHandler = new FragmentAssembler(inputHandler);

                    while (RUNNING.get())
                    {
                        final int fragmentsRead = pingSubscriber.poll(dataHandler, FRAME_COUNT_LIMIT);
                        PING_HANDLER_IDLE_STRATEGY.idle(fragmentsRead);
                    }

                    System.out.println("Shutting down...");

                    inputDisruptor.shutdown();
                    outputDisruptor.shutdown();
                    inputExecutor.shutdown();
                    outputExecutor.shutdown();
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

        final long start = System.nanoTime();

        for (int i = 0; i < numMessages; i++)
        {
            do
            {
                ATOMIC_BUFFER.putLong(0, System.nanoTime());
            }
            while (pingPublisher.offer(ATOMIC_BUFFER, 0, 8) < 0L);

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
}
