/*
 * Copyright 2014 - 2015 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples;

import io.aeron.*;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.*;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class EmbeddedIpcMultiplePingPong
{
    private static final int PING_STREAM_ID = 10;
    private static final int PONG_STREAM_ID = 11;
    private static final String PING_CHANNEL = CommonContext.IPC_CHANNEL;
    private static final String PONG_CHANNEL = CommonContext.IPC_CHANNEL;
    private static final int NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;
    private static final int WARMUP_NUMBER_OF_MESSAGES = SampleConfiguration.WARMUP_NUMBER_OF_MESSAGES;
    private static final int WARMUP_NUMBER_OF_ITERATIONS = SampleConfiguration.WARMUP_NUMBER_OF_ITERATIONS;
    private static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final int FRAME_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final int NUMBER_OF_CONCURRENT_PINGS = SampleConfiguration.NUMBER_OF_CONCURRENT_PINGS;

    private static final UnsafeBuffer ATOMIC_BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));
    private static final Histogram HISTOGRAM = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);
    private static final CountDownLatch PONG_IMAGE_LATCH = new CountDownLatch(NUMBER_OF_CONCURRENT_PINGS);
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
            .dirsDeleteOnStart(true)
            .threadingMode(ThreadingMode.DEDICATED)
            .conductorIdleStrategy(new BackoffIdleStrategy(1, 1, 1, 1))
            .receiverIdleStrategy(new NoOpIdleStrategy())
            .senderIdleStrategy(new NoOpIdleStrategy());

        System.out.println("Launching Driver...");

        try (final MediaDriver ignored = MediaDriver.launch(ctx))
        {
            final Thread pongThread = startPong(ignored.aeronDirectoryName());
            pongThread.start();

            final Thread[] pingThreads = new Thread[NUMBER_OF_CONCURRENT_PINGS];
            for (int i = 0; i < NUMBER_OF_CONCURRENT_PINGS; i++)
            {
                pingThreads[i] = runPing(ignored.aeronDirectoryName(), i);
                pingThreads[i].start();
            }

            for (int i = 0; i < NUMBER_OF_CONCURRENT_PINGS; i++)
            {
                pingThreads[i].join();
            }

            RUNNING.set(false);
            pongThread.join();

            System.out.println("Shutdown Driver...");
        }
    }

    private static Thread runPing(final String embeddedDirName, int id)
    {
        return new Thread("PingThread-" + id)
        {
            public void run()
            {
                final String threadName = Thread.currentThread().getName();

                final Aeron.Context ctx = new Aeron.Context()
                    .availableImageHandler(EmbeddedIpcMultiplePingPong::availablePongImageHandler);
                ctx.aeronDirectoryName(embeddedDirName);

                System.out.println(threadName + " - Publishing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
                System.out.println(threadName + " - Subscribing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);
                System.out.println(threadName + " - Message size of " + MESSAGE_LENGTH + " bytes");

                final FragmentAssembler dataHandler = new FragmentAssembler(EmbeddedIpcMultiplePingPong::pongHandler);

                try (final Aeron aeron = Aeron.connect(ctx);
                     final Publication pingPublication = aeron.addPublication(PING_CHANNEL, PING_STREAM_ID);
                     final Subscription pongSubscription = aeron.addSubscription(PONG_CHANNEL, PONG_STREAM_ID))
                {
                    System.out.println(threadName + " - Waiting for new image from Pong...");

                    PONG_IMAGE_LATCH.await();

                    System.out.println(
                        threadName + " - Warming up... " + WARMUP_NUMBER_OF_ITERATIONS + " iterations of " + WARMUP_NUMBER_OF_MESSAGES + " messages");

                    for (int i = 0; i < WARMUP_NUMBER_OF_ITERATIONS; i++)
                    {
                        sendPingAndReceivePong(dataHandler, pingPublication, pongSubscription, WARMUP_NUMBER_OF_MESSAGES);
                    }

                    HISTOGRAM.reset();
                    System.out.println(threadName + " - Pinging " + NUMBER_OF_MESSAGES + " messages...");

                    final long elapsedTime = sendPingAndReceivePong(dataHandler, pingPublication, pongSubscription,
                        NUMBER_OF_MESSAGES);

                    System.out.println(
                        String.format(threadName + " - %d ops, %d ns, %d ms, rate %.02g ops/s", NUMBER_OF_MESSAGES, elapsedTime,
                            TimeUnit.NANOSECONDS.toMillis(elapsedTime),
                            ((double)NUMBER_OF_MESSAGES / (double)elapsedTime) * 1_000_000_000));

                    System.out.println(threadName + " - Histogram of RTT latencies in microseconds");
                    //HISTOGRAM.outputPercentileDistribution(System.out, 1000.0);

                    System.out.println(threadName + " - Shutting down...");
                }
                catch (InterruptedException ie)
                {
                    LangUtil.rethrowUnchecked(ie);
                }
            }
        };
    }

    private static Thread startPong(final String embeddedDirName)
    {
        return new Thread("PongThread")
        {
            public void run()
            {
                final String threadName = Thread.currentThread().getName();

                System.out.println(threadName + " - Subscribing Ping at " + PING_CHANNEL + " on stream Id " + PING_STREAM_ID);
                System.out.println(threadName + " - Publishing Pong at " + PONG_CHANNEL + " on stream Id " + PONG_STREAM_ID);

                final Aeron.Context ctx = new Aeron.Context();
                ctx.aeronDirectoryName(embeddedDirName);

                try (final Aeron aeron = Aeron.connect(ctx);
                     final Publication pongPublication = aeron.addPublication(PONG_CHANNEL, PONG_STREAM_ID);
                     final Subscription pingSubscription = aeron.addSubscription(PING_CHANNEL, PING_STREAM_ID))
                {
                    final FragmentAssembler dataHandler = new FragmentAssembler(
                        (buffer, offset, length, header) -> pingHandler(pongPublication, buffer, offset, length));

                    while (RUNNING.get())
                    {
                        final int fragmentsRead = pingSubscription.poll(dataHandler, FRAME_COUNT_LIMIT);
                        PING_HANDLER_IDLE_STRATEGY.idle(fragmentsRead);
                    }

                    System.out.println(threadName + " - Shutting down...");
                }
            }
        };
    }

    private static long sendPingAndReceivePong(
        final FragmentHandler fragmentHandler,
        final Publication pingPublication,
        final Subscription pongSubscription,
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
            while (pingPublication.offer(ATOMIC_BUFFER, 0, MESSAGE_LENGTH) < 0L);

            while (pongSubscription.poll(fragmentHandler, FRAGMENT_COUNT_LIMIT) <= 0)
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

        //HISTOGRAM.recordValue(rttNs);
    }

    private static void availablePongImageHandler(final Image image)
    {
        if (PONG_STREAM_ID == image.subscription().streamId() && PONG_CHANNEL.equals(image.subscription().channel()))
        {
            PONG_IMAGE_LATCH.countDown();
        }
    }

    public static void pingHandler(
        final Publication pongPublication, final DirectBuffer buffer, final int offset, final int length)
    {
        while (pongPublication.offer(buffer, offset, length) < 0L)
        {
            PING_HANDLER_IDLE_STRATEGY.idle(0);
        }
    }
}
