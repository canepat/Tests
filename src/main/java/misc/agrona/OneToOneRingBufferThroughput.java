/*
 * Copyright 2015 Real Logic Ltd.
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
package misc.agrona;

import io.aeron.samples.SampleConfiguration;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.*;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.agrona.UnsafeAccess.UNSAFE;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class OneToOneRingBufferThroughput
{
    public static final int BURST_SIZE = 1_000_000;
    public static final int MESSAGE_LENGTH = SampleConfiguration.MESSAGE_LENGTH;

    private static final int MSG_TYPE_ID = 7;

    public static void main(final String[] args) throws Exception
    {
        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        final ByteBuffer buffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH);
        final RingBuffer ringBuffer = new OneToOneRingBuffer(new UnsafeBuffer(buffer));

        final Subscriber subscriber = new Subscriber(running, ringBuffer);
        final Thread subscriberThread = new Thread(subscriber);
        subscriberThread.setName("subscriber");
        final Thread publisherThread = new Thread(new Publisher(running, ringBuffer));
        publisherThread.setName("publisher");
        final Thread rateReporterThread = new Thread(new RateReporter(running, subscriber));
        rateReporterThread.setName("rate-reporter");

        rateReporterThread.start();
        subscriberThread.start();
        publisherThread.start();

        subscriberThread.join();
        publisherThread.join();
        rateReporterThread.join();
    }

    public static final class RateReporter implements Runnable
    {
        private final AtomicBoolean running;
        private final Subscriber subscriber;

        public RateReporter(final AtomicBoolean running, final Subscriber subscriber)
        {
            this.running = running;
            this.subscriber = subscriber;
        }

        public void run()
        {
            long lastTimeStamp = System.currentTimeMillis();
            long lastTotalBytes = subscriber.totalBytes();

            while (running.get())
            {
                LockSupport.parkNanos(1_000_000_000);

                final long newTimeStamp = System.currentTimeMillis();
                final long newTotalBytes = subscriber.totalBytes();

                final long duration = newTimeStamp - lastTimeStamp;
                final long bytesTransferred = newTotalBytes - lastTotalBytes;

                System.out.format(
                    "Duration %dms - %,d messages - %,d bytes\n",
                    duration, bytesTransferred / MESSAGE_LENGTH, bytesTransferred);

                lastTimeStamp = newTimeStamp;
                lastTotalBytes = newTotalBytes;
            }
        }
    }

    public static final class Publisher implements Runnable
    {
        private final AtomicBoolean running;
        private final RingBuffer ringBuffer;

        public Publisher(final AtomicBoolean running, final RingBuffer ringBuffer)
        {
            this.running = running;
            this.ringBuffer = ringBuffer;
        }

        public void run()
        {
            final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MESSAGE_LENGTH));
            long backPressureCount = 0;
            long totalMessageCount = 0;

            outputResults:
            while (running.get())
            {
                for (int i = 0; i < BURST_SIZE; i++)
                {
                    while (!ringBuffer.write(MSG_TYPE_ID, buffer, 0, MESSAGE_LENGTH))
                    {
                        ++backPressureCount;
                        if (!running.get())
                        {
                            break outputResults;
                        }
                    }

                    ++totalMessageCount;
                }
            }

            final double backPressureRatio = backPressureCount / (double)totalMessageCount;
            System.out.format("Publisher back pressure ratio: %f\n", backPressureRatio);
        }
    }

    public static final class Subscriber implements Runnable, MessageHandler
    {
        private static final long TOTAL_BYTES_OFFSET;
        static
        {
            try
            {
                TOTAL_BYTES_OFFSET = UNSAFE.objectFieldOffset(Subscriber.class.getDeclaredField("totalBytes"));
            }
            catch (final Exception ex)
            {
                throw new RuntimeException(ex);
            }
        }

        private final AtomicBoolean running;
        private final RingBuffer ringBuffer;

        private volatile long totalBytes = 0;

        public Subscriber(final AtomicBoolean running, final RingBuffer ringBuffer)
        {
            this.running = running;
            this.ringBuffer = ringBuffer;
        }

        public long totalBytes()
        {
            return totalBytes;
        }

        public void run()
        {
            final IdleStrategy idleStrategy = new YieldingIdleStrategy();

            long failedReads = 0;
            long successfulReads = 0;

            while (running.get())
            {
                final int readCount = ringBuffer.read(this);
                if (0 == readCount)
                {
                    ++failedReads;
                    idleStrategy.idle(0);
                }
                else
                {
                    ++successfulReads;
                }
            }

            final double failureRatio = failedReads / (double)(successfulReads + failedReads);
            System.out.format("Subscriber read failure ratio: %f\n", failureRatio);
        }

        @Override
        public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            UNSAFE.putOrderedLong(this, TOTAL_BYTES_OFFSET, totalBytes + length);
        }
    }
}
