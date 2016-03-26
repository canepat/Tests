package misc.disruptor;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.HdrHistogram.Histogram;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TwoDisruptorsTest
{
    private static class ValueEvent<T>
    {
        private T t;
        private long requestTime;
        
        public T get()
        {
            return t;
        }
        
        public void set(final T t)
        {
            this.t = t;
        }

        public long getRequestTime()
        {
            return requestTime;
        }

        public void setRequestTime(long requestTime)
        {
            this.requestTime = requestTime;
        }

        public static <T> EventFactory<ValueEvent<T>> factory()
        {
            return ValueEvent::new;
        }
    }

    private static class ValuePublisher<T>
    {
        private final RingBuffer<ValueEvent<T>> ringBuffer;

        public ValuePublisher(final RingBuffer<ValueEvent<T>> ringBuffer)
        {
            this.ringBuffer = ringBuffer;
        }

        public void publish(final T t)
        {
            final long sequence = ringBuffer.next();
            try
            {
                final ValueEvent<T> event = ringBuffer.get(sequence);
                event.set(t);
                event.setRequestTime(System.nanoTime());
            }
            finally
            {
                ringBuffer.publish(sequence);
            }
        }
    }

    private static class PrintEventHandler<T> implements EventHandler<ValueEvent<T>>
    {
        @Override
        public void onEvent(final ValueEvent<T> event, final long sequence, final boolean endOfBatch) throws Exception
        {
        }
    }

    private static class TraceEventHandler<T> implements EventHandler<ValueEvent<T>>
    {
        private final CountDownLatch latch;
        private final Histogram histogram;

        public TraceEventHandler(final CountDownLatch latch, final Histogram histogram)
        {
            this.latch = latch;
            this.histogram = histogram;
        }

        @Override
        public void onEvent(final ValueEvent<T> event, final long sequence, final boolean endOfBatch) throws Exception
        {
            final long requestTime = event.getRequestTime();

            histogram.recordValue(System.nanoTime() - requestTime);

            latch.countDown();
        }
    }

    private static class BLEventHandler<T> implements EventHandler<ValueEvent<T>>
    {
        private final RingBuffer<ValueEvent<T>> ringBuffer;

        public BLEventHandler(final RingBuffer<ValueEvent<T>> ringBuffer)
        {
            this.ringBuffer = ringBuffer;
        }

        @Override
        public void onEvent(final ValueEvent<T> event, final long sequence, final boolean endOfBatch) throws Exception
        {
            final long sequence2 = ringBuffer.next();
            try
            {
                final ValueEvent<T> event2 = ringBuffer.get(sequence2);
                event2.set(event.get());
                event2.setRequestTime(event.getRequestTime());
            }
            finally
            {
                ringBuffer.publish(sequence2);
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    public static void main(final String[] args) throws Exception
    {
        final EventFactory<ValueEvent<String>> factory = ValueEvent.factory();
        
        final Disruptor<ValueEvent<String>> disruptorA = new Disruptor<>(
            factory,
            512*1024,
            DaemonThreadFactory.INSTANCE,
            ProducerType.SINGLE,
            new BusySpinWaitStrategy());
        
        final Disruptor<ValueEvent<String>> disruptorB = new Disruptor<>(
            factory,
            512*1024,
            DaemonThreadFactory.INSTANCE,
            ProducerType.SINGLE,
            new BusySpinWaitStrategy());
        
        final PrintEventHandler<String> handlerA = new PrintEventHandler<>();
        final BLEventHandler<String> handlerBL = new BLEventHandler<>(disruptorB.getRingBuffer());
        disruptorA.handleEventsWith(handlerA).then(handlerBL);

        final int iterations = 10_000_000;
        final CountDownLatch latch = new CountDownLatch(iterations);

        final Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(1L), 5);

        final TraceEventHandler<String> handlerC = new TraceEventHandler<>(latch, histogram);
        disruptorB.handleEventsWith(handlerC);
        
        disruptorA.start();
        disruptorB.start();

        final ValuePublisher<String> publisher = new ValuePublisher<>(disruptorA.getRingBuffer());

        final Thread publisherThread = new Thread() {
            @Override
            public void run() {
                for (int i=0; i<iterations; i++) {
                    publisher.publish("xxx");
                }
            }
        };

        final long startTime = System.nanoTime();

        publisherThread.start();

        latch.await();

        final long endTime = System.nanoTime();
        final long elapsedTime = endTime - startTime;

        disruptorA.shutdown();
        disruptorB.shutdown();

        System.out.println(
            String.format("%d ops, %d ns, %d ms, rate %.02g ops/s",
                iterations,
                elapsedTime,
                TimeUnit.NANOSECONDS.toMillis(elapsedTime),
                ((double) iterations / (double) elapsedTime) * 1_000_000_000));

        System.out.println("Histogram of RTT latencies in microseconds");
        histogram.outputPercentileDistribution(System.out, 1000.0);

        System.exit(0);
    }
}
