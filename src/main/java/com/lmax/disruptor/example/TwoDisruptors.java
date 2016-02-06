package com.lmax.disruptor.example;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class TwoDisruptors
{
    private static class ValueEvent<T>
    {
        private T t;

        public T get()
        {
            return t;
        }

        public void set(final T t)
        {
            this.t = t;
        }
    }

    private static class Translator<T> implements EventTranslatorOneArg<ValueEvent<T>, ValueEvent<T>>
    {
        @Override
        public void translateTo(final ValueEvent<T> event, final long sequence, final ValueEvent<T> arg0)
        {
            event.set(arg0.get());
        }
    }

    private static class TTranslator<T> implements EventTranslatorOneArg<ValueEvent<T>, T>
    {
        @Override
        public void translateTo(final ValueEvent<T> event, final long sequence, final T arg0)
        {
            event.set(arg0);
        }
    }

    private static class ValueEventHandler<T> implements EventHandler<ValueEvent<T>>
    {
        private final RingBuffer<ValueEvent<T>> ringBuffer;
        private final Translator<T> translator = new Translator<T>();
        private final TTranslator<T> tTranslator = new TTranslator<T>();

        ValueEventHandler(final RingBuffer<ValueEvent<T>> ringBuffer)
        {
            this.ringBuffer = ringBuffer;
        }

        public void start(final T t)
        {
            ringBuffer.publishEvent(tTranslator, t);
        }

        @Override
        public void onEvent(final ValueEvent<T> event, final long sequence, final boolean endOfBatch) throws Exception
        {
            ringBuffer.publishEvent(translator, event);
        }

        public static <T> EventFactory<ValueEvent<T>> factory()
        {
            return () -> new ValueEvent<>();
        }
    }

    @SuppressWarnings("unchecked")
    public static void main(final String[] args)
    {
        final Executor executor = Executors.newFixedThreadPool(2);
        final EventFactory<ValueEvent<String>> factory = ValueEventHandler.factory();

        final Disruptor<ValueEvent<String>> disruptorA = new Disruptor<>(
            factory,
            1024,
            executor,
            ProducerType.MULTI,
            new BlockingWaitStrategy());

        final Disruptor<ValueEvent<String>> disruptorB = new Disruptor<>(
            factory,
            1024,
            executor,
            ProducerType.SINGLE,
            new BlockingWaitStrategy());

        final ValueEventHandler<String> handlerA = new ValueEventHandler<>(disruptorB.getRingBuffer());
        disruptorA.handleEventsWith(handlerA);

        final ValueEventHandler<String> handlerB = new ValueEventHandler<>(disruptorA.getRingBuffer());
        disruptorB.handleEventsWith(handlerB);

        disruptorA.start();
        disruptorB.start();

        handlerA.start("");
    }
}
