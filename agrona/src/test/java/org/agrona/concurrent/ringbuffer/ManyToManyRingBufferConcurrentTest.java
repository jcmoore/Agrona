/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package org.agrona.concurrent.ringbuffer;

import org.junit.Test;
import org.agrona.BitUtil;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class ManyToManyRingBufferConcurrentTest
{
    private static final int MSG_TYPE_ID = 7;

    @Test
    public void shouldExchangeMessagesSingleReader()
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH);
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        final RingBuffer ringBuffer = new ManyToManyRingBuffer(unsafeBuffer);

        final int reps = 10 * 1000 * 1000;
        final int numProducers = 2;
        final CyclicBarrier barrier = new CyclicBarrier(numProducers);

        for (int i = 0; i < numProducers; i++)
        {
            new Thread(new Producer(i, barrier, ringBuffer, reps)).start();
        }

        final int[] counts = new int[numProducers];

        final MessageHandler handler =
            (msgTypeId, buffer, index, length) ->
            {
                final int producerId = buffer.getInt(index);
                final int iteration = buffer.getInt(index + BitUtil.SIZE_OF_INT);

                final int count = counts[producerId];
                assertThat(iteration, is(count));

                counts[producerId]++;
            };

        int msgCount = 0;
        while (msgCount < (reps * numProducers))
        {
            final int readCount = ringBuffer.read(handler);
            if (0 == readCount)
            {
                Thread.yield();
            }

            msgCount += readCount;
        }

        assertThat(msgCount, is(reps * numProducers));
    }

    class Producer implements Runnable
    {
        private final int producerId;
        private final RingBuffer ringBuffer;
        private final CyclicBarrier barrier;
        private final int reps;

        Producer(final int producerId, final CyclicBarrier barrier, final RingBuffer ringBuffer, final int reps)
        {
            this.producerId = producerId;
            this.ringBuffer = ringBuffer;
            this.barrier = barrier;
            this.reps = reps;
        }

        public void run()
        {
            try
            {
                barrier.await();
            }
            catch (final Exception ignore)
            {
            }

            final RingBuffer ringBuffer = this.ringBuffer;

            final int length = BitUtil.SIZE_OF_INT * 7; // messages will straddle the buffer boundary at this size
            final int repsValueOffset = BitUtil.SIZE_OF_INT;
            final UnsafeBuffer srcBuffer = new UnsafeBuffer(new byte[1024]);

            srcBuffer.putInt(0, producerId);

            for (int i = 0; i < reps; i++)
            {
                srcBuffer.putInt(repsValueOffset, i);

                while (!ringBuffer.write(MSG_TYPE_ID, srcBuffer, 0, length))
                {
                    Thread.yield();
                }
            }
        }
    }

    @Test
    public void shouldExchangeMessagesMultiReader()
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH);
        final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
        final RingBuffer ringBuffer = new ManyToManyRingBuffer(unsafeBuffer);

        final int reps = 10 * 1000 * 1000;
        final int numProducers = 2;
        final int numConsumers = 8;
        final CyclicBarrier writeBarrier = new CyclicBarrier(numProducers);
        final CyclicBarrier readBarrier = new CyclicBarrier(numConsumers);

        final UnsafeBuffer sharedBuffer = new UnsafeBuffer(new byte[1024]);
        final UnsafeBuffer totalBuffer = new UnsafeBuffer(new byte[1024]);

        totalBuffer.putIntVolatile(0, reps * numProducers);

        for (int i = 0; i < numProducers; i++)
        {
            new Thread(new Producer(i, writeBarrier, ringBuffer, reps)).start();
        }

        for (int i = 0; i < numConsumers; i++)
        {
            new Thread(new Consumer(i + 1, readBarrier, ringBuffer, totalBuffer, sharedBuffer)).start();
        }

        int msgCount = totalBuffer.getIntVolatile(0);
        while (msgCount > 0)
        {
            Thread.yield();
            msgCount = totalBuffer.getIntVolatile(0);
        }

        assertThat(msgCount, is(0));
        msgCount = 0;

        for (int i = 0; i < numConsumers; i++)
        {
            final int consumerIndex = (1 + i) * BitUtil.SIZE_OF_INT;
            msgCount += totalBuffer.getIntVolatile(consumerIndex);
        }

        assertThat(msgCount, is(reps * numProducers));

        for (int i = 0; i < numProducers; i++)
        {
            final int producerIndex = i * BitUtil.SIZE_OF_INT;
            assertThat(sharedBuffer.getIntVolatile(producerIndex), is(reps));
        }
    }

    class Consumer implements Runnable
    {
        private final int consumerId;
        private final RingBuffer ringBuffer;
        private final CyclicBarrier barrier;
        private final UnsafeBuffer compareBuffer;
        private final UnsafeBuffer sharedBuffer;
        private final UnsafeBuffer totalBuffer;

        Consumer(final int consumerId, final CyclicBarrier barrier, final RingBuffer ringBuffer,
            final UnsafeBuffer totalBuffer, final UnsafeBuffer sharedBuffer)
        {
            this.consumerId = consumerId;
            this.ringBuffer = ringBuffer;
            this.barrier = barrier;
            this.totalBuffer = totalBuffer;
            this.sharedBuffer = sharedBuffer;
            this.compareBuffer = new UnsafeBuffer(new byte[sharedBuffer.capacity()]);
        }

        public void run()
        {
            try
            {
                barrier.await();
            }
            catch (final Exception ignore)
            {
            }

            final RingBuffer ringBuffer = this.ringBuffer;
            final UnsafeBuffer compareBuffer = this.compareBuffer;
            final UnsafeBuffer sharedBuffer = this.sharedBuffer;
            final UnsafeBuffer totalBuffer = this.totalBuffer;
            final int totalIndex = this.consumerId * BitUtil.SIZE_OF_INT;

            final MessageHandler handler =
                (msgTypeId, buffer, index, length) ->
                {
                    final int producerId = buffer.getInt(index);
                    final int iteration = buffer.getInt(index + BitUtil.SIZE_OF_INT);
                    final int testIndex = producerId * BitUtil.SIZE_OF_INT;

                    final int count = sharedBuffer.getAndAddInt(testIndex, 1);
                    final int test = compareBuffer.getAndSetInt(testIndex, count);
                    assertThat(iteration, greaterThanOrEqualTo(test));
                };

            while (totalBuffer.getIntVolatile(0) > 0)
            {
                final int readCount = ringBuffer.read(handler);
                if (0 == readCount)
                {
                    Thread.yield();
                }
                else
                {
                    totalBuffer.getAndAddInt(totalIndex, readCount);
                    totalBuffer.getAndAddInt(0, -readCount);
                }
            }
        }
    }
}
