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

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;

import static org.agrona.BitUtil.align;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.*;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.*;

/**
 * A ring-buffer that supports the exchange of messages from many producers to multiple consumers.
 */
public class ManyToManyRingBuffer extends ManyToOneRingBuffer
{
    /**
     * Interim value specifying that an has already been acquired -- and should be released eventually.
     */
    public static final int INTERIM_ACQUIRED_INDEX = Integer.MIN_VALUE;

    private final int headReadPositionIndex;
    private final int headPeekPositionIndex;
    private final int headAcquiredPositionIndex;

    /**
     * {@inheritDoc}
     */
    public ManyToManyRingBuffer(final AtomicBuffer buffer)
    {
        super(buffer);

        final int capacity = this.capacity();
        headReadPositionIndex = capacity + HEAD_POSITION_OFFSET;
        headPeekPositionIndex = capacity + HEAD_CACHE_POSITION_OFFSET;
        headAcquiredPositionIndex = capacity + HEAD_CACHE_POSITION_OFFSET + SIZE_OF_INT;
    }

    private int acquirePeek(final AtomicBuffer buffer)
    {
        final int headPeekPositionIndex = this.headPeekPositionIndex;
        final int peek = buffer.getAndSetInt(headPeekPositionIndex, INTERIM_ACQUIRED_INDEX);

        if (peek >= 0)
        {
            buffer.putIntVolatile(headAcquiredPositionIndex, peek);
        }

        return peek;
    }

    private int releasePeek(final AtomicBuffer buffer, final int value, final int expect)
    {
        final int peek = buffer.getAndSetInt(headAcquiredPositionIndex, -1 - value);

        if (peek == expect)
        {
            buffer.putIntVolatile(headPeekPositionIndex, value);
            return -1 - expect;
        }
        else
        {
            // unexpected condition
            buffer.putIntVolatile(headPeekPositionIndex, value);
            return INTERIM_ACQUIRED_INDEX;
        }
    }

    /**
     * {@inheritDoc}
     */
    public int read(final MessageHandler handler, final int messageCountLimit)
    {
        final AtomicBuffer buffer = this.buffer();
        final int mask = this.capacity() - 1;
        int bytesRead = 0;
        int messagesRead = 0;
        int peekIndex = acquirePeek(buffer);
        int markIndex = -1;
        int markBytes = 0;

        try
        {
            while (peekIndex >= 0)
            {
                final long header = buffer.getLongVolatile(peekIndex);
                final int recordLength = recordLength(header);

                if (recordLength <= 0)
                {
                    peekIndex = releasePeek(buffer, peekIndex, peekIndex);
                }
                else
                {
                    final int recordIndex = peekIndex;
                    markBytes = align(recordLength, ALIGNMENT);
                    peekIndex = releasePeek(buffer, mask & (peekIndex + markBytes), peekIndex);
                    markIndex = recordIndex;

                    bytesRead += markBytes;

                    final int messageTypeId = messageTypeId(header);
                    if (PADDING_MSG_TYPE_ID != messageTypeId)
                    {
                        ++messagesRead;
                        handler.onMessage(messageTypeId, buffer,
                            recordIndex + HEADER_LENGTH, recordLength - HEADER_LENGTH);
                    }

                    buffer.putLongVolatile(markIndex, makeHeader(-markBytes, PADDING_MSG_TYPE_ID));
                    markIndex = -1;
                    markBytes = 0;

                    if (messagesRead < messageCountLimit)
                    {
                        peekIndex = acquirePeek(buffer);
                    }
                }
            }
        }
        finally
        {
            if (markIndex >= 0)
            {
                buffer.putLongVolatile(markIndex, makeHeader(-markBytes, PADDING_MSG_TYPE_ID));
                markIndex = -1;
                markBytes = 0;
            }

            if (peekIndex >= 0)
            {
                peekIndex = releasePeek(buffer, peekIndex, peekIndex);
            }

            if (bytesRead > 0)
            {
                // dual reclamation attemps per read
                reclaimRead(buffer);
                reclaimRead(buffer);
            }
        }

        return messagesRead;
    }

    private void reclaimRead(final AtomicBuffer buffer)
    {
        final int headReadPositionIndex = this.headReadPositionIndex;
        final int capacity = this.capacity();
        final int mask = capacity - 1;
        final long clear = buffer.getLongVolatile(headReadPositionIndex);

        int bytesCleared = 0;
        int clearIndex = mask & (int)clear;

        do
        {
            final long header = buffer.getLongVolatile(clearIndex);
            final int recordLength = recordLength(header);

            if (recordLength > 0 || messageTypeId(header) != PADDING_MSG_TYPE_ID)
            {
                break;
            }
            else if (!buffer.compareAndSetLong(clearIndex, header, makeHeader(recordLength, 0)))
            {
                break;
            }
            else
            {
                final int recordBytes = align(-recordLength, ALIGNMENT);
                bytesCleared += recordBytes;
                clearIndex += recordBytes;
            }
        }
        while (clearIndex < capacity);

        if (bytesCleared > 0)
        {
            buffer.setMemory(mask & (int)clear, bytesCleared, (byte)0);
            buffer.putLongOrdered(headReadPositionIndex, clear + bytesCleared);
        }
    }
}
