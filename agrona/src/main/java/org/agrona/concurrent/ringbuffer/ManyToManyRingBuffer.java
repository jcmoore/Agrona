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
 *
 * Solo-consumers (for instance {@link OneToOneRingBuffer} and {@link ManyToOneRingBuffer})
 * must not be allowed to {@link RingBuffer#read} from the same {@link AtomicBuffer} as these multi-consumers.
 */
public class ManyToManyRingBuffer extends ManyToOneRingBuffer
{
    /**
     * Interim value specifying that an has already been acquired -- and should be released eventually.
     */
    public static final int INTERIM_ACQUIRED_INDEX = Integer.MIN_VALUE;

    /**
     * Record type is reading to prevent multiple access consumer access to the same message.
     */
    public static final int READING_MSG_TYPE_ID;

    /**
     * Record type is handled to permit buffer space to be reclaimed for subsequent writes.
     */
    public static final int HANDLED_MSG_TYPE_ID;

    /**
     * Record type is zeroing to prevent multiple reclamations of the same buffer space.
     */
    public static final int ZEROING_MSG_TYPE_ID;

    static
    {
        int enumerate = Integer.MIN_VALUE;
        READING_MSG_TYPE_ID = ++enumerate;
        HANDLED_MSG_TYPE_ID = ++enumerate;
        ZEROING_MSG_TYPE_ID = ++enumerate;
    }

    private final int headReadPositionIndex;
    private final int headSkipPositionIndex;
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
        headSkipPositionIndex = capacity + HEAD_CACHE_POSITION_OFFSET;
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
        //return readClaim(handler, messageCountLimit);
        return readAcquire(handler, messageCountLimit);
    }

    private int readClaim(final MessageHandler handler, final int messageCountLimit)
    {
        final AtomicBuffer buffer = this.buffer();
        final int headReadPositionIndex = this.headReadPositionIndex;
        final int mask = this.capacity() - 1;

        int messageCount = 0;
        int handledCount = 0;
        long indexType = readReserve(buffer);

        while (indexType != 0 && messageCount < messageCountLimit)
        {
            final long head = buffer.getLongVolatile(headReadPositionIndex);

            final int recordIndex = recordLength(indexType);
            final int messageTypeId = messageTypeId(indexType);
            final long recordHeader = buffer.getLongVolatile(recordIndex);
            final int messageLength = recordLength(recordHeader);

            if (PADDING_MSG_TYPE_ID != messageTypeId)
            {
                try
                {
                    handler.onMessage(messageTypeId, buffer,
                        recordIndex + HEADER_LENGTH, messageLength - HEADER_LENGTH);
                }
                finally
                {
                    messageCount += 1;
                }
            }

            readShortcut(buffer, mask & (recordIndex + align(messageLength, ALIGNMENT)), head);

            buffer.putLongVolatile(recordIndex, makeHeader(messageLength, HANDLED_MSG_TYPE_ID));
            handledCount++;

            indexType = readReserve(buffer);
        }

        if (handledCount > 0)
        {
            final long current = reclaimHandled(buffer);
            readShortcut(buffer, mask & (int)current, current);
        }

        return messageCount;
    }

    private long readReserve(final AtomicBuffer buffer)
    {
        final int indexBitDepth = this.indexBitDepth();
        final int headSkipPositionIndex = this.headSkipPositionIndex;
        final int mask = this.capacity() - 1;

        final long bits = buffer.getAndAddLong(headSkipPositionIndex, 1L << indexBitDepth);
        final long skip = bits >> indexBitDepth;
        long after = bits & mask;
        long count = 0;

        int recordIndex;
        int messageLength;
        int messageTypeId;
        long recordHeader;

        do
        {
            recordIndex = mask & (int)after;
            recordHeader = buffer.getLongVolatile(recordIndex);
            messageLength = recordLength(recordHeader);
            messageTypeId = messageTypeId(recordHeader);

            if (messageLength < 0 || messageTypeId == 0)
            {
                readShortcut(buffer, recordIndex, buffer.getLongVolatile(headReadPositionIndex));
                return 0;
            }
            else
            {
                after += align(messageLength, ALIGNMENT);
            }
        }
        while (count++ < skip);

        if (messageTypeId > 0 || messageTypeId == PADDING_MSG_TYPE_ID)
        {
            if (buffer.compareAndSetLong(recordIndex, recordHeader, makeHeader(messageLength, READING_MSG_TYPE_ID)))
            {
                readShortcut(buffer, recordIndex, buffer.getLongVolatile(headReadPositionIndex));
                return makeHeader(recordIndex, messageTypeId);
            }
        }

        readShortcut(buffer, recordIndex, buffer.getLongVolatile(headReadPositionIndex));
        return 0;
    }

    private void readShortcut(final AtomicBuffer buffer, final int index, final long head)
    {
        final int indexBitDepth = this.indexBitDepth();
        final int capacity = this.capacity();
        final int headSkipPositionIndex = this.headSkipPositionIndex;
        final int mask = capacity - 1;

        final long bits = buffer.getLongVolatile(headSkipPositionIndex);
        final long skip = bits >> indexBitDepth;
        final long before = bits & mask;

        final long presume = before < (mask & head) ? before + capacity : before;
        final long suggest = index < (mask & head) ? index + capacity : index;

        if (suggest > presume)
        {
            final long replaced = buffer.getAndSetLong(headSkipPositionIndex, index);
            final long after = replaced & mask;
            final long value = after < (mask & head) ? after + capacity : after;

            if (value > suggest)
            {
                buffer.putLongVolatile(headSkipPositionIndex, replaced);
            }
        }
    }

    private int readAcquire(final MessageHandler handler, final int messageCountLimit)
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

                    buffer.putLongVolatile(markIndex, makeHeader(markBytes, HANDLED_MSG_TYPE_ID));
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
                buffer.putLongVolatile(markIndex, makeHeader(markBytes, HANDLED_MSG_TYPE_ID));
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
                reclaimHandled(buffer);
                reclaimHandled(buffer);
            }
        }

        return messagesRead;
    }

    private long reclaimHandled(final AtomicBuffer buffer)
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

            if (recordLength < 0 || messageTypeId(header) != HANDLED_MSG_TYPE_ID)
            {
                break;
            }
            else if (!buffer.compareAndSetLong(clearIndex, header, makeHeader(recordLength, ZEROING_MSG_TYPE_ID)))
            {
                break;
            }
            else
            {
                final int recordBytes = align(recordLength, ALIGNMENT);
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

        return clear + bytesCleared;
    }
}
