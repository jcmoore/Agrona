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

//import org.junit.Test;
//import org.agrona.BitUtil;
//import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
//import java.util.concurrent.CyclicBarrier;

//import static org.hamcrest.core.Is.is;
//import static org.junit.Assert.assertThat;
import static org.agrona.concurrent.ringbuffer.RingBufferDescriptor.TRAILER_LENGTH;

public class ManyToOneRingBufferConcurrentTest
{
    private static final int MSG_TYPE_ID = 7;

    private final ByteBuffer byteBuffer = ByteBuffer.allocateDirect((16 * 1024) + TRAILER_LENGTH);
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    private final RingBuffer ringBuffer = new ManyToOneRingBuffer(unsafeBuffer);

}
