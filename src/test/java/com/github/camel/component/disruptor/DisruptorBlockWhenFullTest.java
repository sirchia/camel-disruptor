/*
 * Copyright 2012 Riccardo Sirchia
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.camel.component.disruptor;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

/**
 * Tests that a Disruptor producer blocks when a message is sent while the ring buffer is full.
 */
public class DisruptorBlockWhenFullTest extends CamelTestSupport {
    private static final int QUEUE_SIZE = 8;

    private static final int DELAY = 100;

    private static final String MOCK_URI = "mock:blockWhenFullOutput";

    private static final String DEFAULT_URI = "disruptor:foo?size=" + QUEUE_SIZE;

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from(DEFAULT_URI).delay(DELAY).to(MOCK_URI);
            }
        };
    }

    @Test
    public void testDisruptorBlockingWhenFull() throws Exception {
        getMockEndpoint(MOCK_URI).setExpectedMessageCount(QUEUE_SIZE + 20);

        DisruptorEndpoint disruptor = context.getEndpoint(DEFAULT_URI, DisruptorEndpoint.class);
        assertEquals(QUEUE_SIZE, disruptor.remainingCapacity());

        sendSoManyOverCapacity(DEFAULT_URI, QUEUE_SIZE, 20);
        assertMockEndpointsSatisfied();
    }

    /**
     * This method make sure that we hit the limit by sending 'soMany' messages over the given capacity which allows the
     * delayer to kick in.
     */
    private void sendSoManyOverCapacity(String uri, int capacity, int soMany) {
        for (int i = 0; i < (capacity + soMany); i++) {
            template.sendBody(uri, "Message " + i);
        }
    }

}
