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

import com.github.camel.component.disruptor.DisruptorEndpoint;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ExchangeTimedOutException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

/**
 * @version
 */
public class DisruptorTimeoutTest extends CamelTestSupport {
    private int timeout = 100;

    @Test
    public void testDisruptorNoTimeout() throws Exception {
        Future<String> out = template.asyncRequestBody("disruptor:foo", "World", String.class);
        assertEquals("Bye World", out.get());
    }

    @Test
    public void testDisruptorTimeout() throws Exception {
        Future<String> out = template.asyncRequestBody("disruptor:foo?timeout=" + timeout, "World", String.class);
        try {
            out.get();
            fail("Should have thrown an exception");
        } catch (ExecutionException e) {
            assertIsInstanceOf(CamelExecutionException.class, e.getCause());
            assertIsInstanceOf(ExchangeTimedOutException.class, e.getCause().getCause());

            DisruptorEndpoint de = (DisruptorEndpoint) context.getRoute("disruptor").getEndpoint();
            assertNotNull("Consumer endpoint cannot be null", de);
            // TODO: Check why this is not always true.
            // assertEquals("Timeout Exchanges should be removed from disruptor", 0, de.getSize() -
            // de.remainingCapacity());
            assertMockEndpointsSatisfied();
        }
    }

    @Test
    public void testDisruptorTimeoutWithStoppedRoute() throws Exception {
        context.stopRoute("disruptor");
        timeout = 500;
        testDisruptorTimeout();
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("disruptor:foo").routeId("disruptor").to("mock:before").delay(250).transform(body().prepend("Bye ")).to("mock:result");
            }
        };
    }
}