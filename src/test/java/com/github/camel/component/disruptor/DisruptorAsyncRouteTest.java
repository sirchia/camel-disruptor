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
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

/**
 * Unit test based on user forum request.
 */
public class DisruptorAsyncRouteTest extends CamelTestSupport {
    @Test
    public void testSendAsync() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedBodiesReceived("Hello World");

        // START SNIPPET: e2
        Object out = template.requestBody("direct:start", "Hello World");
        assertEquals("OK", out);
        // END SNIPPET: e2

        assertMockEndpointsSatisfied();
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            // START SNIPPET: e1
            @Override
            public void configure() throws Exception {
                from("direct:start")
                        // send it to the disruptor ring buffer that is async
                        .to("disruptor:next")
                                // return a constant response
                        .transform(constant("OK"));

                from("disruptor:next").to("mock:result");
            }
            // END SNIPPET: e1
        };
    }
}
