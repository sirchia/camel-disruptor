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

import com.github.camel.component.disruptor.DisruptorComponent;
import java.util.Iterator;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

/**
 *
 */
public class DisruptorComponentReferenceEndpointTest extends CamelTestSupport {
    @Test
    public void testDisruptorComponentReference() throws Exception {
        DisruptorComponent disruptor = context.getComponent("disruptor", DisruptorComponent.class);

        String fooKey = disruptor.getDisruptorKey("disruptor://foo");
        assertEquals(1, disruptor.getDisruptors().get(fooKey).getCount());
        assertEquals(2, numberOfReferences(disruptor));

        // add a second consumer on the endpoint
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("disruptor:foo?concurrentConsumers=1").routeId("foo2").to("mock:foo2");
            }
        });

        assertEquals(2, disruptor.getDisruptors().get(fooKey).getCount());
        assertEquals(3, numberOfReferences(disruptor));

        // remove the 1st route
        context.stopRoute("foo");
        context.removeRoute("foo");

        assertEquals(1, disruptor.getDisruptors().get(fooKey).getCount());
        assertEquals(2, numberOfReferences(disruptor));

        // remove the 2nd route
        context.stopRoute("foo2");
        context.removeRoute("foo2");

        // and there is no longer disruptors for the foo key
        assertTrue(disruptor.getDisruptors().get(fooKey).hasNullReference());

        // there should still be a bar
        assertEquals(1, numberOfReferences(disruptor));
        String barKey = disruptor.getDisruptorKey("disruptor://bar");
        assertEquals(1, disruptor.getDisruptors().get(barKey).getCount());
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("disruptor:foo").routeId("foo").to("mock:foo");

                from("disruptor:bar").routeId("bar").to("mock:bar");
            }
        };
    }

    private int numberOfReferences(DisruptorComponent disruptor) {
        int num = 0;
        Iterator<DisruptorComponent.DisruptorReference> it = disruptor.getDisruptors().values().iterator();
        while (it.hasNext()) {
            num += it.next().getCount();
        }
        return num;
    }

}
