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

import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

/**
 * @version
 */
public class DisruptorConfigureTest extends CamelTestSupport {
    @Test
    public void testSizeConfigured() throws Exception {
        DisruptorEndpoint endpoint = resolveMandatoryEndpoint("disruptor:foo?size=2000", DisruptorEndpoint.class);
        assertEquals("size", 2048, endpoint.getSize());
        assertEquals("remainingCapacity", 2048, endpoint.remainingCapacity());
    }

    @Test
    public void testConcurrentConsumersConfigured() {
        DisruptorEndpoint endpoint = resolveMandatoryEndpoint("disruptor:foo?concurrentConsumers=5", DisruptorEndpoint.class);
        assertEquals("concurrentConsumers", 5, endpoint.getConcurrentConsumers());
    }

    @Test
    public void testDefaults() {
        DisruptorEndpoint endpoint = resolveMandatoryEndpoint("disruptor:foo", DisruptorEndpoint.class);
        assertEquals("concurrentConsumers: wrong default", 1, endpoint.getConcurrentConsumers());
        assertEquals("bufferSize: wrong default", DisruptorComponent.DEFAULT_BUFFER_SIZE, endpoint.getSize());
        assertEquals("timeout: wrong default", 30000L, endpoint.getTimeout());
    }
}
