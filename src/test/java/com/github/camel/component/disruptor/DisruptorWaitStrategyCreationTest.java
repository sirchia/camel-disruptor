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

import com.lmax.disruptor.WaitStrategy;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

/**
 * Tests correct creation of all supposedly possible wait strategies.
 */
public class DisruptorWaitStrategyCreationTest {
    @Test
    public void testCreateWaitStrategyInstance() throws Exception {
        for (DisruptorWaitStrategy strategy : DisruptorWaitStrategy.values()) {
            WaitStrategy waitStrategyInstance = strategy.createWaitStrategyInstance();

            assertNotNull(waitStrategyInstance);
            assertTrue(waitStrategyInstance instanceof WaitStrategy);
        }
    }
}
