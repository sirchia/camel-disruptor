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

import java.util.concurrent.CountDownLatch;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Test;

/**
 * TODO: documentation
 */
public class SedaDisruptorCompareTest extends CamelTestSupport {

    @Produce(uri = "disruptor:test")
    protected ProducerTemplate template;

    private static final int SPEED_TEST_EXCHANGE_COUNT = 10000;
    private static final Integer VALUE = Integer.valueOf(42);

    private ExchangeAwaiter exchangeAwaiter =  new ExchangeAwaiter(SPEED_TEST_EXCHANGE_COUNT);


    @Test
    public void speedTestDisruptor() throws InterruptedException {
        exchangeAwaiter.reset();

        long start = System.currentTimeMillis();

        for (int i = 0; i < SPEED_TEST_EXCHANGE_COUNT; ++i) {
            template.asyncSendBody("disruptor:speedTest", VALUE);
        }

        exchangeAwaiter.awaitMessagesReceived();

        long stop = System.currentTimeMillis();

        System.out.println("disruptor time spent = " + (stop-start) + " ms");

    }

    @Test
    public void speedTestSeda() throws InterruptedException {
        exchangeAwaiter.reset();

        long start = System.currentTimeMillis();

        for (int i = 0; i < SPEED_TEST_EXCHANGE_COUNT; ++i) {
            template.asyncSendBody("seda:speedTest", VALUE);
        }

        exchangeAwaiter.awaitMessagesReceived();

        long stop = System.currentTimeMillis();

        System.out.println("seda time spent = " + (stop-start) + " ms");

    }


    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("disruptor:speedTest").process(exchangeAwaiter);

                from("seda:speedTest").process(exchangeAwaiter);
            }
        };
    }

    private static final class ExchangeAwaiter implements Processor {

        private CountDownLatch latch;
        private final int count;

        public ExchangeAwaiter(int count) {
            this.count = count;
        }

        public void reset() {
            latch = new CountDownLatch(count);
        }

        public void awaitMessagesReceived() throws InterruptedException {
            latch.await();
        }

        @Override
        public void process(Exchange exchange) throws Exception {
            latch.countDown();
        }
    }
}
