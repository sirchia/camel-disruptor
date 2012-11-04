/*
 * Copyright (c) 2012, Riccardo Sirchia
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
