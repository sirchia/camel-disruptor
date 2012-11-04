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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Assert;
import org.junit.Test;

/**
 * TODO: documentation
 */
public class BasicDisruptorComponentTest extends CamelTestSupport {
    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @Produce(uri = "disruptor:test")
    protected ProducerTemplate template;

    private static final Integer VALUE = Integer.valueOf(42);

    private ThreadCounter threadCounter = new ThreadCounter();

    @Test
    public void testProduce() throws InterruptedException {
        resultEndpoint.expectedBodiesReceived(VALUE);
        resultEndpoint.setExpectedMessageCount(1);

        template.asyncSendBody("disruptor:test", VALUE);

        resultEndpoint.await(5, TimeUnit.SECONDS);
        resultEndpoint.assertIsSatisfied();
    }


    @Test
    public void testAsynchronous() throws InterruptedException {
        threadCounter.reset();
        
        int messagesSent = 1000;

        resultEndpoint.setExpectedMessageCount(messagesSent);

        long currentThreadId = Thread.currentThread().getId();

        for (int i = 0; i < messagesSent; ++i) {
            template.asyncSendBody("disruptor:testAsynchronous", VALUE);
        }

        resultEndpoint.await(20, TimeUnit.SECONDS);
        resultEndpoint.assertIsSatisfied();

        Assert.assertTrue(threadCounter.getThreadIdCount() > 0);
        Assert.assertFalse(threadCounter.getThreadIds().contains(currentThreadId));
    }

    @Test
    public void testMultipleConsumers() throws InterruptedException {
        threadCounter.reset();
        
        int messagesSent = 1000;

        resultEndpoint.setExpectedMessageCount(messagesSent);

        for (int i = 0; i < messagesSent; ++i) {
            template.asyncSendBody("disruptor:testMultipleConsumers?concurrentConsumers=4", VALUE);
        }

        resultEndpoint.await(20, TimeUnit.SECONDS);

        //sleep for another second to check for duplicate messages in transit
        Thread.sleep(1000);

        System.out.println("count = " + resultEndpoint.getReceivedCounter());
        resultEndpoint.assertIsSatisfied();

        Assert.assertEquals(4, threadCounter.getThreadIdCount());
    }



    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("disruptor:test").to("mock:result");
                from("disruptor:testAsynchronous").process(threadCounter).to("mock:result");
                from("disruptor:testMultipleConsumers?concurrentConsumers=4").process(threadCounter).to("mock:result");
            }
        };
    }

    private static final class ThreadCounter implements Processor {

        private Set<Long> threadIds = new HashSet<Long>();

        public void reset() {
            threadIds.clear();
        }
        
        @Override
        public void process(Exchange exchange) throws Exception {
            threadIds.add(Thread.currentThread().getId());
        }

        public Set<Long> getThreadIds() {
            return Collections.unmodifiableSet(threadIds);
        }

        public int getThreadIdCount() {
            return threadIds.size();
        }
    }
}
