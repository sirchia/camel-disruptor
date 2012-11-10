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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.impl.LoggingExceptionHandler;
import org.apache.camel.spi.ExceptionHandler;
import org.apache.camel.support.ServiceSupport;
import org.apache.camel.util.ExchangeHelper;

/**
 * TODO: documentation
 *
 * TODO: SuspendableService, ShutdownAware ?
 */
public class DisruptorConsumer extends ServiceSupport implements Consumer {

    private final DisruptorEndpoint endpoint;
    private final Processor processor;
    private ExceptionHandler exceptionHandler;

    public DisruptorConsumer(DisruptorEndpoint endpoint, Processor processor) {
        this.endpoint = endpoint;
        this.processor = processor;
    }

    public ExceptionHandler getExceptionHandler() {
        if (exceptionHandler == null) {
            exceptionHandler = new LoggingExceptionHandler(getClass());
        }
        return exceptionHandler;
    }

    public void setExceptionHandler(ExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public DisruptorEndpoint getEndpoint() {
        return endpoint;
    }

    @Override
    protected void doStart() throws Exception {
        getEndpoint().onStarted(this);
    }

    @Override
    protected void doStop() throws Exception {
        getEndpoint().onStopped(this);
    }

    public Set<LifecycleAwareExchangeEventHandler> createEventHandlers(int concurrentConsumers) {
        Set<LifecycleAwareExchangeEventHandler> eventHandlers = new HashSet<LifecycleAwareExchangeEventHandler>();

        for (int i = 0; i < concurrentConsumers; ++i) {
            eventHandlers.add(new ConsumerEventHandler(i, concurrentConsumers));
        }

        return eventHandlers;
    }

    /**
     * Strategy to prepare exchange for being processed by this consumer
     *
     * @param exchange the exchange
     * @return the exchange to process by this consumer.
     */
    protected Exchange prepareExchange(Exchange exchange) {
        // send a new copied exchange with new camel context
        Exchange newExchange = ExchangeHelper.copyExchangeAndSetCamelContext(exchange, getEndpoint().getCamelContext());
        // set the from endpoint
        newExchange.setFromEndpoint(getEndpoint());
        return newExchange;
    }

    public class ConsumerEventHandler implements LifecycleAwareExchangeEventHandler {

        private final int ordinal;
        private final int concurrentConsumers;
        private volatile CountDownLatch latch;

        public ConsumerEventHandler(int ordinal, int concurrentConsumers) {

            this.ordinal = ordinal;
            this.concurrentConsumers = concurrentConsumers;
        }

        @Override
        public void onEvent(final ExchangeEvent event, long sequence, boolean endOfBatch) throws Exception {
            // Consumer threads are managed at the endpoint to achieve the optimal performance.
            // However, both multiple consumers (pub-sub style multicasting) as well as 'worker-pool' consumers dividing
            // exchanges amongst them are scheduled on their own threads and are provided with all exchanges.
            // To prevent duplicate exchange processing by worker-pool event handlers, they are all given an ordinal,
            // which can be used to determine whether he should process the exchange, or leave it for his brethren.
            //see http://code.google.com/p/disruptor/wiki/FrequentlyAskedQuestions#How_do_you_arrange_a_Disruptor_with_multiple_consumers_so_that_e
            if (sequence % concurrentConsumers == ordinal) {
                Exchange exchange = prepareExchange(event.getExchange());

                try {
                    processor.process(exchange);
                } catch (Throwable e) {
                    if (exchange != null) {
                        getExceptionHandler().handleException("Error processing exchange", exchange, e);
                    } else {
                        getExceptionHandler().handleException(e);
                    }
                }
            }
        }

        @Override
        public void await() throws InterruptedException {
            if (latch != null) {
                latch.await();
            }
        }

        @Override
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            if (latch != null) {
                return latch.await(timeout, unit);
            } else {
                return true;
            }
        }

        @Override
        public void onStart() {
            latch = new CountDownLatch(1);
        }

        @Override
        public void onShutdown() {
            latch.countDown();
        }
    }
}
