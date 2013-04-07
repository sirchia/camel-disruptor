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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.camel.*;
import org.apache.camel.impl.LoggingExceptionHandler;
import org.apache.camel.spi.ExceptionHandler;
import org.apache.camel.spi.Synchronization;
import org.apache.camel.support.ServiceSupport;
import org.apache.camel.util.AsyncProcessorConverterHelper;
import org.apache.camel.util.AsyncProcessorHelper;
import org.apache.camel.util.ExchangeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: documentation
 */
public class DisruptorConsumer extends ServiceSupport implements Consumer, SuspendableService {

    private static final Logger LOG = LoggerFactory.getLogger(DisruptorConsumer.class);

    private final DisruptorEndpoint endpoint;
    private final AsyncProcessor processor;
    private ExceptionHandler exceptionHandler;

    public DisruptorConsumer(DisruptorEndpoint endpoint, Processor processor) {
        this.endpoint = endpoint;
        this.processor = AsyncProcessorConverterHelper.convert(processor);
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

    @Override
    protected void doSuspend() throws Exception {
        getEndpoint().onStopped(this);
    }

    @Override
    protected void doResume() throws Exception {
        getEndpoint().onStarted(this);
    }

    public Set<LifecycleAwareExchangeEventHandler> createEventHandlers(int concurrentConsumers) {
        Set<LifecycleAwareExchangeEventHandler> eventHandlers = new HashSet<LifecycleAwareExchangeEventHandler>();

        for (int i = 0; i < concurrentConsumers; ++i) {
            eventHandlers.add(new ConsumerEventHandler(i, concurrentConsumers));
        }

        return eventHandlers;
    }

    private Exchange prepareExchange(Exchange exchange) {
        // send a new copied exchange with new camel context
        Exchange newExchange = ExchangeHelper.copyExchangeAndSetCamelContext(exchange, endpoint.getCamelContext());
        // set the from endpoint
        newExchange.setFromEndpoint(endpoint);
        return newExchange;
    }

    private void process(Exchange exchange) {
        int size = endpoint.getConsumers().size();

        // if there are multiple consumers then we should make a copy of the exchange
        if (size > 1) {

            // validate multiple consumers has been enabled
            if (!endpoint.isMultipleConsumersSupported()) {
                throw new IllegalStateException("Multiple consumers for the same endpoint is not allowed: " + endpoint);
            }

            // handover completions, as we need to done this when the multicast is done
            final List<Synchronization> completions = exchange.handoverCompletions();


            // send a new copied exchange with new camel context
            exchange = prepareExchange(exchange);
        }

        // use the regular processor and use the asynchronous routing engine to support it
        AsyncProcessorHelper.process(processor, exchange, new AsyncCallback() {
            public void done(boolean doneSync) {
                // noop
            }
        });
    }

    private class ConsumerEventHandler implements LifecycleAwareExchangeEventHandler {

        private final int ordinal;
        private final int concurrentConsumers;
        private volatile boolean started = false;
        private volatile CountDownLatch startedLatch = new CountDownLatch(1);
        private volatile CountDownLatch stoppedLatch = new CountDownLatch(1);

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
                Exchange exchange = event.getExchange();

                try {
                    process(exchange);
                } catch (Exception e) {
                    if (exchange != null) {
                        getExceptionHandler().handleException("Error processing exchange", exchange, e);
                    } else {
                        getExceptionHandler().handleException(e);
                    }
                }
            }
        }

        @Override
        public void awaitStarted() throws InterruptedException {
            if (!started) {
                startedLatch.await();
            }
        }

        @Override
        public boolean awaitStarted(long timeout, TimeUnit unit) throws InterruptedException {
            return started || startedLatch.await(timeout, unit);
        }

        @Override
        public void awaitStopped() throws InterruptedException {
            if (started) {
                stoppedLatch.await();
            }
        }

        @Override
        public boolean awaitStopped(long timeout, TimeUnit unit) throws InterruptedException {
            return !started || stoppedLatch.await(timeout, unit);
        }

        @Override
        public void onStart() {
            stoppedLatch = new CountDownLatch(1);
            startedLatch.countDown();
            started = true;
        }

        @Override
        public void onShutdown() {
            startedLatch = new CountDownLatch(1);
            stoppedLatch.countDown();
            started = false;
        }
    }
}
