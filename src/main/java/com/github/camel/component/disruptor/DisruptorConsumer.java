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
import org.apache.camel.*;
import org.apache.camel.impl.LoggingExceptionHandler;
import org.apache.camel.spi.ExceptionHandler;
import org.apache.camel.spi.ShutdownAware;
import org.apache.camel.support.ServiceSupport;
import org.apache.camel.util.AsyncProcessorConverterHelper;
import org.apache.camel.util.AsyncProcessorHelper;
import org.apache.camel.util.ExchangeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Consumer for the Disruptor component.
 */
public class DisruptorConsumer extends ServiceSupport implements Consumer, SuspendableService, ShutdownAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorConsumer.class);

    private final DisruptorEndpoint endpoint;
    private final AsyncProcessor processor;
    private final AsyncCallback noopAsyncCallback = new NoopAsyncCallback();

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

    Set<LifecycleAwareExchangeEventHandler> createEventHandlers(int concurrentConsumers) {
        Set<LifecycleAwareExchangeEventHandler> eventHandlers = new HashSet<LifecycleAwareExchangeEventHandler>();

        for (int i = 0; i < concurrentConsumers; ++i) {
            eventHandlers.add(new ConsumerEventHandler(i, concurrentConsumers));
        }

        return eventHandlers;
    }

    @Override
    public boolean deferShutdown(ShutdownRunningTask shutdownRunningTask) {
        // deny stopping on shutdown as we want disruptor consumers to run in case some other queues
        // depend on this consumer to run, so it can complete its exchanges
        return true;
    }

    @Override
    public void prepareShutdown(boolean forced) {
        // nothing
    }

    @Override
    public int getPendingExchangesSize() {
        return getEndpoint().getDisruptor().getPendingExchangeSize();
    }

    @Override
    public String toString() {
        return "DisruptorConsumer[" + endpoint + "]";
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

            // send a new copied exchange with new camel context
            exchange = prepareExchange(exchange);
        } else {
            //we don't need to copy the exchange (as only one consumer will process it
            //but we do set the FromEndpoint to remain compatible with the behaviour of the SEDA endpoint
            exchange.setFromEndpoint(endpoint);

            //also clear the FromRouteId to have the unit of work processor set it to our route id
            //as would have happened when we created a new exchange
            exchange.setFromRouteId(null);
        }

        // use the regular processor and use the asynchronous routing engine to support it
        AsyncProcessorHelper.process(processor, exchange, noopAsyncCallback);
    }

    /**
     * Implementation of the {@link LifecycleAwareExchangeEventHandler} interface that passes all Exchanges to the
     * {@link Processor} registered at this {@link DisruptorConsumer}.
     */
    private class ConsumerEventHandler extends AbstractLifecycleAwareExchangeEventHandler {

        private final int ordinal;

        private final int concurrentConsumers;
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
                boolean ignore = exchange.getProperty(DisruptorEndpoint.DISRUPTOR_IGNORE_EXCHANGE, false, boolean.class);
                if (ignore) {
                    // Property was set and it was set to true, so don't process Exchange.
                    LOGGER.trace("Ignoring exchange {}", exchange);
                    return;
                }

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

    }

    private static class NoopAsyncCallback implements AsyncCallback {
        public void done(boolean doneSync) {
            // noop
        }
    }
}
