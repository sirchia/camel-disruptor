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

import com.lmax.disruptor.EventHandler;
import java.util.concurrent.ExecutorService;
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
public class DisruptorConsumer extends ServiceSupport implements Consumer, EventHandler<ExchangeEvent> {

    private final DisruptorEndpoint endpoint;
    private final Processor processor;
    private ExecutorService executor;
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
        executor = getEndpoint().getCamelContext().getExecutorServiceManager().newFixedThreadPool(this, getEndpoint().getEndpointUri(), getEndpoint().getConcurrentConsumers());
        getEndpoint().onStarted(this);
    }

    @Override
    protected void doStop() throws Exception {
        getEndpoint().onStopped(this);
        executor.shutdown();
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

    @Override
    public void onEvent(final ExchangeEvent event, long sequence, boolean endOfBatch) throws Exception {
        executor.execute(new Runnable() {
            @Override
            public void run() {
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
        });
    }
}
