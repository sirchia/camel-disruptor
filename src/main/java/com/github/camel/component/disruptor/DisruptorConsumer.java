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
