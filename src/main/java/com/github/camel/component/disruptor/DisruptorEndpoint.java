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

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import java.util.concurrent.ExecutorService;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;

/**
 * TODO: documentation
 *
 * TODO: MultipleConsumersSupport
 */
public class DisruptorEndpoint extends DefaultEndpoint {

    private final Disruptor<ExchangeEvent> disruptor;
    private final ExecutorService executor;
    private RingBuffer<ExchangeEvent> ringBuffer;
    private DisruptorConsumer disruptorConsumer;
    private final int concurrentConsumers;

    public DisruptorEndpoint(String endpointUri, DisruptorComponent component, int bufferSize, int concurrentConsumers) {
        super(endpointUri, component);
        this.concurrentConsumers = concurrentConsumers;

        executor = getCamelContext().getExecutorServiceManager().newSingleThreadExecutor(this, endpointUri);

        this.disruptor = new Disruptor<ExchangeEvent>(ExchangeEventFactory.INSTANCE, bufferSize, executor);
    }

    @Override
    public Producer createProducer() throws Exception {
        return new DisruptorProducer(this);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        if (disruptorConsumer != null) {
            throw new IllegalStateException("Already created a disruptor consumer, multiple consumers not yet supported");
        }

        disruptorConsumer = new DisruptorConsumer(this, processor);

        return disruptorConsumer;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public DisruptorComponent getComponent() {
        return (DisruptorComponent)super.getComponent();
    }

    void onStarted(DisruptorConsumer consumer) throws Exception {
        if (consumer != disruptorConsumer) {
            throw new IllegalArgumentException("Tries to respond on startup of consumer not created by this endpoint");
        }
        //start the disruptor
        disruptor.handleEventsWith(disruptorConsumer);
        ringBuffer = disruptor.start();
    }

    void onStopped(DisruptorConsumer consumer) throws Exception {
        if (consumer != disruptorConsumer) {
            throw new IllegalArgumentException("Tries to respond on stop of consumer not created by this endpoint");
        }

        //shutdown the disruptor
        disruptor.shutdown();

        //shutdown the thread pool
        getCamelContext().getExecutorServiceManager().shutdownNow(executor);
    }


    public RingBuffer<ExchangeEvent> getRingBuffer() {
        return ringBuffer;
    }

    public int getConcurrentConsumers() {
        return concurrentConsumers;
    }
}
