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
