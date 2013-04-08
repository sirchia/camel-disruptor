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
import com.lmax.disruptor.dsl.ProducerType;
import java.util.*;
import java.util.concurrent.*;
import org.apache.camel.*;
import org.apache.camel.api.management.ManagedAttribute;
import org.apache.camel.impl.DefaultEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: documentation
 */
public class DisruptorEndpoint extends DefaultEndpoint implements MultipleConsumersSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorEndpoint.class);

    private volatile RingBuffer<ExchangeEvent> activeRingBuffer;

    private final DelayedExecutor delayedExecutor = new DelayedExecutor();

    private final String endpointUri;
    private int bufferSize = 1024;
    private int concurrentConsumers;
    private boolean multipleConsumers;
    private ProducerType producerType = ProducerType.MULTI;
    private DisruptorWaitStrategy waitStrategy = DisruptorWaitStrategy.BLOCKING;
    private long timeout = 30000;
    private WaitForTaskToComplete waitForTaskToComplete = WaitForTaskToComplete.IfReplyExpected;

    private final Set<DisruptorProducer> producers = new CopyOnWriteArraySet<DisruptorProducer>();
    private final Set<DisruptorConsumer> consumers = new CopyOnWriteArraySet<DisruptorConsumer>();

    //access to the following fields have synchronized access all guarded by 'this'
    private ExecutorService executor;
    private Disruptor<ExchangeEvent> disruptor;
    private Set<LifecycleAwareExchangeEventHandler> activeEventHandlers;

    public DisruptorEndpoint(String endpointUri, DisruptorComponent component, int concurrentConsumers) {
        super(endpointUri, component);
        this.endpointUri = endpointUri;
        this.concurrentConsumers = concurrentConsumers;
    }

    @ManagedAttribute(description = "Buffer max capacity (rounded up to next power of 2)")
    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int size) {
        this.bufferSize = size;
    }


    public void setConcurrentConsumers(int concurrentConsumers) {
        this.concurrentConsumers = concurrentConsumers;
    }

    @ManagedAttribute(description = "Number of concurrent consumers")
    public int getConcurrentConsumers() {
        return concurrentConsumers;
    }

    public WaitForTaskToComplete getWaitForTaskToComplete() {
        return waitForTaskToComplete;
    }

    public void setWaitForTaskToComplete(WaitForTaskToComplete waitForTaskToComplete) {
        this.waitForTaskToComplete = waitForTaskToComplete;
    }

    @ManagedAttribute()
    public ProducerType getProducerType() {
        return producerType;
    }

    public void setProducerType(ProducerType producerType) {
        this.producerType = producerType;
    }

    @ManagedAttribute(description = "Disruptor wait strategy used by consumers")
    public DisruptorWaitStrategy getWaitStrategy() {
        return waitStrategy;
    }

    public void setWaitStrategy(DisruptorWaitStrategy waitStrategy) {
        this.waitStrategy = waitStrategy;
    }

    @ManagedAttribute
    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @ManagedAttribute
    public boolean isMultipleConsumers() {
        return multipleConsumers;
    }

    public void setMultipleConsumers(boolean multipleConsumers) {
        this.multipleConsumers = multipleConsumers;
    }

    /**
     * Returns the current active consumers on this endpoint
     */
    public Set<DisruptorConsumer> getConsumers() {
        return Collections.unmodifiableSet(consumers);
    }

    /**
     * Returns the current active producers on this endpoint
     */
    public Set<DisruptorProducer> getProducers() {
        return Collections.unmodifiableSet(producers);
    }

    @Override
    @ManagedAttribute
    public boolean isMultipleConsumersSupported() {
        return isMultipleConsumers();
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public Producer createProducer() throws Exception {
        return new DisruptorProducer(this, waitForTaskToComplete, timeout);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        return new DisruptorConsumer(this, processor);
    }

    @Override
    protected void doStop() throws Exception {
        synchronized (this) {
            stopDisruptorAndAwaitActiveConsumers();
        }

        super.doStop();
    }

    @Override
    public DisruptorComponent getComponent() {
        return (DisruptorComponent)super.getComponent();
    }

    void onStarted(DisruptorConsumer consumer) throws Exception {
        synchronized (this) {
            if (consumers.add(consumer)) {
                LOGGER.debug("Starting consumer {} on endpoint {}", consumer, endpointUri);

                reconfigureDisruptor();
            } else {
                LOGGER.debug("Tried to start Consumer {} on endpoint {} but it was already started", consumer,
                        endpointUri);
            }
        }

    }


    void onStopped(DisruptorConsumer consumer) throws Exception {
        synchronized (this) {

            if (consumers.remove(consumer)) {
                LOGGER.debug("Stopping consumer {} on endpoint {}", consumer, endpointUri);

                reconfigureDisruptor();
            } else {
                LOGGER.debug("Tried to stop Consumer {} on endpoint {} but it was already stopped", consumer,
                        endpointUri);
            }


        }
    }

    void onStarted(DisruptorProducer producer) {
        producers.add(producer);
    }

    void onStopped(DisruptorProducer producer) {
        producers.remove(producer);
    }

    /**
     * This method awaits completion of previously active consumers, and reconfigures and starts a new disruptor with
     * the currently active consumers.
     *
     * @throws Exception
     */
    private void reconfigureDisruptor() throws Exception {
        stopDisruptorAndAwaitActiveConsumers();

        disruptor = newInitializedDisruptor();

        //everything is now done and we are ready to start our new eventhandlers
        //start by resizing our executor to match the new state
        resizeThreadPoolExecutor(activeEventHandlers.size());

        if (executor != null) {
            //and use our delayed executor to really really execute the event handlers now
            delayedExecutor.executeDelayedCommands(executor);
        }

        //make sure all event handlers are correctly started before we continue
        for (LifecycleAwareExchangeEventHandler eventHandler : activeEventHandlers) {
            boolean eventHandlerStarted = false;
            while (!eventHandlerStarted) {
                try {
                    //The disruptor start command executed above should have triggered a start signal to all
                    //event processors which, in their death, should notify our event handlers. They respond by
                    //switching a latch and we want to await that latch here to make sure they are started.
                    if (!eventHandler.awaitStarted(10, TimeUnit.SECONDS)) {
                        //we wait for a relatively long, but limited amount of time to prevent an application using
                        //this component from hanging indefinitely
                        //Please report a bug if you can repruduce this
                        LOGGER.error("Disruptor/event handler failed to start properly, PLEASE REPORT");
                    }
                    eventHandlerStarted = true;
                } catch (InterruptedException e) {
                    //just retry
                }
            }
        }

        //Done! let her rip!
    }

    private Disruptor<ExchangeEvent> newInitializedDisruptor() throws Exception {

        Disruptor<ExchangeEvent> newDisruptor = new Disruptor<ExchangeEvent>(ExchangeEventFactory.INSTANCE,
                bufferSize, delayedExecutor, producerType,
                waitStrategy.createWaitStrategyInstance());

        activeEventHandlers = new HashSet<LifecycleAwareExchangeEventHandler>();
        //create eventhandlers for each consumer and add them to the new set of registeredEventHandlers
        for (DisruptorConsumer consumer : consumers) {
            activeEventHandlers.addAll(consumer.createEventHandlers(concurrentConsumers));
        }

        //register the event handlers with the Disruptor
        newDisruptor.handleEventsWith(new ArrayList<LifecycleAwareExchangeEventHandler>(activeEventHandlers)
                .toArray(new LifecycleAwareExchangeEventHandler[activeEventHandlers.size()]));

        //call start on the disruptor to finalize its initialization and
        //atomically swap the new ringbuffer with the old one to prevent producers from adding more exchanges to it.
        activeRingBuffer = newDisruptor.start();

        return newDisruptor;
    }

    private void stopDisruptorAndAwaitActiveConsumers() {
        //Now shutdown the currently active Disruptor and stop all consumers registered there.
        if (disruptor != null) {
            disruptor.shutdown();

            //they have already been given a trigger to halt when they are done by shutting down the disruptor
            //we do however want to await their completion before they are scheduled to process events from the new
            for (LifecycleAwareExchangeEventHandler eventHandler : activeEventHandlers) {
                boolean eventHandlerFinished = false;
                //the disruptor is now empty and all consumers are either done or busy processing their last exchange
                while (!eventHandlerFinished) {
                    try {
                        //The disruptor shutdown command executed above should have triggered a halt signal to all
                        //event processors which, in their death, should notify our event handlers. They respond by
                        //switching a latch and we want to await that latch here to make sure they are done.
                        if (!eventHandler.awaitStopped(105, TimeUnit.SECONDS)) {
                            //we wait for a relatively long, but limited amount of time to prevent an application using
                            //this component from hanging indefinitely
                            //Please report a bug if you can repruduce this
                            LOGGER.error("Disruptor/event handler failed to shut down properly, PLEASE REPORT");
                        }
                        eventHandlerFinished = true;
                    } catch (InterruptedException e) {
                        //just retry
                    }
                }
            }

            disruptor = null;
        }
    }

    /**
     * Called by DisruptorProducers to publish new exchanges on the RingBuffer
     *
     * @param exchange
     */
    public void publish(Exchange exchange) {
        RingBuffer<ExchangeEvent> ringBuffer = activeRingBuffer;

        long sequence = ringBuffer.next();
        ringBuffer.getPreallocated(sequence).setExchange(exchange);
        ringBuffer.publish(sequence);
    }

    private void resizeThreadPoolExecutor(int newSize) {
        if (executor == null && newSize > 0) {
            //no thread pool executor yet, create a new one
            executor = getCamelContext().getExecutorServiceManager().newFixedThreadPool(this, endpointUri,
                    newSize);
        } else if (executor != null && newSize <= 0) {
            //we need to shut down our executor
            getCamelContext().getExecutorServiceManager().shutdown(executor);
            executor = null;
        } else if (executor instanceof ThreadPoolExecutor) {
            //our thread pool executor is of type ThreadPoolExecutor, we know how to resize it
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
            threadPoolExecutor.setCorePoolSize(newSize);
            threadPoolExecutor.setMaximumPoolSize(newSize);
        } else {
            //hmmm...no idea what kind of executor this is...just kill it and start fresh
            getCamelContext().getExecutorServiceManager().shutdown(executor);

            executor = getCamelContext().getExecutorServiceManager().newFixedThreadPool(this, endpointUri,
                    newSize);
        }
    }


    /**
     * When a consumer is added or removed, we need to create a new Disruptor due to its static configuration.
     * However, we would like to reuse our thread pool executor and only add or remove the threads we need.
     *
     * On a reconfiguration of the Disruptor, we need to atomically swap the current RingBuffer with a new and fully
     * configured one in order to keep the producers operational without the risk of losing messages.
     *
     * Configuration of a RingBuffer by the Disruptor's start method has a side effect that immediately starts execution
     * of the event processors (consumers) on the Executor passed as a constructor argument which is stored in a
     * final field. In order to be able to delay actual execution of the event processors until the event processors of
     * the previous RingBuffer are done processing and the thread pool executor has been resized to match the new
     * consumer count, we delay their execution using this class.
     */
    private static class DelayedExecutor implements Executor {

        private static Queue<Runnable> delayedCommands = new LinkedList<Runnable>();

        @Override
        public void execute(Runnable command) {
            delayedCommands.offer(command);
        }

        public void executeDelayedCommands(Executor actualExecutor) {
            Runnable command;

            while ((command = delayedCommands.poll()) != null) {
                actualExecutor.execute(command);
            }
        }
    }
}
