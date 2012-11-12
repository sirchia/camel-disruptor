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

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.camel.*;
import org.apache.camel.impl.DefaultEndpoint;

/**
 * TODO: documentation
 */
public class DisruptorEndpoint extends DefaultEndpoint implements MultipleConsumersSupport {

    private final AtomicReference<RingBuffer<ExchangeEvent>> activeRingBuffer = new AtomicReference<RingBuffer<ExchangeEvent>>();

    private final DelayedExecutor delayedExecutor = new DelayedExecutor();

    private final String endpointUri;
    private final int bufferSize;
    private final int concurrentConsumers;
    private final boolean multipleConsumers;
    private final DisruptorWaitStrategy waitStrategy;
    private final DisruptorClaimStrategy claimStrategy;

    //access to the following fields are all guarded by 'this'
    private ExecutorService executor;
    private Disruptor<ExchangeEvent> disruptor;
    private Set<LifecycleAwareExchangeEventHandler> activeEventHandlers;
    private final Map<DisruptorConsumer, List<BatchEventProcessor<ExchangeEvent>>> runningBatchEventProcessors =
            new HashMap<DisruptorConsumer, List<BatchEventProcessor<ExchangeEvent>>>();
    private final Set<DisruptorConsumer> startedConsumers = new HashSet<DisruptorConsumer>();

    public DisruptorEndpoint(String endpointUri, DisruptorComponent component, int bufferSize, int concurrentConsumers,
                             boolean multipleConsumers, DisruptorWaitStrategy waitStrategy, DisruptorClaimStrategy claimStrategy) {
        super(endpointUri, component);
        this.endpointUri = endpointUri;
        this.bufferSize = bufferSize;
        this.concurrentConsumers = concurrentConsumers;
        this.multipleConsumers = multipleConsumers;
        this.waitStrategy = waitStrategy;
        this.claimStrategy = claimStrategy;
    }

    @Override
    public boolean isMultipleConsumersSupported() {
        return multipleConsumers;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public Producer createProducer() throws Exception {
        return new DisruptorProducer(this);
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
            if (!startedConsumers.add(consumer)) {
                throw new IllegalStateException("Tried to restart a consumer that was already started");
            }

            reconfigureDisruptor();
        }

    }


    void onStopped(DisruptorConsumer consumer) throws Exception {
        synchronized (this) {

            if (!startedConsumers.remove(consumer)) {
                throw new IllegalStateException("Tried to stop a consumer that was not started at this endpoint");
            }

            reconfigureDisruptor();
        }
    }

    /**
     * This method awaits completion of previously active consumers, and reconfigures and starts a new disruptor with
     * the currently active consumers.
     *
     * @throws Exception
     */
    private void reconfigureDisruptor() throws Exception {
        Set<LifecycleAwareExchangeEventHandler> newEventHandlers = new HashSet<LifecycleAwareExchangeEventHandler>();
        Disruptor<ExchangeEvent> newDisruptor = newInitializedDisruptor(newEventHandlers);

        stopDisruptorAndAwaitActiveConsumers();

        //everything is now done and we are ready to start our new eventhandlers
        //start by resizing our executor to match the new state
        resizeThreadPoolExecutor(startedConsumers.size() * concurrentConsumers);

        //and use our delayed executor to really really execute the event handlers now
        delayedExecutor.executeDelayedCommands(executor);

        //align our administration
        disruptor = newDisruptor;
        activeEventHandlers = newEventHandlers;

        //Done! let her rip!
    }

    private Disruptor<ExchangeEvent> newInitializedDisruptor(
            Set<LifecycleAwareExchangeEventHandler> registeredEventHandlers) throws Exception {
        //TODO ClaimStrategy, WaitStrategy
        Disruptor<ExchangeEvent> newDisruptor = new Disruptor<ExchangeEvent>(ExchangeEventFactory.INSTANCE,
                delayedExecutor, claimStrategy.createClaimStrategyInstance(bufferSize),
                waitStrategy.createWaitStrategyInstance());

        //create eventhandlers for each consumer and add them to the new set of registeredEventHandlers
        for (DisruptorConsumer consumer : startedConsumers) {
            registeredEventHandlers.addAll(consumer.createEventHandlers(concurrentConsumers));
        }

        //register the event handlers with the Disruptor
        newDisruptor.handleEventsWith(new ArrayList<LifecycleAwareExchangeEventHandler>(registeredEventHandlers)
                .toArray(new LifecycleAwareExchangeEventHandler[registeredEventHandlers.size()]));

        //call start on the disruptor to finalize its initialization and
        //atomically swap the new ringbuffer with the old one to prevent producers from adding more exchanges to it.
        activeRingBuffer.set(newDisruptor.start());

        return newDisruptor;
    }

    private void stopDisruptorAndAwaitActiveConsumers() throws InterruptedException {
        //Now shutdown the currently active Disruptor and stop all consumers registered there.
        if (disruptor != null) {
            disruptor.shutdown();

            //the disruptor is now empty and all consumers are either done or busy processing their last exchange
            //they have already been given a trigger to halt when they are done by shutting down the disruptor
            //we do however want to await their completion before they are scheduled to process events from the new
            for (LifecycleAwareExchangeEventHandler eventHandler : activeEventHandlers) {
                eventHandler.await();
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
        RingBuffer<ExchangeEvent> ringBuffer = activeRingBuffer.get();

        long sequence = ringBuffer.next();
        ringBuffer.get(sequence).setExchange(exchange);
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
            executor.shutdown();

            executor = getCamelContext().getExecutorServiceManager().newFixedThreadPool(this, endpointUri,
                    newSize);
        }
    }


    /**
     * When a consumer is added or removed, we need to create a new Disruptor due to its static configuration.
     * However, we would like to reuse our thread pool executor and only add or remove the threads we need.
     *
     * On a reconfiguraion of the Disruptor, we need to atomically swap the current RingBuffer with a new and fully
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
