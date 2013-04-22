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
import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holder for Disruptor references.
 * <p/>
 * This is used to keep track of the usages of the Disruptors, so we know when a Disruptor is no longer in use, and
 * can safely be discarded.
 */
class DisruptorReference {
    private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorReference.class);

    private final Set<DisruptorEndpoint> endpoints = Collections.newSetFromMap(new WeakHashMap<DisruptorEndpoint, Boolean>(4));
    private final DisruptorComponent component;
    private final String uri;

    //TODO ascertain thread safe access to disruptor
    private Disruptor<ExchangeEvent> disruptor;

    private final DelayedExecutor delayedExecutor = new DelayedExecutor();

    private ExecutorService executor;

    private final ProducerType producerType;

    private final int size;

    private final DisruptorWaitStrategy waitStrategy;

    private LifecycleAwareExchangeEventHandler[] handlers = new LifecycleAwareExchangeEventHandler[0];

    private final Queue<Exchange> temporaryExchangeBuffer;

    DisruptorReference(final DisruptorComponent component, final String uri, final int size, final ProducerType producerType, final DisruptorWaitStrategy waitStrategy)
            throws Exception {
        this.component = component;
        this.uri = uri;
        this.size = size;
        this.producerType = producerType;
        this.waitStrategy = waitStrategy;
        temporaryExchangeBuffer = new ArrayBlockingQueue<Exchange>(size);
        reconfigure();
    }

    private void createDisruptor() throws Exception {
        disruptor = new Disruptor<ExchangeEvent>(ExchangeEventFactory.INSTANCE, size, delayedExecutor, producerType,
                waitStrategy.createWaitStrategyInstance());
    }

    public int getEndpointCount() {
        return endpoints.size();
    }

    public boolean hasNullReference() {
        return disruptor == null;
    }

    public void publish(final Exchange exchange) {
        final RingBuffer<ExchangeEvent> ringBuffer = disruptor.getRingBuffer();

        final long sequence = ringBuffer.next();
        ringBuffer.get(sequence).setExchange(exchange);
        ringBuffer.publish(sequence);
    }

    public void reconfigure() throws Exception {
        /*
            TODO handle reconfigure correctly with a full buffer and producers blocking
            Instead of blocking until a fre spot on the ringbuffer is avaiable, they now
            probably get an exception
         */
        shutdown();

        createDisruptor();

        final ArrayList<LifecycleAwareExchangeEventHandler> eventHandlers = new ArrayList<LifecycleAwareExchangeEventHandler>();

        for (final DisruptorEndpoint endpoint : endpoints) {
            final Collection<LifecycleAwareExchangeEventHandler> consumerEventHandlers = endpoint.createConsumerEventHandlers();

            if (consumerEventHandlers != null) {
                eventHandlers.addAll(consumerEventHandlers);
            }
        }

        handleEventsWith(eventHandlers.toArray(new LifecycleAwareExchangeEventHandler[eventHandlers.size()]));

        start();
    }

    private void start() {
        disruptor.start();

        if (executor != null) {
            //and use our delayed executor to really really execute the event handlers now
            delayedExecutor.executeDelayedCommands(executor);
        }

        //make sure all event handlers are correctly started before we continue
        for (final LifecycleAwareExchangeEventHandler handler : handlers) {
            boolean eventHandlerStarted = false;
            while (!eventHandlerStarted) {
                try {
                    //The disruptor start command executed above should have triggered a start signal to all
                    //event processors which, in their death, should notify our event handlers. They respond by
                    //switching a latch and we want to await that latch here to make sure they are started.
                    if (!handler.awaitStarted(10, TimeUnit.SECONDS)) {
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

        //now empty out all buffered Exchange if we had any
        final List<Exchange> exchanges = new ArrayList<Exchange>(temporaryExchangeBuffer.size());
        while (!temporaryExchangeBuffer.isEmpty()) {
            exchanges.add(temporaryExchangeBuffer.remove());
        }
        //and offer them again to our new ringbuffer
        for (final Exchange exchange : exchanges) {
            publish(exchange);
        }
    }

    private void shutdown() {
        if (disruptor != null) {
            //check if we had a blocking event handler to keep an empty disruptor 'busy'
            if (handlers != null && handlers.length == 1 && handlers[0] instanceof BlockingExchangeEventHandler) {
                //yes we did, unblock it so we can get rid of our backlog empty its pending exchanged in our temporary buffer
                final BlockingExchangeEventHandler blockingExchangeEventHandler = (BlockingExchangeEventHandler) handlers[0];
                blockingExchangeEventHandler.unblock();
            }

            disruptor.shutdown();

            //they have already been given a trigger to halt when they are done by shutting down the disruptor
            //we do however want to await their completion before they are scheduled to process events from the new
            for (final LifecycleAwareExchangeEventHandler eventHandler : handlers) {
                boolean eventHandlerFinished = false;
                //the disruptor is now empty and all consumers are either done or busy processing their last exchange
                while (!eventHandlerFinished) {
                    try {
                        //The disruptor shutdown command executed above should have triggered a halt signal to all
                        //event processors which, in their death, should notify our event handlers. They respond by
                        //switching a latch and we want to await that latch here to make sure they are done.
                        if (!eventHandler.awaitStopped(10, TimeUnit.SECONDS)) {
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

            handlers = new LifecycleAwareExchangeEventHandler[0];

            disruptor = null;
        }

        if (executor != null) {
            component.getCamelContext().getExecutorServiceManager().shutdown(executor);
            executor = null;
        }
    }

    private void handleEventsWith(final LifecycleAwareExchangeEventHandler[] newHandlers) {
        if (newHandlers == null || newHandlers.length == 0) {
            handlers = new LifecycleAwareExchangeEventHandler[1];
            handlers[0] = new BlockingExchangeEventHandler();
        } else {
            handlers = newHandlers;
        }
        resizeThreadPoolExecutor(handlers.length);
        disruptor.handleEventsWith(handlers);
    }

    public long remainingCapacity() {
        return disruptor.getRingBuffer().remainingCapacity();
    }

    public DisruptorWaitStrategy getWaitStrategy() {
        return waitStrategy;
    }

    ProducerType getProducerType() {
        return producerType;
    }

    public int getBufferSize() {
        return disruptor.getRingBuffer().getBufferSize();
    }

    public void addEndpoint(final DisruptorEndpoint disruptorEndpoint) {
        endpoints.add(disruptorEndpoint);
    }

    public void removeEndpoint(final DisruptorEndpoint disruptorEndpoint) {
        if (getEndpointCount() == 1) {
            this.shutdown();
        }
        endpoints.remove(disruptorEndpoint);
    }

    public int getPendingExchangeSize() {
        if (disruptor != null) {
            return (int) (getBufferSize() - remainingCapacity() + temporaryExchangeBuffer.size());
        }
        return temporaryExchangeBuffer.size();
    }

    private void resizeThreadPoolExecutor(final int newSize) {
        if (executor == null && newSize > 0) {
            //no thread pool executor yet, create a new one
            executor = component.getCamelContext().getExecutorServiceManager().newFixedThreadPool(this, uri,
                    newSize);
        } else if (executor != null && newSize <= 0) {
            //we need to shut down our executor
            component.getCamelContext().getExecutorServiceManager().shutdown(executor);
            executor = null;
        } else if (executor instanceof ThreadPoolExecutor) {
            //our thread pool executor is of type ThreadPoolExecutor, we know how to resize it
            final ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
            threadPoolExecutor.setCorePoolSize(newSize);
            threadPoolExecutor.setMaximumPoolSize(newSize);
        } else if (newSize > 0) {
            //hmmm...no idea what kind of executor this is...just kill it and start fresh
            component.getCamelContext().getExecutorServiceManager().shutdown(executor);

            executor = component.getCamelContext().getExecutorServiceManager().newFixedThreadPool(this, uri,
                    newSize);
        }
    }

    /**
     * Implementation of the {@link LifecycleAwareExchangeEventHandler} interface that blocks all calls to the #onEvent
     * method until the #unblock method is called.
     */
    private class BlockingExchangeEventHandler extends AbstractLifecycleAwareExchangeEventHandler {

        private final CountDownLatch blockingLatch = new CountDownLatch(1);
        @Override
        public void onEvent(final ExchangeEvent event, final long sequence, final boolean endOfBatch) throws Exception {
            blockingLatch.await();
            final Exchange exchange = event.getExchange();

            if (exchange.getProperty(DisruptorEndpoint.DISRUPTOR_IGNORE_EXCHANGE, false, boolean.class)) {
                // Property was set and it was set to true, so don't process Exchange.
                LOGGER.trace("Ignoring exchange {}", exchange);
            } else {
                temporaryExchangeBuffer.offer(exchange);
            }
        }

        public void unblock() {
            blockingLatch.countDown();
        }

    }

    /**
     * When a consumer is added or removed, we need to create a new Disruptor due to its static configuration. However, we
     * would like to reuse our thread pool executor and only add or remove the threads we need. On a reconfiguraion of the
     * Disruptor, we need to atomically swap the current RingBuffer with a new and fully configured one in order to keep
     * the producers operational without the risk of losing messages. Configuration of a RingBuffer by the Disruptor's
     * start method has a side effect that immediately starts execution of the event processors (consumers) on the
     * Executor passed as a constructor argument which is stored in a final field. In order to be able to delay actual
     * execution of the event processors until the event processors of the previous RingBuffer are done processing and the
     * thread pool executor has been resized to match the new consumer count, we delay their execution using this class.
     */
    private static class DelayedExecutor implements Executor {

        private final Queue<Runnable> delayedCommands = new LinkedList<Runnable>();

        @Override
        public void execute(final Runnable command) {
            delayedCommands.offer(command);
        }

        public void executeDelayedCommands(final Executor actualExecutor) {
            Runnable command;

            while ((command = delayedCommands.poll()) != null) {
                actualExecutor.execute(command);
            }
        }
    }
}
