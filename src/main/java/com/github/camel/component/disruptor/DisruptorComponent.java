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

import com.lmax.disruptor.dsl.ProducerType;
import java.util.HashMap;
import java.util.Map;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: documentation
 * Parameters:
 * - bufferSize: size of the ringbuffer (will be rounded up to nearest power of 2), default 1024
 * - concurrentConsumers: number of concurrent threads processing exchanges, default 1
 * - multipleConsumers: whether multiple consumers or Publish-Subscribe style multicast is supported, default false
 */
public class DisruptorComponent extends DefaultComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(DisruptorComponent.class);

    public static final int DEFAULT_BUFFER_SIZE = 1024;
    public static final int MAX_CONCURRENT_CONSUMERS = 500;


    private int bufferSize = -1;
    //for SEDA compatibility only
    private int queueSize = -1;

    private int defaultConcurrentConsumers = 1;
    private boolean defaultMultipleConsumers = false;
    private ProducerType defaultProducerType = ProducerType.MULTI;
    private DisruptorWaitStrategy defaultWaitStrategy = DisruptorWaitStrategy.BLOCKING;

    //synchronized access guarded by this
    private final Map<String, DisruptorReference> disruptors = new HashMap<String, DisruptorReference>();

    @Override
    protected Endpoint createEndpoint(final String uri, final String remaining, final Map<String, Object> parameters) throws Exception {
        final int concurrentConsumers = getAndRemoveParameter(parameters, "concurrentConsumers", Integer.class, defaultConcurrentConsumers);
        final boolean limitConcurrentConsumers = getAndRemoveParameter(parameters, "limitConcurrentConsumers", Boolean.class, true);
        if (limitConcurrentConsumers && concurrentConsumers > MAX_CONCURRENT_CONSUMERS) {
            throw new IllegalArgumentException("The limitConcurrentConsumers flag in set to true. ConcurrentConsumers cannot be set at a value greater than "
                    + MAX_CONCURRENT_CONSUMERS + " was " + concurrentConsumers);
        }

        if (concurrentConsumers < 0) {
            throw new IllegalArgumentException("concurrentConsumers found to be " + concurrentConsumers +
                    ", must be greater than 0");
        }

        int size = 0;
        if (parameters.containsKey("size")) {
            size = getAndRemoveParameter(parameters, "size", int.class);
            if (size <= 0) {
                throw new IllegalArgumentException("size found to be " + size + ", must be greater than 0");
            }
        }

        //TODO actually use pollTimeout parameter?
        parameters.remove("pollTimeout");

        final DisruptorWaitStrategy waitStrategy = getAndRemoveParameter(parameters, "waitStrategy", DisruptorWaitStrategy.class, defaultWaitStrategy);

        final ProducerType producerType = getAndRemoveParameter(parameters, "producerType", ProducerType.class, defaultProducerType);

        final boolean multipleConsumers = getAndRemoveParameter(parameters, "multipleConsumers", boolean.class, defaultMultipleConsumers);

        final DisruptorReference disruptorReference = getOrCreateDisruptor(uri, size, producerType, waitStrategy);
        final DisruptorEndpoint disruptorEndpoint = new DisruptorEndpoint(uri, this, disruptorReference, concurrentConsumers, multipleConsumers);
        disruptorEndpoint.configureProperties(parameters);

        return disruptorEndpoint;
    }

    private DisruptorReference getOrCreateDisruptor(final String uri, final int size, final ProducerType producerType, final DisruptorWaitStrategy waitStrategy)
            throws Exception {
        final String key = getDisruptorKey(uri);

        final int sizeToUse;
        if (size > 0) {
            sizeToUse = size;
        } else if (bufferSize > 0) {
            sizeToUse = bufferSize;
        } else if (queueSize > 0) {
            sizeToUse = queueSize;
        } else {
            sizeToUse = DEFAULT_BUFFER_SIZE;
        }
        synchronized (this) {
            DisruptorReference ref = getDisruptors().get(key);
            if (ref == null) {
                ref = new DisruptorReference(this, uri, powerOfTwo(sizeToUse), producerType, waitStrategy);
                getDisruptors().put(key, ref);
            }

            return ref;
        }
    }

    private static int powerOfTwo(int size) {
        size--;
        size |= size >> 1;
        size |= size >> 2;
        size |= size >> 4;
        size |= size >> 8;
        size |= size >> 16;
        size++;
        return size;
    }

    static String getDisruptorKey(String uri) {
        if (uri.contains("?")) {
            // strip parameters
            uri = uri.substring(0, uri.indexOf('?'));
        }
        return uri;
    }

    @Override
    protected void doStop() throws Exception {
        synchronized (this) {
            getDisruptors().clear();
        }
        super.doStop();
    }

    Map<String, DisruptorReference> getDisruptors() {
        return disruptors;
    }

    public int getDefaultConcurrentConsumers() {
        return defaultConcurrentConsumers;
    }

    public void setDefaultConcurrentConsumers(final int defaultConcurrentConsumers) {
        this.defaultConcurrentConsumers = defaultConcurrentConsumers;
    }

    public boolean isDefaultMultipleConsumers() {
        return defaultMultipleConsumers;
    }

    public void setDefaultMultipleConsumers(final boolean defaultMultipleConsumers) {
        this.defaultMultipleConsumers = defaultMultipleConsumers;
    }

    public ProducerType getDefaultProducerType() {
        return defaultProducerType;
    }

    public void setDefaultProducerType(final ProducerType defaultProducerType) {
        this.defaultProducerType = defaultProducerType;
    }

    public DisruptorWaitStrategy getDefaultWaitStrategy() {
        return defaultWaitStrategy;
    }

    public void setDefaultWaitStrategy(final DisruptorWaitStrategy defaultWaitStrategy) {
        this.defaultWaitStrategy = defaultWaitStrategy;
    }

    @Deprecated
    public void setQueueSize(final int size) {
        LOGGER.warn("Using deprecated queueSize parameter for SEDA compatibility, use bufferSize instead");
        queueSize = size;
    }

    @Deprecated
    public int getQueueSize() {
        LOGGER.warn("Using deprecated queueSize parameter for SEDA compatibility, use bufferSize instead");
        return queueSize;
    }

    public void setBufferSize(final int size) {
        bufferSize = size;
    }

    public int getBufferSize() {
        return bufferSize;
    }
}
