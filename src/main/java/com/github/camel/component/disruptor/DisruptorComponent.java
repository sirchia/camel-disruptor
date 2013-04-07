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

import java.util.Map;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;

/**
 * TODO: documentation
 * Parameters:
 *  - bufferSize: size of the ringbuffer (will be rounded up to nearest power of 2), default 1024
 *  - concurrentConsumers: number of concurrent threads processing exchanges, default 1
 *  - multipleConsumers: whether multiple consumers or Publish-Subscribe style multicast is supported, default false
 */
public class DisruptorComponent extends DefaultComponent {

    protected final int maxConcurrentConsumers = 500;
    protected int defaultConcurrentConsumers = 1;

    private boolean defaultMultipleConsumers = false;

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        int consumers = getAndRemoveParameter(parameters, "concurrentConsumers", Integer.class, defaultConcurrentConsumers);
        boolean limitConcurrentConsumers = getAndRemoveParameter(parameters, "limitConcurrentConsumers", Boolean.class, true);
        if (limitConcurrentConsumers && consumers >  maxConcurrentConsumers) {
            throw new IllegalArgumentException("The limitConcurrentConsumers flag in set to true. ConcurrentConsumers cannot be set at a value greater than "
                    + maxConcurrentConsumers + " was " + consumers);
        }

        if (consumers < 0) {
            throw new IllegalArgumentException("concurrentConsumers found to be " + consumers +
                    ", must be greater than 0");
        }

        DisruptorEndpoint disruptorEndpoint = new DisruptorEndpoint(uri, this, consumers);
        disruptorEndpoint.configureProperties(parameters);
        return disruptorEndpoint;
    }

    public int getDefaultConcurrentConsumers() {
        return defaultConcurrentConsumers;
    }

    public void setDefaultConcurrentConsumers(int defaultConcurrentConsumers) {
        this.defaultConcurrentConsumers = defaultConcurrentConsumers;
    }

    public boolean isDefaultMultipleConsumers() {
        return defaultMultipleConsumers;
    }

    public void setDefaultMultipleConsumers(boolean defaultMultipleConsumers) {
        this.defaultMultipleConsumers = defaultMultipleConsumers;
    }
}
