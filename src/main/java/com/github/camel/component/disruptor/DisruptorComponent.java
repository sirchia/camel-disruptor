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

import java.util.Map;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;

/**
 * TODO: documentation
 * Parameters:
 *  - bufferSize: size of the ringbuffer (will be rounded up to nearest power of 2), default 1024
 *  - concurrentConsumers: number of concurrent threads processing exchanges, default 1
 */
public class DisruptorComponent extends DefaultComponent {
    private int defaultBufferSize = 1024;

    private int defaultConcurrentConsumers = 1;

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        int bufferSize = getAndRemoveParameter(parameters, "bufferSize", int.class, defaultBufferSize);
        int concurrentConsumers = getAndRemoveParameter(parameters, "concurrentConsumers", int.class, defaultConcurrentConsumers);

        return new DisruptorEndpoint(uri, this, bufferSize, concurrentConsumers);
    }

    public int getDefaultBufferSize() {
        return defaultBufferSize;
    }

    public void setDefaultBufferSize(int defaultBufferSize) {
        this.defaultBufferSize = defaultBufferSize;
    }

    public int getDefaultConcurrentConsumers() {
        return defaultConcurrentConsumers;
    }

    public void setDefaultConcurrentConsumers(int defaultConcurrentConsumers) {
        this.defaultConcurrentConsumers = defaultConcurrentConsumers;
    }
}
