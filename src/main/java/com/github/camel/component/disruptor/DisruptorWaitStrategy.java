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

import com.lmax.disruptor.*;

/**
 * TODO: documentation
 */
public enum DisruptorWaitStrategy {
    /**
     * Blocking strategy that uses a lock and condition variable for {@link EventProcessor}s waiting on a barrier.
     *
     * This strategy can be used when throughput and low-latency are not as important as CPU resource.
     */
    Blocking(BlockingWaitStrategy.class),

    /**
     * Sleeping strategy that initially spins, then uses a Thread.yield(), and eventually for the minimum number of nanos
     * the OS and JVM will allow while the {@link com.lmax.disruptor.EventProcessor}s are waiting on a barrier.
     *
     * This strategy is a good compromise between performance and CPU resource. Latency spikes can occur after quiet periods.
     */
    Sleeping(SleepingWaitStrategy.class),

    /**
     * Busy Spin strategy that uses a busy spin loop for {@link com.lmax.disruptor.EventProcessor}s waiting on a barrier.
     *
     * This strategy will use CPU resource to avoid syscalls which can introduce latency jitter.  It is best
     * used when threads can be bound to specific CPU cores.
     */
    BusySpin(BusySpinWaitStrategy.class),

    /**
     * Yielding strategy that uses a Thread.yield() for {@link com.lmax.disruptor.EventProcessor}s waiting on a barrier
     * after an initially spinning.
     *
     * This strategy is a good compromise between performance and CPU resource without incurring significant latency spikes.
     */
    Yielding(YieldingWaitStrategy.class);

    private final Class<? extends WaitStrategy> waitStrategyClass;

    private DisruptorWaitStrategy(Class<? extends WaitStrategy> waitStrategyClass) {

        this.waitStrategyClass = waitStrategyClass;
    }

    public WaitStrategy createWaitStrategyInstance() throws Exception {
        return waitStrategyClass.getConstructor().newInstance();
    }
}
