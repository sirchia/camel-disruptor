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

import com.lmax.disruptor.ClaimStrategy;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import com.lmax.disruptor.MultiThreadedLowContentionClaimStrategy;
import com.lmax.disruptor.SingleThreadedClaimStrategy;

/**
 * TODO: documentation
 */
public enum DisruptorClaimStrategy {
    /**
     * Strategy to be used when there are multiple publisher threads claiming sequences.
     *
     * This strategy is reasonably forgiving when the multiple publisher threads are highly contended or working in an
     * environment where there is insufficient CPUs to handle multiple publisher threads.  It requires 2 CAS operations
     * for a single publisher, compared to the {@link com.lmax.disruptor.MultiThreadedLowContentionClaimStrategy} strategy which needs only a single
     * CAS and a lazySet per publication.
     */
    MULTI_THREADED(MultiThreadedClaimStrategy.class),

    /**
     * Strategy to be used when there are multiple publisher threads claiming sequences.
     *
     * This strategy requires sufficient cores to allow multiple publishers to be concurrently claiming sequences and those
     * thread a contented relatively infrequently.
     */
    MULTI_THREADED_LOW_CONTENTION(MultiThreadedLowContentionClaimStrategy.class),

    /**
     * Optimised strategy can be used when there is a single publisher thread claiming sequences.
     *
     * This strategy must <b>not</b> be used when multiple threads are used for publishing concurrently on the same
     * endpoint.
     */
    SINGLE_THREADED(SingleThreadedClaimStrategy.class);

    private final Class<? extends ClaimStrategy> claimStrategyClass;

    private DisruptorClaimStrategy(Class<? extends ClaimStrategy> claimStrategyClass) {

        this.claimStrategyClass = claimStrategyClass;
    }

    public ClaimStrategy createClaimStrategyInstance(int bufferSize) throws Exception {
        return claimStrategyClass.getConstructor(int.class).newInstance(bufferSize);
    }
}
