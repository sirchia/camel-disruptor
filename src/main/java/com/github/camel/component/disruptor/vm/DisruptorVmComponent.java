package com.github.camel.component.disruptor.vm;

import com.github.camel.component.disruptor.DisruptorComponent;
import com.github.camel.component.disruptor.DisruptorReference;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of the <a href="http://camel.apache.org/vm.html">VM components</a>
 * for asynchronous SEDA exchanges on a
 * <a href="https://github.com/LMAX-Exchange/disruptor">LMAX Disruptor</a> within the classloader tree containing
 * the camel-disruptor.jar. i.e. to handle communicating across CamelContext instances and possibly across
 * web application contexts, providing that camel-disruptor.jar is on the system classpath.
 *
 * @version
 */
public class DisruptorVmComponent extends DisruptorComponent {
    protected static final Map<String, DisruptorReference> DISRUPTORS = new HashMap<String, DisruptorReference>();
    private static final AtomicInteger START_COUNTER = new AtomicInteger();

    @Override
    public Map<String, DisruptorReference> getDisruptors() {
        return DISRUPTORS;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        START_COUNTER.incrementAndGet();
    }

    @Override
    protected void doStop() throws Exception {
        if (START_COUNTER.decrementAndGet() <= 0) {
            // clear queues when no more vm components in use
            getDisruptors().clear();
        }
    }
}
