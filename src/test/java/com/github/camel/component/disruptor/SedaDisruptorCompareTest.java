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

import com.lmax.disruptor.collections.Histogram;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.camel.*;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This class does not perform any functional test, but instead makes a comparison between the performance of the
 * Disruptor and SEDA component in several use cases.
 * <p/>
 * As memory management may have great impact on the results, it is adviced to run this test with a large, fixed heap
 * (e.g. run with -Xmx1024m -Xms1024m JVM parameters)
 */
@Ignore
@RunWith(value = Parameterized.class)
public class SedaDisruptorCompareTest extends CamelTestSupport {

    @Produce
    protected ProducerTemplate producerTemplate;

    private static final int SPEED_TEST_EXCHANGE_COUNT = 80000;
    private static final long[] LATENCY_HISTOGRAM_BOUNDS = new long[]{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000};

    private ExchangeAwaiter[] exchangeAwaiters;
    private final String componentName;
    private final String endpointUri;
    private final int amountProducers;
    private final int amountConsumers;

    @BeforeClass
    public static void legend() {
        System.out.println("-----------------------");
        System.out.println("- Tests output legend -");
        System.out.println("-----------------------");
        System.out.println("P: Number of concurrent Producer(s) sharing the load for publishing exchanges to the disruptor.");
        System.out.println("C: Number of Consumer(s) receiving a copy of each exchange from the disruptor (pub/sub).");
        System.out.println("CCT: Number of ConcurrentConsumerThreads sharing the load for consuming exchanges from the disruptor.");
        System.out.println("Each test is creating " + SPEED_TEST_EXCHANGE_COUNT + " exchanges.");
        System.out.println();
    }

    public SedaDisruptorCompareTest(String componentName, String endpointUri, int amountProducers, int amountConsumers, int concurrentConsumerThreads) {

        this.componentName = componentName + " (" + amountProducers + "P, " + amountConsumers + "C, " + concurrentConsumerThreads + "CCT)";
        this.endpointUri = endpointUri;
        this.amountProducers = amountProducers;
        this.amountConsumers = amountConsumers;
        exchangeAwaiters = new ExchangeAwaiter[amountConsumers];
        for (int i = 0; i < amountConsumers; ++i) {
            exchangeAwaiters[i] = new ExchangeAwaiter(SPEED_TEST_EXCHANGE_COUNT);
        }
    }

    private static int singleProducer() {
        return 1;
    }

    private static int multipleProducers() {
        return 4;
    }

    private static int singleConsumer() {
        return 1;
    }

    private static int multipleConsumers() {
        return 4;
    }

    private static int singleConcurrentConsumerThread() {
        return 1;
    }

    private static int multipleConcurrentConsumerThreads() {
        return 2;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<Object[]>();

        // This parameter set can be compared to the next and shows the impact of a 'long' endpoint name
        // It defines all parameters to the same values as the default, so the result should be the same as
        // 'seda:speedtest'. This shows that disruptor has a slight disadvantage as its name is longer than 'seda' :)
        parameters.add(new Object[]{"SEDA LONG (1P, 1C, 1CCT)", "seda:speedtest?concurrentConsumers=1&waitForTaskToComplete=IfReplyExpected&timeout=30000&multipleConsumers=false&limitConcurrentConsumers=true&blockWhenFull=false", singleProducer(), singleConsumer(),
                singleConcurrentConsumerThread()});
        addParameterPair(parameters, singleProducer(), singleConsumer(), singleConcurrentConsumerThread());
        addParameterPair(parameters, singleProducer(), singleConsumer(), multipleConcurrentConsumerThreads());
        addParameterPair(parameters, singleProducer(), multipleConsumers(), singleConcurrentConsumerThread());
        addParameterPair(parameters, singleProducer(), multipleConsumers(), multipleConcurrentConsumerThreads());
        addParameterPair(parameters, multipleProducers(), singleConsumer(), singleConcurrentConsumerThread());
        addParameterPair(parameters, multipleProducers(), singleConsumer(), multipleConcurrentConsumerThreads());
        addParameterPair(parameters, multipleProducers(), multipleConsumers(), singleConcurrentConsumerThread());
        addParameterPair(parameters, multipleProducers(), multipleConsumers(), multipleConcurrentConsumerThreads());

        return parameters;
    }

    private static void addParameterPair(List<Object[]> parameters, int producers, int consumers, int parallelConsumerThreads) {
        String multipleConsumerOption = (consumers > 1 ? "multipleConsumers=true" : "");
        String concurrentConsumerOptions = (parallelConsumerThreads > 1 ? "concurrentConsumers=" + parallelConsumerThreads : "");

        String options = "";
        if (!multipleConsumerOption.isEmpty() || !concurrentConsumerOptions.isEmpty()) {
            options += "?";
        }

        if (!multipleConsumerOption.isEmpty()) {
            options += multipleConsumerOption;
            if (!concurrentConsumerOptions.isEmpty()) {
                options += "&";
            }
        }
        if (!concurrentConsumerOptions.isEmpty()) {
            options += concurrentConsumerOptions;
        }

        parameters.add(new Object[]{"SEDA", "seda:speedtest" + options, producers, consumers, parallelConsumerThreads});
        parameters.add(new Object[]{"Disruptor", "disruptor:speedtest" + options, producers, consumers, parallelConsumerThreads});
    }

    @Test
    public void speedTestDisruptor() throws InterruptedException {

        System.out.println("Warming up for test of: " + componentName);

        performTest(true);
        System.out.println("Starting real test of: " + componentName);

        forceGC();
        Thread.sleep(1000);

        performTest(false);
    }

    private void forceGC() {
        // unfortunately there is no nice API that forces the Garbage collector to run, but it may consider our request
        // more seriously if we ask it twice :)
        System.gc();
        System.gc();
    }

    private void resetExchangeAwaiters() {
        for (ExchangeAwaiter exchangeAwaiter : exchangeAwaiters) {
            exchangeAwaiter.reset();
        }
    }

    private void awaitExchangeAwaiters() throws InterruptedException {
        for (ExchangeAwaiter exchangeAwaiter : exchangeAwaiters) {
            while (!exchangeAwaiter.awaitMessagesReceived(10, TimeUnit.SECONDS)) {
                System.err.println("Processing takes longer then expected: " + componentName + " " + exchangeAwaiter.getStatus());
            }
        }
    }

    private void outputExchangeAwaitersResult(long start) throws InterruptedException {
        for (ExchangeAwaiter exchangeAwaiter : exchangeAwaiters) {
            long stop = exchangeAwaiter.getCountDownReachedTime();
            Histogram histogram = exchangeAwaiter.getLatencyHistogram();

            System.out.printf("%-45s time spent = %5d ms. Latency (ms): %s %n", componentName, (stop - start), histogram.toString());
        }
    }

    private void performTest(boolean warmup) throws InterruptedException {
        resetExchangeAwaiters();

        ProducerThread[] producerThread = new ProducerThread[amountProducers];
        for (int i = 0; i < producerThread.length; ++i) {
            producerThread[i] = new ProducerThread(SPEED_TEST_EXCHANGE_COUNT / amountProducers);
        }

        long start = System.currentTimeMillis();

        for (int i = 0; i < producerThread.length; ++i) {
            producerThread[i].start();
        }

        awaitExchangeAwaiters();

        if (!warmup) {
            outputExchangeAwaitersResult(start);
        }

    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                for (ExchangeAwaiter exchangeAwaiter : exchangeAwaiters) {
                    from(endpointUri).process(exchangeAwaiter);
                }
            }
        };
    }

    private static final class ExchangeAwaiter implements Processor {

        private CountDownLatch latch;
        private final int count;
        private long countDownReachedTime;

        private Queue<Long> latencyQueue = new ConcurrentLinkedQueue<Long>();

        public ExchangeAwaiter(int count) {
            this.count = count;
        }

        public void reset() {
            latencyQueue = new ConcurrentLinkedQueue<Long>();
            latch = new CountDownLatch(count);
        }

        public boolean awaitMessagesReceived(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        public String getStatus() {
            StringBuilder sb = new StringBuilder(100);
            sb.append("processed ");
            sb.append(count - latch.getCount());
            sb.append('/');
            sb.append(count);
            sb.append(" messages");

            return sb.toString();
        }

        @Override
        public void process(Exchange exchange) throws Exception {
            long sentTimeNs = exchange.getIn().getBody(Long.class);
            latencyQueue.offer(Long.valueOf(System.nanoTime() - sentTimeNs));

            latch.countDown();
            if (latch.getCount() == 0) {
                countDownReachedTime = System.currentTimeMillis();
            }
        }

        public long getCountDownReachedTime() {
            return countDownReachedTime;
        }

        public Histogram getLatencyHistogram() {
            Histogram histogram = new Histogram(LATENCY_HISTOGRAM_BOUNDS);
            for (Long latencyValue : latencyQueue) {
                histogram.addObservation(latencyValue / 1000000);
            }
            return histogram;
        }
    }

    private final class ProducerThread extends Thread {

        private final int totalMessageCount;
        private int producedMessageCount = 0;

        public ProducerThread(int totalMessageCount) {
            super("TestDataProducerThread");
            this.totalMessageCount = totalMessageCount;
        }

        public void run() {
            while (producedMessageCount++ < totalMessageCount) {
                producerTemplate.sendBody(endpointUri, ExchangePattern.InOnly, System.nanoTime());
            }
        }

        public String getStatus() {
            StringBuilder sb = new StringBuilder(100);
            sb.append("produced ");
            sb.append(producedMessageCount - 1);
            sb.append('/');
            sb.append(totalMessageCount);
            sb.append(" messages");

            return sb.toString();
        }
    }
}
