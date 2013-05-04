Disruptor Component
---
The **disruptor:** component provides asynchronous [SEDA](http://www.eecs.harvard.edu/~mdw/proj/seda/) behavior much as the standard SEDA Component, but utilizes a
[Disruptor](https://github.com/LMAX-Exchange/disruptor) instead of a
[BlockingQueue](http://docs.oracle.com/javase/1.5.0/docs/api/java/util/concurrent/BlockingQueue.html) utilized by the
standard [SEDA Component](http://camel.apache.org/seda.html).

As with the SEDA component, queues are only visible within a *single*
[CamelContext](http://camel.apache.org/camelcontext.html) and no support is provided for persistence or recovery.

The main advantage of choosing to use the Disruptor Component over the SEDA Component is performance in use cases where
there is high contention between producer(s) and/or multicasted or concurrent Consumers. In those cases, significant
increases of throughput and reduction of latency has been observed. Performance in scenarios without contention is
comparable to the SEDA Component.

The Disruptor is implemented with the intention of mimicing the behaviour and options of the SEDA Component as much as
possible. The main differences with the SEDA Component are the following:
* The buffer used is always bounded in size (default 1024 exchanges).
* As a the buffer is always bouded, the default behaviour for the Disruptor is to block while the buffer is full
instead of throwing an exception. This default behaviour may be configured on the component (see options).
* The Disruptor requires its consumers (multicasted or otherwise) to be statically configured. Adding or removing
consumers on the fly requires complete flushing of all pending exchanges in the Disruptor.
* As a result of the reconfiguration: Data sent over a Disruptor is directly processed and 'gone' if there is at least
one consumer, late joiners only get new exchanges published after they've joined.
* The *pollTimeout* option is not supported by the Disruptor Component.
* When a producer blocks on a full Disruptor, it does not respond to thread interrupts.

URI format
---
    disruptor:someName[?options]
Where **someName** can be any string that uniquely identifies the endpoint within the current
[CamelContext](http://camel.apache.org/camelcontext.html).
You can append query options to the URI in the following format:

    ?option=value&option=value&…

Options
---
<table>
<tr><td>Name</td><td>Default</td><td>Description</td></tr>
<tr>
<td>size</td>
<td>1024</td>
<td>The maximum capacity of the Disruptors ringbuffer. Will be effectively increased to the nearest power of two.
**Notice:** Mind if you use this option, then its the first endpoint being created with the queue name, that determines
the size. To make sure all endpoints use same size, then configure the size option on all of them, or the first endpoint
being created. </td>
</tr>

<tr>
<td>bufferSize</td>
<td></td>
<td>**Component only:** The maximum default size (capacity of the number of messages it can hold) of the Disruptors
ringbuffer. This option is used if size is not in use.</td>
</tr>

<tr>
<td>queueSize</td>
<td></td>
<td>**Component only:** Additional option to specify the *bufferSize* to maintain maximum compatibility with the SEDA
Component.</td>
</tr>

<tr>
<td>concurrentConsumers</td>
<td>1</td>
<td>Number of concurrent threads processing exchanges.</td>
</tr>

<tr>
<td>waitForTaskToComplete</td>
<td>IfReplyExpected</td>
<td>Option to specify whether the caller should wait for the async task to complete or not before continuing.
The following three options are supported: *Always*, *Never* or *IfReplyExpected*. The first two values are self-explanatory.
The last value, *IfReplyExpected*, will only wait if the message is
[Request Reply](http://camel.apache.org/request-reply.html) based. See more information about
[Async](http://camel.apache.org/async.html) messaging.</td>
</tr>

<tr>
<td>timeout</td>
<td>30000</td>
<td>Timeout (in milliseconds) before a producer will stop waiting for an asynchronous task to complete. See
*waitForTaskToComplete* and [Async](http://camel.apache.org/async.html) for more details. You can disable timeout by
using 0 or a negative value.</td>
</tr>

<tr>
<td>multipleConsumers</td>
<td>false</td>
<td>Specifies whether multiple consumers are allowed. If enabled, you can use Disruptor for
[Publish-Subscribe](http://en.wikipedia.org/wiki/Publish–subscribe_pattern) messaging. That is, you can send a message
to the Disruptor and have each consumer receive a copy of the message. When enabled, this option should be specified on
every consumer endpoint.</td>
</tr>

<tr>
<td>defaultMultipleConsumers</td>
<td></td>
<td>**Component only:** Allows to set the default allowance of multiple consumers for endpoints created by this comonent
used when *multipleConsumers* is not provided.</td>
</tr>

<tr>
<td>limitConcurrentConsumers</td>
<td>true</td>
<td>Whether to limit the number of concurrentConsumers to the maximum of 500. By default, an exception will be thrown if
a Disruptor endpoint is configured with a greater number. You can disable that check by turning this option off.</td>
</tr>

<tr>
<td>blockWhenFull</td>
<td>true</td>
<td>Whether a thread that sends messages to a full Disruptor will block until the ringbuffer's capacity is no longer
exhausted. By default, the calling thread will block and wait until the message can be accepted. By disabling this
option, an exception will be thrown stating that the queue is full.</td>
</tr>

<tr>
<td>defaultBlockWhenFull</td>
<td></td>
<td>**Component only:** Allows to set the default producer behaviour when the ringbuffer is full for endpoints created
by this comonent used when *blockWhenFull* is not provided.</td>
</tr>

<tr>
<td>waitStrategy</td>
<td>Blocking</td>
<td>Defines the strategy used by consumer threads to wait on new exchanges to be published. The options allowed are:
*Blocking*, *Sleeping*, *BusySpin* and *Yielding*. Refer to the section below for more information on this subject</td>
</tr>

<tr>
<td>defaultWaitStrategy</td>
<td></td>
<td>**Component only:** Allows to set the default wait strategy for endpoints created by this comonent used when
*waitStrategy* is not provided.</td>
</tr>

<tr>
<td>producerType</td>
<td>Multi</td>
<td>Defines the producers allowed on the Disruptor. The options allowed are: *Multi* to allow multiple producers and
*Single* to enable certain optimizations only allowed when one concurrent producer (on one thread or otherwise
synchronized) is active.</td>
</tr>

<tr>
<td>defaultProducerType</td>
<td></td>
<td>**Component only:** Allows to set the default producer type for endpoints created by this comonent used when
*producerType* is not provided.</td>
</tr>

</table>

Wait strategies
---
TODO

Use of Request Reply
---
TODO

Concurrent consumers
---
TODO

Thread pools
---
TODO

Sample
---
TODO

Using multipleConsumers
---
TODO

