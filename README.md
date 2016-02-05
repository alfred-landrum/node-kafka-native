# kafka-native

The kafka-native client provides consume and produce functionality for Kafka, using the [librdkafka](https://github.com/edenhill/librdkafka) native library for performance. Periodic stats on local and kafka queue lengths are provided for monitoring and analysis. Message and offset progress is atomically recorded to local storage. Multi-core consume side processing is possible via node cluster.

## Example
```javascript
var kafka_native = require('kafka-native');
var broker = 'localhost:9092';
var topic = 'example';

var producer = new kafka_native.Producer({
    broker: broker
});

var consumer = new kafka_native.Consumer({
    broker: broker,
    topic: topic,
    offset_directory: './kafka-offsets',
    receive_callback: function(data) {
        data.messages.forEach(function(m) {
            console.log('message: ', m.topic, m.partition, m.offset, m.payload);
        });
        return Promise.resolve();
    }
});

producer.partition_count(topic)
.then(function(npartitions) {
    var partition = 0;
    setInterval(function() {
        producer.send(topic, partition++ % npartitions, ['hello']);
    }, 1000);

    return consumer.start();
});
```

See the `examples` directory for more.

## Installation

Install via npm, which will install dependencies and compile the node addon:

```bash
$ npm install kafka-native
```

### Development and testing

If needed, the script `scripts/test-server.sh` can be used to create a test/dev kafka instance. This uses docker-compose to launch kafka and zookeeper containers. If you're using Mac/Windows, make sure you've run the appropriate `eval $(docker-machine env ...)` command before launching the test-server control script.

```bash
$ # Mac/Windows: eval $(docker-machine env <your-default-machine-name>)
$ eval $(scripts/test-server.sh start)
$ npm test
$ scripts/test-server.sh stop
```

If you already have a running kafka instance, you can export its brokerlist to `NODE_KAFKA_NATIVE_BROKER` before running the tests or examples. Note that the mocha tests will fail if automatic topic creation is not allowed, or if topics are created with just one partition.

### Code coverage
Code coverage report is available by running:

```bash
$ npm test --coverage
```
You can then browse locally to `$REPO/coverage/lcov-report/index.html` to see Istanbul's report.

## Usage
### Producer
Creating a new `Producer` requires only a broker, after which, you can push messages into Kafka via the `producer.send(topic, partition, messages)` call.

To find the number of partitions for a topic, you can use the `producer.partition_count(topic)` call. It's up to the user of `Producer` to determine how it wants to distribute any messages across a topic's partitions. The examples in the `examples` directory all round-robin messages.

#### Statistics
The periodic stats from the Producer report the queue length of messages awaiting transmission via `send_queue_length`. This is also reported from each `send` call. You can cap the maximum number of locally enqueued messages via the `queue_buffering_max_messages` option; attempts to send messages when over this limit will cause the messages to be dropped.

The full librdafka JSON statistics object is returned as `rdkafka_stats`, in case you want to parse out further stats.


### Consumer
The `Consumer` requires a local directory to record processed message offsets, as well as the broker and topic to use. Once a `Consumer` has been created, its `start()` method will initiate pulling messages from Kafka, and feeding them back to the configured `receive_callback`. The `Consumer` will keep trying to pull messages from Kafka in the background, up to the number of kilobytes of memory specified by the `queued_max_messages_kbytes` option, so that it can feed the `receive_callback` as soon as possible.

#### Flow control
When your `receive_callback` returns - or if it returns a Promise, when that Promise is fulfilled _or_ rejected - the message offsets will be recorded locally, to ensure that any future Consumer instances don't ask for that message offset again. This information is recorded as a set of files in the `offset_directory`, one per topic-partition.

Flow control is possible via the `pause()` and `resume()` methods. With these methods, you can easily match the rate of pulling new messages from Kafka with your processing rate:
```javascript
var consumer = new Consumer({
    receive_callback: function(data) {
        consumer.pause();
        return process_messages(data.messages)
        .finally(function() {
            consumer.resume();
        });
    }
});
```

#### Multiple consumers
For Kafka topics with multiple partitions, you may want multiple consumers processing the topic. At `Consumer` initialization time, via the `num_workers` and `worker_slot` options, you may configure the Consumer to work with only a subset of the available partitions. See `examples/node-cluster.js` for a working example that uses the node cluster module to automatically create several consumer processes.

#### Statistics
The `Consumer` will report periodic stats if the `stats_callback` option is supplied:
- `waiting_kafka`: The number of messages that are sitting at the Kafka broker waiting to be pulled.
- `waiting_local`: The number of messages that have been pulled from kafka, and are sitting in local memory, but have not yet been handed to the caller's `receive_callback`.
- `kafka_log_size`: The number of messages sitting in Kafka's log storage for the Consumer's partitions. This number does include messages that have already been pulled and processed, so its mostly useful to ensure your broker message expiration parameters are working as you expect.

Note that the `Consumer` stats report only on their slice of the topic, as configured via the `num_workers` and `worker_slot` options. That is, if you configure multiple Consumers to split the work on a topic, you'll want to sum/aggregate their statistics in your analytics backend to get a full picture of topic processing.

The full librdafka JSON statistics object is returned as `rdkafka_stats`, in case you want to parse out further stats.

### Limitations
Although Kafka can support binary keys and payloads, the consumer will try to stringify any payload received, which won't be useful if you're pushing binary data into a topic. There's currently no way to produce key/payload pairs; the Producer's send call only takes an array of payloads at this time.

The librdkafka version in use doesn't currently support any interaction with Zookeeper, nor is its broker offset commit functionality useful. This is why the `Consumer` manages its offsets locally.

## API
### Producer
`var producer = new Producer(options);`

options:
- broker (string, required) The broker list of your Kafka servers, eg: 'localhost:9092'.
- stats_callback (function) See below.
- delivery_report_callback (function) See below.
- statistics_interval_ms (number, default 5000) Milliseconds between reporting statistics to the stats_callback.
- queue_buffering_max_messages (number, default 500,000) Maximum number of messages allowed on the producer queue.
- queue_buffering_max_ms (number, default 250) Maximum time, in milliseconds, to delay sending a message in the hope that other messages will shortly be sent as well.
- driver_options (object) - overrides for [librdkafka global configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
- topic_options (object) - overrides for [librdkafka topic configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

`stats_callback(info)`

info:
- send_queue_length (number) The number of messages awaiting transmission to kafka broker.
- rdkafka_stats (object) All statistics as reported by librdkafka.

`delivery_report_callback(info)`

info:
- errors (number) count of transmission errors since last delivery report callback.
- rdkafka_errinfo (array of objects) Raw delivery report from librdafka.

`producer.send(topic, partitions, payloads)`
- topic (string) Name of topic that should receive these payloads.
- partition (number) Partition id that should receive these payloads.
- payloads (Array) Array of strings to be pushed.
- returns: (object)
   - enqueued (number) The count of messages from this call successfully enqueued for transmission to kafka.
   - send_queue_length (number) The number of messages awaiting transmission to kafka broker.

`producer.partition_count(topic)`
- topic (string) Name of topic to request count of partitions.
- returns: [Promise](http://bluebirdjs.com/docs/getting-started.html) for the number of partitions in the topic.

`producer.stop(options)`
- options (object, optional) - optional options to [bluebird-retry](https://www.npmjs.com/package/bluebird-retry) to control how long to wait for the outbound message queue to drain before returning.
- returns: [Promise](http://bluebirdjs.com/docs/getting-started.html)
    - resolves when outbound queue of messages to Kafka is drained, with a default timeout of 15 seconds.

### Consumer
`var consumer = new Consumer(options);`

options:
- logger (object) Logging facility; defaults to `console`.
- broker (string, required) The broker list of your Kafka servers, eg: 'localhost:9092'.
- topic (string, required) Name of topic to consume messages from.
- receive_callback (function, required) See below.
- stats_callback (function) See below.
- max_messages_per_callback (number, default 10000) - maximum number of messages passed to a single invocation of the receive_callback.
- statistics_interval_ms (number, default 5000) Milliseconds between reporting statistics to the stats_callback.
- queued_max_messages_kbytes (number, default 1,000,000) Maximum number of kilobytes allowed in the librdkafka consumer queue.
- driver_options (object) - overrides for [librdkafka global configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
- topic_options (object) - overrides for [librdkafka topic configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

`receive_callback(info)`

info:
- repeats (number) If non-zero, signals that a message was received with an offset below what was expected. May indicate that the kafka topic was re-created.
- misses (number) If non-zero, signals that a message was received with a higher offset than expected. This indicates that some messages were missed; ie, the broker deleted them due to the topic's expiration policy before the consumer was able to read them. High possibility that the consumer isn't able to process data as fast as messages are created.
- messages (array of message objects):
    - partition (number) partition of this message.
    - offset (number) offset for this message.
    - payload (string) payload of this message.
    - key (string) key of this message if it was created with one.

`stats_callback(info)`

info:
- `waiting_kafka`: The number of messages that are sitting at the Kafka broker waiting to be pulled.
- `waiting_local`: The number of messages that have been pulled from kafka, and are sitting in local memory, but have not yet been handed to the caller's `receive_callback`.
- `kafka_log_size`: The number of messages sitting in Kafka's log storage for the Consumer's partitions. This number does include messages that have already been pulled and processed, so its mostly useful to ensure your broker message expiration parameters are working as you expect.
- rdkafka_stats (object) All statistics as reported by librdkafka.

`consumer.start()`
- returns: [Promise](http://bluebirdjs.com/docs/getting-started.html) for an array of partitions this consumer instance will pull from. Unless you're using the worker_id/num_workers to divvy work among consumers, this will be an array of all the partitions in the topic.

`consumer.pause()`

Prevents further receive_callback invocations until a consumer.resume() call.

`consumer.resume()`

Re-allows receive_callback invocations after using consumer.pause().

`consumer.stop()`
- returns: [Promise](http://bluebirdjs.com/docs/getting-started.html) that resolves after any pending offset commits have been written to disk.

## License
The wrapper library and addon are licensed for distribution by the MIT License.

The repository also includes a copy of the librdkafka library (from https://github.com/edenhill/librdkafka) which is licensed as specified in deps/librdkafka/LICENSE.
