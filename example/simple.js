var kafka_native = require('../index');
var broker = process.env.NODE_KAFKA_NATIVE_BROKER || 'localhost:9092';
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
        })
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
