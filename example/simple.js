var jut_node_kafka = require('../index');
var broker = 'localhost:9092';
var topic = 'example';

var producer = new jut_node_kafka.Producer({
    broker: broker
});
var consumer = new jut_node_kafka.Consumer({
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
