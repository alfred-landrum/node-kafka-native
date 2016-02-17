var cluster = require('cluster');
var kafka_native = require('../index');

var num_workers = 2;
var broker = process.env.NODE_KAFKA_NATIVE_BROKER || 'localhost:9092';
var topic = 'example';

function master() {
    var workers = {};

    function fork_worker(slot) {
        var worker = cluster.fork({
            KAFKA_NATIVE_WORKER_COUNT: num_workers,
            KAFKA_NATIVE_WORKER_SLOT: slot,
        });
        workers[worker.id] = { id: worker.id, slot: slot};
    }

    cluster.on('exit', function(worker, code, signal) {
        var slot = workers[worker.id].slot;
        console.log('worker for slot', slot, 'pid', worker.process.pid, 'died, reforking');
        delete workers[worker.id];
        fork_worker(slot);
    });

    for (var i = 0; i < num_workers; ++i) {
        fork_worker(i);
    }

    var producer = new kafka_native.Producer({
        broker: broker
    });

    producer.partition_count(topic)
    .then(function(npartitions) {
        var partition = 0;
        setInterval(function() {
            console.log('producer send to partition', partition);
            producer.send(topic, partition, ['hello']);
            partition = (partition + 1) % npartitions;
        }, 1000);
    });
}

function worker() {
    var num_workers = Number(process.env.KAFKA_NATIVE_WORKER_COUNT);
    var worker_slot = Number(process.env.KAFKA_NATIVE_WORKER_SLOT);

    console.log('worker pid', process.pid, 'slot', worker_slot);

    var consumer = new kafka_native.Consumer({
        broker: broker,
        topic: topic,
        num_workers: num_workers,
        worker_slot: worker_slot,
        offset_directory: './kafka-offsets',
        receive_callback: function(data) {
            data.messages.forEach(function(m) {
                console.log('worker', worker_slot, 'message: ', m.topic, m.partition, m.offset, m.payload);
            });
            return Promise.resolve();
        }
    });
    consumer.start();
}

if (cluster.isMaster) {
    master();
} else {
    worker();
}
