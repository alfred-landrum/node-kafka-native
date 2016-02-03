var _ = require('lodash');
var bluebird_retry = require('bluebird-retry');
var expect = require('chai').expect;
var uuid = require('uuid');
var node_kafka = require('../index');
var Promise = require('bluebird');
var Tmp = require('tmp');

// Tests below assume a kafka broker is running at this address.
var broker = 'localhost:9092';
var default_timeout = 30000;

// Tests below assume auto topic creation is enabled in the broker.
var gen_topic_name = function() {
    return 'jut-node-kafka-test-' + uuid.v4();
}

var default_retry_options = { max_tries: 10, interval: 1000 };
var retry = function(func, retry_options) {
    retry_options = retry_options || default_retry_options;
    return bluebird_retry(func, retry_options);
}

// http://bluebirdjs.com/docs/api/deferred-migration.html
function defer() {
    var resolve, reject;
    var promise = new Promise(function() {
        resolve = arguments[0];
        reject = arguments[1];
    });
    return {
        resolve: resolve,
        reject: reject,
        promise: promise
    };
}

describe('user level api', function() {
    this.timeout(default_timeout);

    it('should create a Consumer', function() {
        var tmpdir = Tmp.dirSync().name;
        var topic = gen_topic_name();

        var consumer = new node_kafka.Consumer({
            broker: broker,
            topic: topic,
            offset_directory: tmpdir,
            receive_callback: function() {
                return Promise.resolve();
            },
        });
        return consumer.start()
        .delay(100).then(function() {
            consumer.pause();
            return Promise.resolve();
        })
        .delay(100).then(function() {
            consumer.resume();
            return Promise.resolve();
        })
        .then(function() {
            return consumer.stop();
        });
    });

    it('should create a Producer', function() {
        var topic = gen_topic_name();

        var producer = new node_kafka.Producer({
            broker: broker,
        });
        return producer.partition_count(topic)
        .then(function(npartitions) {
            expect(npartitions).gt(0);
            for (var i = 0; i < npartitions; ++i) {
                producer.send(topic, i, ['p' + i]);
            }
            return Promise.delay(100);
        })
        .then(function() {
            return producer.stop();
        });
    });

    it('should record offsets processed in offset directory', function() {
        var topic = gen_topic_name();
        var tmpdir = Tmp.dirSync().name;
        var producer = new node_kafka.Producer({
            broker: broker,
        });

        var npartitions;
        function send() {
            for (var i = 0; i < npartitions; ++i) {
                producer.send(topic, i, ['p' + i]);
            }
        }
        // Create a new consumer and wait to receive a messages
        // that should _only_ have the given offset.
        function create_and_verify_consumer(expected_offset) {
            var received = {};
            var signal = defer();
            var consumer = new node_kafka.Consumer({
                broker: broker,
                topic: topic,
                offset_directory: tmpdir,
                receive_callback: function(data) {
                    return Promise.try(function() {
                        expect(data.repeats).equals(0);
                        expect(data.misses).equals(0);
                        _.each(data.messages, function(msg) {
                            expect(msg.topic).equals(topic);
                            expect(msg.payload).equals('p' + msg.partition);
                            if (msg.offset !== expected_offset) {
                                signal.reject(new Error('saw unexpected offset ' + msg.offset));
                            } else {
                                received[msg.partition] = true;
                            }
                        });
                        if (_.keys(received).length === npartitions) {
                            signal.resolve();
                        }
                    });
                },
            });

            return consumer.start()
            .then(function() {
                return signal.promise;
            })
            .finally(function() {
                return consumer.stop();
            });
        }

        return producer.partition_count(topic)
        .then(function(n) {
            npartitions = n;
            send();
            return create_and_verify_consumer(0);
        })
        .then(function() {
            send();
            return create_and_verify_consumer(1);
        });
    });

});
