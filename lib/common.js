var _ = require('lodash');
var Promise = require('bluebird');
var retry = require('bluebird-retry');
var addon = require('../build/Release/jut-node-kafka');

var Errors = {
    // Errors reported by kafka broker
    '0': 'RD_KAFKA_RESP_ERR_NO_ERROR',
    '1': 'RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE',
    '2': 'RD_KAFKA_RESP_ERR_INVALID_MSG',
    '3': 'RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART',
    '4': 'RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE',
    '5': 'RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE',
    '6': 'RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION',
    '7': 'RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT',
    '8': 'RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE',
    '9': 'RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE',
    '10': 'RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE',
    '11': 'RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH',
    '12': 'RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE',

    // Internal errors to rdkafka
    '-200': 'RD_KAFKA_RESP_ERR__BEGIN',
    '-199': 'RD_KAFKA_RESP_ERR__BAD_MSG',
    '-198': 'RD_KAFKA_RESP_ERR__BAD_COMPRESSION',
    '-197': 'RD_KAFKA_RESP_ERR__DESTROY',
    '-196': 'RD_KAFKA_RESP_ERR__FAIL',
    '-195': 'RD_KAFKA_RESP_ERR__TRANSPORT',
    '-194': 'RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE',
    '-193': 'RD_KAFKA_RESP_ERR__RESOLVE',
    '-192': 'RD_KAFKA_RESP_ERR__MSG_TIMED_OUT',
    '-191': 'RD_KAFKA_RESP_ERR__PARTITION_EOF',
    '-190': 'RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION',
    '-189': 'RD_KAFKA_RESP_ERR__FS',
    '-188': 'RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC',
    '-187': 'RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN',
    '-186': 'RD_KAFKA_RESP_ERR__INVALID_ARG',
    '-185': 'RD_KAFKA_RESP_ERR__TIMED_OUT',
    '-184': 'RD_KAFKA_RESP_ERR__QUEUE_FULL',
    '-183': 'RD_KAFKA_RESP_ERR__ISR_INSUFF',
    '-100': 'RD_KAFKA_RESP_ERR__END',
    '-1': 'RD_KAFKA_RESP_ERR_UNKNOWN',
};

// Partial list of offsets with special meaning in rdkafka/kafka broker api.
var Offsets = {
    RD_KAFKA_OFFSET_BEGINNING: -2,
    RD_KAFKA_OFFSET_END: -1,
    RD_KAFKA_OFFSET_STORED: -1000,
};

// Returns a Promise for JSON-ified results of rdkafka's rdkafka_get_metadata
// function.
// handle is an instance of RawProducer or RawConsumer.
// retry_options is an optional bluebird-retry option object.
function fetch_metadata(handle, topic, retry_options) {
    retry_options = retry_options || { max_tries:15, interval: 1000 };

    function pfn(resolve, reject) {
        handle.get_metadata(topic, function(kerror, metadata) {
            function _reject(errstr) {
                return reject(new Error('fetch_metadata: ' + errstr));
            }

            if (kerror) {
                return _reject('fetch_metadata error ' + kerror.message + ' on topic ' + topic);
            }

            var tinfo = _.find(metadata.topics, {topic: topic});
            if (!tinfo) {
                return _reject('topic ' + topic + ' not found');
            }
            if (tinfo.error) {
                return _reject('topic error ' + tinfo.error + ' on topic ' + topic);
            }

            var errpart = _.find(tinfo.partitions, function(p) { return p.error; });
            if (errpart) {
                return _reject('partition error ' + errpart.error + ' on partition ' + errpart.id + ' topic ' + topic);
            }

            resolve(tinfo);
        });
    }

    return retry(function() { return new Promise(pfn); }, retry_options);
}

// Returns a Promise for the current number of partitions of a topic.
// handle is an instance of RawProducer or RawConsumer.
// retry_options is an optional bluebird-retry option object.
function fetch_partition_count(handle, topic, retry_options) {
    return fetch_metadata(handle, topic, retry_options)
    .then(function(metadata) {
        return metadata.partitions.length;
    });
}

module.exports = {
    Errors: Errors,
    Offsets: Offsets,
    RawConsumer: addon.Consumer,
    RawProducer: addon.Producer,
    fetch_metadata: fetch_metadata,
    fetch_partition_count: fetch_partition_count,
};
