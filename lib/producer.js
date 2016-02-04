var _ = require('lodash');
var common = require('./common');
var retry = require('bluebird-retry');
var validateOpts = require('validate-options');


function Producer(options) {
    var self = this;
    validateOpts.hasAll(options,'broker');

    self.options = options;
    self.logger = options.logger || console;

    // Allow overriding for testing.
    var RawProducer = options.raw_producer || common.RawProducer;
    self.fetch_partition_count = options.fetch_partition_count || common.fetch_partition_count;

    self.handle = new RawProducer({
        stat_cb: options.stats_callback ? self._rdkafka_stats_callback.bind(self) : null,
        dr_cb: options.delivery_report_callback ? self._rdkafka_dr_callback.bind(self) : null,
        driver_options: _.defaults({}, options.driver_options, {
            // Track only delivery failures for stats
            'delivery.report.only.error': 1,
            'metadata.broker.list': options.broker,
            'statistics.interval.ms': options.statistics_interval_ms || 5000,
            'queue.buffering.max.messages': options.queue_buffering_max_messages || 500000,
            // queue.buffering.max.ms acts as an upper bound on latency
            'queue.buffering.max.ms': options.queue_buffering_max_ms || 250,
        }),
        topic_options: _.defaults({}, options.topic_options),
        log_cb: function(event) {
            self.logger.info('rdkafka:', event.level, event.facility, event.message);
        },
        error_cb: function(error) {
            self.logger.error('rdkafka:', error);
        },
    });
}

Producer.prototype._rdkafka_dr_callback = function(errinfo) {
    this.options.delivery_report_callback({
        errors: _.sumBy(errinfo, function(e) { return e.count; }),
        rdkafka_errinfo: errinfo,
    });
};

Producer.prototype._rdkafka_stats_callback = function(statstr) {
    // calculate sum of pending messages for each topic
    var stats = JSON.parse(statstr);
    var send_queue_length = _.mapValues(stats.topics, function(tinfo) {
        return _.sumBy(tinfo.partitions, function(p) { return p.msgq_cnt; });
    });
    this.options.stats_callback({
        send_queue_length: send_queue_length,
        rdkafka_stats: stats,
    });
}

// Returns number of partitions for this topic.
Producer.prototype.partition_count = function(topic) {
    return this.fetch_partition_count(this.handle, topic)
    .then(function(num_partitions) {
        return num_partitions;
    });
}

// Returns an object with two properties:
// enqueued: number (<= messages.length) of given messages enqueued for transmission to broker
// send_queue_length: current number of outstanding messages awaiting transmission
Producer.prototype.send = function(topic, partition, payloads) {
    var result = this.handle.send(topic, partition, payloads);
    return {
        enqueued: result.enqueued,
        send_queue_length: result.send_queue_length,
    };
}

Producer.prototype.stop = function(drain_wait_options) {
    var self = this;
    drain_wait_options = drain_wait_options || { max_tries:15, interval: 1000 };
    var wait_for_empty_outq = function() {
        var qlen = self.handle.outq_length();
        if (qlen === 0) {
            return Promise.resolve();
        }
        throw new Error('Producer: stop still waiting for empty outq');
    };
    return retry(wait_for_empty_outq, drain_wait_options)
    .then(function() {
        self.handle.stop();
    });
}

module.exports = {
    Producer: Producer,
};
