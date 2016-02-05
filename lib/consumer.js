var _ = require('lodash');
var assert = require('assert');
var common = require('./common');
var validateOpts = require('validate-options');
var OffsetDirectory = require('./offsetdirectory');
var Promise = require('bluebird');


function Consumer(options) {
    var self = this;
    validateOpts.hasAll(options,
        'broker','topic','receive_callback');
    self.options = options;
    self.logger = options.logger || console;

    // Worker info: if used, then 0 <= worker_slot < num_workers must hold,
    // and each worker will calculate a fair share of partitions to consume.
    self.worker_slot = options.worker_slot || 0;
    self.num_workers = options.num_workers || 1;
    assert(self.worker_slot >= 0);
    assert(self.worker_slot < self.num_workers);

    // partition -> next offset we expect to receive
    self.expected_offsets = {};

    // partition -> list of {offset,done} objects, tracks offsets
    // currently in use by options.receive_callback.
    self.receiving_offsets = {};

    // Allow overriding for testing.
    var OffsetManager = options.offset_manager || OffsetDirectory;
    var RawConsumer = options.raw_consumer || common.RawConsumer;
    self.fetch_partition_count = options.fetch_partition_count || common.fetch_partition_count;

    self.offset_mgr = new OffsetManager(options);
    this.handle = new RawConsumer({
        topic: options.topic,
        recv_cb: this._rdkakfa_recv_callback.bind(this),
        max_messages_per_callback: options.max_messages_per_callback || 10000,
        // see rdkafka_defaultconf.c for all driver ('global') and topic options
        driver_options: _.defaults({}, options.driver_options, {
            'metadata.broker.list': options.broker,
            'statistics.interval.ms': options.statistics_interval_ms || 5000,
            'queued.max.messages.kbytes': options.queued_max_messages_kbytes || 1000000,
        }),
        topic_options: _.defaults({}, options.topic_options, {
            'auto.commit.enable': false,
            'auto.offset.reset': 'smallest',
        }),
        stat_cb: options.stats_callback ? self._rdkafka_stats_callback.bind(self) : null,
        log_cb: function(event) {
            self.logger.info('rdkafka:', event.level, event.facility, event.message);
        },
        error_cb: function(error) {
            self.logger.error('rdkafka:', error);
        },
    });
}

// Returns array of this worker_slot's partitions
Consumer.prototype._my_partitions = function(num_partitions) {
    var self = this;
    var assignments = _.map(_.range(self.num_workers), function() { return []; });
    var next = -1;
    _.each(_.range(num_partitions), function(i) {
        next = (next + 1) % self.num_workers;
        assignments[next].push(i);
    });
    return assignments[self.worker_slot];
};

// Returns array of partition ids this worker will consume.
Consumer.prototype.start = function() {
    var self = this;
    return self.fetch_partition_count(self.handle, self.options.topic)
    .then(function(num_partitions) {
        if (!num_partitions) {
            throw new Error('no partitions for topic ' + self.options.topic);
        }
        if (num_partitions < self.num_workers) {
            throw new Error('only ' + num_partitions + ' for ' + self.num_workers + ' workers ');
        }

        var partitions = self._my_partitions(num_partitions);
        return self.offset_mgr.read(partitions)
        .then(function(offsets) {
            _.each(offsets, function(offset, i) {
                var partition = partitions[i];
                self.expected_offsets[partition] = offset + 1;
                self.receiving_offsets[partition] = [];
            });
        });
    })
    .then(function() {
        self.handle.start(self.expected_offsets);
        return _.keys(self.expected_offsets);
    });
};

Consumer.prototype.pause = function() {
    this.handle.pause();
};

Consumer.prototype.resume = function() {
    this.handle.resume();
};

Consumer.prototype.stop = function() {
    var self = this;
    return Promise.try(function() {
        self.handle.stop();
        // Ensure any pending commits hit the disk
        return self.offset_mgr.commit();
    });
};

// Callback from RawConsumer
Consumer.prototype._rdkakfa_recv_callback = function(kmsgs) {
    var self = this;

    if (kmsgs.length === 0) {
        return;
    }

    var offsets = {};
    var misses = 0;
    var repeats = 0;
    var messages = [];

    for (var i = 0; i < kmsgs.length; ++i) {
        var kmsg = kmsgs[i];
        if (self.expected_offsets[kmsg.partition] === undefined ||
            self.receiving_offsets[kmsg.partition] === undefined) {
            self.logger.error('skipping message for unexpected partition ' + kmsg.partition);
            continue;
        }
        var current = self.expected_offsets[kmsg.partition];

        if (kmsg.offset < current) {
            // This can happen if the topic/partition data has been reset,
            // which can happen if kafka's data directory was moved, or we're
            // suddenly talking to a new kafka instance.
            repeats++;
            self.logger.warn('earlier offset than expected: topic', self.options.topic,
                                'partition', kmsg.partition,
                                'received message offset', kmsg.offset, 'expected', current,
                                'may indicate unintended offset directory, and cause repeated data');
        } else if (kmsg.offset > current) {
            // This can happen when we are not keeping up with the incoming
            // rate of messages.
            misses += kmsg.offset - current;
        }

        messages.push(kmsg);
        offsets[kmsg.partition] = kmsg.offset;
        self.expected_offsets[kmsg.partition] = kmsg.offset + 1;
    }

    _.each(offsets, function(offset, partition) {
        self.receiving_offsets[partition].push({offset: offset, done: false});
    });

    Promise.try(function() {
        return self.options.receive_callback({
            messages: messages,
            misses: misses,
            repeats: repeats,
        });
    })
    .finally(function() {
        self._receive_done(offsets);
    });
};

// Higher level application is done processing the given offsets,
// commit those offsets to disk, ensuring that we're committing
// offsets in order.
Consumer.prototype._receive_done = function(offsets) {
    var self = this;

    var commits = {};
    _.each(offsets, function(offset, partition) {
        var offlist = self.receiving_offsets[partition];
        var entry = _.find(offlist, {offset: offset});

        entry.done = true;
        while (offlist.length !== 0 && offlist[0].done) {
            commits[partition] = offlist[0].offset;
            offlist.shift();
        }
    });

    if (_.isEmpty(commits)) {
        return;
    }

    self.offset_mgr.commit(commits).catch(function(err) {
        self.logger.warn('kafka_consumer commit failed: ', err.message);
        // dont rethrow, no one will catch.
    });
};

// Callback from RawConsumer/rdkafka periodic statistics.
// Parses & calls user level stat_cb with seeminly the most useful
// statistics for monitoring:
Consumer.prototype._rdkafka_stats_callback = function(statstr) {
    if (_.isEmpty(this.expected_offsets)) {
        // havent read partition count yet
        return;
    }

    var stats = JSON.parse(statstr);

    var partitions = stats.topics[this.options.topic].partitions;
    var partition_ids = _.keys(this.expected_offsets);

    // all our partitions are going into the same librdafka queue,
    // hence we only need to look at one partitions fetchq_cnt.
    var waiting_local = partitions[partition_ids[0]].fetchq_cnt;

    var waiting_kafka = _.sumBy(partition_ids, function(p) {
        // lag may be -1 if librdkafka hasn't read offsets from server
        if (partitions[p].consumer_lag <= 0) {
            return 0;
        }
        return partitions[p].consumer_lag;
    });

    // This the number of messages on disk in kafka, some to all
    // of which have been processed.
    var kafka_log_size = _.sumBy(partition_ids, function(p) {
        // offsets may be -1 if rdkafka hasn't read offsets from server
        if (partitions[p].hi_offset < 0 || partitions[p].lo_offset < 0) {
            return 0;
        }
        return partitions[p].hi_offset - partitions[p].lo_offset;
    });

    this.options.stats_callback({
        waiting_local: waiting_local,
        waiting_kafka: waiting_kafka,
        kafka_log_size: kafka_log_size,
        rdkafka_stats: stats,
    });
};


module.exports = {
    Consumer: Consumer,
    OffsetDirectory: OffsetDirectory,
};
