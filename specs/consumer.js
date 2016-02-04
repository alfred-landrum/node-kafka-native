var _ = require('lodash');
var expect = require('chai').expect;
var testdata = require('./testdata');
var Promise = require('bluebird');
var Consumer = require('../lib/consumer').Consumer;

function make_offset_manager(initial_offsets, commit_fn) {
    var clazz = function() { };
    clazz.prototype.read = function() {
        return Promise.resolve(initial_offsets);
    };
    clazz.prototype.commit = function(poffsets) {
        if (commit_fn) {
            return commit_fn(poffsets);
        }
        return Promise.resolve();
    };
    return clazz;
}

function make_raw_consumer() {
    var clazz = function() { };
    clazz.prototype.start = function() {
        return Promise.resolve();
    };
    clazz.prototype.stop = function () {
        return Promise.resolve();
    };
    clazz.prototype.pause = function () { };
    clazz.prototype.resume = function () { };
    return clazz;
}

function ConsumerTest(options) {
    var npartions = options.npartions || 1;
    var initial_offsets = options.initial_offsets || _.fill(Array(npartions), -1);
    var RawConsumer = make_raw_consumer();
    var OffsetManager = make_offset_manager(initial_offsets, options.commit);
    var options = {
        receive_callback: options.receive_callback || Promise.resolve,
        stats_callback: options.stats_callback,
        broker: 'broker',
        topic: 'testtopic',
        fetch_partition_count: function() { return Promise.resolve(npartions); },
        offset_manager: OffsetManager,
        raw_consumer: RawConsumer,
    };
    this.options = options;
    this.consumer = new Consumer(options);
}

ConsumerTest.prototype.inject_kmsgs = function(kmsgs) {
    this.consumer._rdkakfa_recv_callback(kmsgs);
};

ConsumerTest.prototype.inject_stats = function(statstr) {
    this.consumer._rdkafka_stats_callback(statstr);
};

function gen_kmsgs(partitions, offsets) {
    var kmsgs = [];
    for (var p = 0; p < partitions; p++) {
        for (var o = 0; o < offsets; o++) {
            kmsgs.push({
                partition: p,
                offset: o,
                payload: 'p' + p + 'o' + o,
            })
        }
    }
    return kmsgs;
}

function gen_commit(kmsgs) {
    return _.transform(kmsgs, function(result, msg) {
        var cur = result[msg.partition] === undefined ? -1 : result[msg.partition];
        result[msg.partition] = Math.max(cur, msg.offset);
    }, {});
}

function filter_kmsgs(kmsgs) {
    return kmsgs.map(function(m) {
        return {
            partition: m.partition,
            payload: m.payload,
            offset: m.offset,
        };
    });
}


describe('consumer', function() {
    it('should deliver and commit good messages', function(done) {
        var received;
        var kmsgs = gen_kmsgs(2, 2);
        var test = new ConsumerTest({
            receive_callback: function(_r) {
                received = _r;
                return Promise.resolve();
            },
            commit: function(poffsets) {
                return Promise.try(function() {
                    expect(received).to.not.be.undefined;
                    expect(received.misses).to.equal(0);
                    expect(received.repeats).to.equal(0);
                    expect(filter_kmsgs(received.messages)).to.deep.equal(kmsgs);
                    expect(poffsets).to.deep.equal(gen_commit(kmsgs));
                }).then(done, done);
            },
            npartions: 2,
        });
        test.consumer.start().then(function() {
            test.inject_kmsgs(kmsgs);
        });
    });

    // A 'missed' offset occurs when we aren't keeping up with the
    // messags arriving at the kafka broker.
    it('should report missed messages', function(done) {
        var received;
        var kmsgs = gen_kmsgs(2, 2);
        kmsgs.push({
            partition: 0,
            offset: 3,
            payload: 'p0o3',
        });
        var test = new ConsumerTest({
            receive_callback: function(_r) {
                received = _r;
                return Promise.resolve();
            },
            commit: function(poffsets) {
                return Promise.try(function() {
                    expect(received).to.not.be.undefined;
                    expect(received.misses).to.equal(1);
                    expect(received.repeats).to.equal(0);
                    expect(filter_kmsgs(received.messages)).to.deep.equal(kmsgs);
                    expect(poffsets).to.deep.equal(gen_commit(kmsgs));
                }).then(done, done);
            },
            npartions: 2,
        });
        test.consumer.start().then(function() {
            test.inject_kmsgs(kmsgs);
        });
    });

    it('should handle repeated offsets', function(done) {
        var received;
        var kmsgs = gen_kmsgs(2, 2);
        var expected_commit = gen_commit(kmsgs);
        kmsgs.push({
            partition: 0,
            offset: 0,
            payload: 'p0o0-2',
        });
        expected_commit[0] = 0;

        var test = new ConsumerTest({
            receive_callback: function(_r) {
                received = _r;
                return Promise.resolve();
            },
            commit: function(poffsets) {
                return Promise.try(function() {
                    expect(received).to.not.be.undefined;
                    expect(received.misses).to.equal(0);
                    expect(received.repeats).to.equal(1);
                    expect(filter_kmsgs(received.messages)).to.deep.equal(kmsgs);
                    expect(poffsets).to.deep.equal(expected_commit);
                }).then(done, done);
            },
            npartions: 2,
        });
        test.consumer.start().then(function() {
            test.inject_kmsgs(kmsgs);
        });
    });

    it('should parse rdkafka stats', function(done) {
        var statstr = testdata.consumer.statstr1;
        var test = new ConsumerTest({
            npartions: 2,
            stats_callback: function(stats) {
                Promise.try(function() {
                    var expected = {
                        waiting_local: 20,
                        waiting_kafka: 0,
                        kafka_log_size: 55,
                        rdkafka_stats: JSON.parse(statstr),
                    };
                    expect(expected).to.deep.equal(stats);
                }).then(done, done);
            }
        });
        test.consumer.start().then(function() {
            test.inject_stats(statstr);
        });
    });
});
