var _ = require('lodash');
var expect = require('chai').expect;
var Promise = require('bluebird');
var Producer = require('../lib/producer').Producer;
var testdata = require('./testdata');

function make_raw_producer(send_fn, outq_fn) {
    var clazz = function() { }
    clazz.prototype.send = function(topic, partition, payloads) {
        if (send_fn) {
            return send_fn(topic, partition, payloads);
        }
    };
    clazz.prototype.outq_length = outq_fn || function() { return 0; }
    clazz.prototype.stop = function() { }
    return clazz;
}

function ProducerTest(options) {
    var npartions = options.npartions || 1;
    var initial_offsets = options.initial_offsets || _.fill(Array(npartions), -1);
    var RawProducer = make_raw_producer(options.send, options.outq_length);
    var options = {
        broker: 'broker',
        fetch_partition_count: function() { return Promise.resolve(npartions); },
        raw_producer: RawProducer,
        stats_callback: options.stats_callback,
        delivery_report_callback: options.delivery_report_callback,
    };
    this.options = options;
    this.producer = new Producer(options);
}

ProducerTest.prototype.inject_stats = function(statstr) {
    this.producer._rdkafka_stats_callback(statstr);
};

ProducerTest.prototype.inject_dr = function(errinfo) {
    this.producer._rdkafka_dr_callback(JSON.parse(errinfo));
};


describe('producer', function() {
    it('should send messages', function() {
        var test_topic = 'testtopic';
        var test_partition = 0;
        var test_payloads = ['hello','there'];
        var test_result = {enqueued: 2, send_queue_length: 2};

        var test = new ProducerTest({
            send: function(topic, partition, payloads) {
                expect(topic).to.equal(test_topic);
                expect(partition).to.equal(test_partition);
                expect(payloads).to.deep.equal(test_payloads);
                return _.extend({}, test_result, {'dont': 'show_up'});
            },
            npartions: 1,
        });
        return Promise.try(function() {
            var result = test.producer.send(test_topic, test_partition, test_payloads);
            expect(result).to.deep.equal(test_result);
        });
    });

    it('should wait for send queue to drain when stopped', function() {
        this.timeout(5000);

        var wait = true;
        var called = false;
        var test = new ProducerTest({
            outq_length: function() {
                if (wait) { return 1; }
                called = true;
                return 0;
            }
        });
        setTimeout(function() {
            wait = 0;
        }, 2000);
        return test.producer.stop()
        .then(function() {
            expect(called).to.equal(true);
        });
    });

    it('should parse rdkafka stats', function(done) {
        var statstr = testdata.producer.statstr1;
        var test = new ProducerTest({
            stats_callback: function(stats) {
                Promise.try(function() {
                    expect(stats.queued).to.deep.equal({testtopic: 0});
                    expect(stats.rdkafka_stats).to.deep.equal(JSON.parse(statstr));
                }).then(done, done);
            }
        });
        test.inject_stats(statstr);
    });

    it('should parse rdkafka delivery reports', function(done) {
        var drstr = testdata.producer.drstr1;
        var test = new ProducerTest({
            delivery_report_callback: function(errinfo) {
                Promise.try(function() {
                    expect(errinfo.errors).to.equal(2);
                    expect(errinfo.rdkafka_errinfo).to.deep.equal(JSON.parse(drstr));
                }).then(done, done);
            }
        });
        test.inject_dr(drstr);
    });

});
