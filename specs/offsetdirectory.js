var _ = require('lodash');
var assert = require('assert');
var expect = require('chai').expect;
var Promise = require('bluebird');
var OffsetDirectory = require('../lib/offsetdirectory');

function test_odir() {
    return new OffsetDirectory({
        offset_directory: '/dev/null',
        topic: 'testtopic',
    });
}

describe('OffsetDirectory', function() {
    it('should use _commit_one to commit individual offsets', function() {
        var odir = test_odir();

        var res = {};
        odir._commit_one = function(partition, offset) {
            res[partition] = offset;
            return Promise.resolve();
        };

        var poffsets = {0: 1, 1: 2, 2: 3};
        return odir.commit(poffsets)
        .then(function() {
            expect(res).to.deep.equal(poffsets);
        });
    });

    it('should use _commit to commit partition/offsets', function() {
        var odir = test_odir();

        var res = {};
        function cfn(poffsets) {
            _.extend(res, poffsets);
            return Promise.resolve();
        };
        odir._commit = cfn.bind(odir);

        var poffsets = {0: 1, 1: 2, 2: 3};
        return odir.commit(poffsets)
        .then(function() {
            expect(res).to.deep.equal(poffsets);
        });
    });

    it('should use _read_one to read offsets', function() {
        var odir = test_odir();

        var poffsets = {0: 1, 1: 2, 2: 3};
        odir._read_one = function(partition) {
            return poffsets[partition];
        };

        var partitions = _.keys(poffsets);
        return odir.read(partitions)
        .then(function(offsets) {
            expect(offsets.length).to.equal(partitions.length);
            var res = _.fromPairs(_.zip(partitions, offsets));
            expect(res).to.deep.equal(poffsets);
        });
    });

    // concurrent commit test needs explicit Promise control, this from:
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

    it('should handle multiple concurrent commit requests', function() {
        var odir = test_odir();

        var commit_params = [
            {0: 0, 1: 0},
            {0: 1, 1: 1, 2: 0},
            {0: 2, 1: 2},
        ];
        var current_state = {};
        var expected_state = {0: 2, 1: 2, 2: 0};

        var final_defer = defer();

        function commit2(poffsets) {
            _.extend(current_state, poffsets);
            return Promise.resolve();
        }

        var final_promise;
        function commit1(poffsets) {
            return new Promise(function(resolve) {
                expect(poffsets).to.deep.equal(commit_params[0]);

                _.extend(current_state, poffsets);

                Promise.delay(100)
                .then(function() {
                    odir._commit = commit2.bind(odir);

                    odir.commit(commit_params[1]);

                    odir.commit(commit_params[2])
                    .then(function() {
                        final_defer.resolve();
                    });
                    resolve();
                })
            });
        }

        odir._commit = commit1.bind(odir);
        odir.commit(commit_params[0]);

        return final_defer.promise.then(function() {
            expect(current_state).to.deep.equal(expected_state);
        });
    });

});
