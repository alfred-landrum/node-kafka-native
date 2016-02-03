var _ = require('lodash');
var path = require('path');
var Promise = require('bluebird');
var validateOpts = require('validate-options');

var fs = Promise.promisifyAll(require('fs'));
var fse = Promise.promisifyAll(require('fs-extra'));

// Manages files that record offsets of processed messages. It's expected
// that multiple consumer worker processes may be using the same directory
// for tracking offsets across different partitions and topics.
function OffsetDirectory(options) {
    validateOpts.hasAll(options, 'offset_directory','topic');
    this.path = options.offset_directory;
    this.topic = options.topic;

    // Promise for current commit action
    this._current_commit = null;

    // Offsets & promise for next commit after _current_commit completes
    this._pending_offsets = {};
    this._next_commit = null;
}

OffsetDirectory.prototype._filename = function(partition) {
    return this.topic + '-' + partition + '.offset';
};

// Return last committed offset for the partition, or -1 if none found.
OffsetDirectory.prototype._read_one = function(partition) {
    var fpath = path.join(this.path, this._filename(partition));
    return fs.readFileAsync(fpath, 'utf8')
    .then(function(data) {
        var offset = Number(data.trim());
        if (_.isNaN(offset)) {
            throw new Error('bad file format');
        }
        return offset;
    })
    .catch(function(err) {
        if (err.code === 'ENOENT') {
            return -1;
        }
        throw err;
    });
};

// Map an array of partiion ids to their last committed offset,
// or -1 if none found.
OffsetDirectory.prototype.read = function(partitions) {
    return Promise.map(partitions, this._read_one.bind(this));
};

// Atomically update the commit offset file for this partition.
OffsetDirectory.prototype._commit_one = function(partition, offset) {
    var str = offset.toString() + '\n';
    var buf = new Buffer(str);
    var len = Buffer.byteLength(str);
    var fname = this._filename(partition);
    var tmpfile = path.join(this.path, '.working-' + fname);
    var fpath = path.join(this.path, fname);

    return fse.mkdirpAsync(this.path)
    .then(function() {
        return fs.openAsync(tmpfile, 'w');
    })
    .then(function(fd) {
        return fs.writeAsync(fd, buf, 0, len, 0)
        .then(function() {
            return fs.fsyncAsync(fd);
        })
        .finally(function() {
            return fs.closeAsync(fd);
        });
    })
    .then(function() {
        return fs.renameAsync(tmpfile, fpath);
    });
};

// Atomically update the commit offset file for this partition.
OffsetDirectory.prototype._commit = function(poffsets) {
    var self = this;
    return Promise.map(_.keys(poffsets), function(partition) {
        var offset = poffsets[partition];
        return self._commit_one(partition, offset);
    });
};

// Update commit offset files for partitions.
// partition_offsets is an object of partition id -> offset.
// Can wait on pending commits to complete by calling with no parameter.
OffsetDirectory.prototype.commit = function(partition_offsets) {
    var self = this;
    partition_offsets = partition_offsets || {};

    function do_commit(poffsets) {
        self._current_commit = self._commit(poffsets)
        .finally(function() {
            self._current_commit = null;
        });
        return self._current_commit;
    }

    if (!self._current_commit) {
        return do_commit(_.clone(partition_offsets));
    }

    // If there's a commit in progress, overwrite any pending offsets
    // with this call, and setup promise to fulfill after _current_commit.
    _.extend(self._pending_offsets, partition_offsets);
    if (!self._next_commit) {
        self._next_commit = self._current_commit.finally(function() {
            var poffsets = self._pending_offsets;
            self._pending_offsets = {};
            self._next_commit = null;
            return do_commit(poffsets);
        });
    }
    return self._next_commit;
};


module.exports = OffsetDirectory;
