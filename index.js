var common = require('./lib/common');
var Consumer = require('./lib/consumer').Consumer;
var Producer = require('./lib/producer').Producer;

module.exports = {
    Consumer: Consumer,
    Producer: Producer,
    Errors: common.Errors,
    Offsets: common.Offsets,
};
