var addon = require('./build/Release/jut-node-kafka');

module.exports = {
    RawConsumer: addon.Consumer,
    RawProducer: addon.Producer,
};
