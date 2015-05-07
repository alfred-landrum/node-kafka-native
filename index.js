var addon = require('./build/Release/jut-node-kafka');

var rd_kafka_err_str_to_code = {
    /* Internal errors to rdkafka: */
    RD_KAFKA_RESP_ERR__BEGIN: -200,     /* begin internal error codes */
    RD_KAFKA_RESP_ERR__BAD_MSG: -199,   /* Received message is incorrect */
    RD_KAFKA_RESP_ERR__BAD_COMPRESSION: -198, /* Bad/unknown compression */
    RD_KAFKA_RESP_ERR__DESTROY: -197,   /* Broker is going away */
    RD_KAFKA_RESP_ERR__FAIL: -196,      /* Generic failure */
    RD_KAFKA_RESP_ERR__TRANSPORT: -195, /* Broker transport error */
    RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE: -194, /* Critical system resource
                              * failure */
    RD_KAFKA_RESP_ERR__RESOLVE: -193,   /* Failed to resolve broker */
    RD_KAFKA_RESP_ERR__MSG_TIMED_OUT: -192, /* Produced message timed out*/
    RD_KAFKA_RESP_ERR__PARTITION_EOF: -191, /* Reached the end of the
                          * topic+partition queue on
                          * the broker.
                          * Not really an error. */
    RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: -190, /* Permanent:
                              * Partition does not
                              * exist in cluster. */
    RD_KAFKA_RESP_ERR__FS: -189,        /* File or filesystem error */
    RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC: -188, /* Permanent:
                          * Topic does not exist
                          * in cluster. */
    RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN: -187, /* All broker connections
                             * are down. */
    RD_KAFKA_RESP_ERR__INVALID_ARG: -186,  /* Invalid argument, or
                         * invalid configuration */
    RD_KAFKA_RESP_ERR__TIMED_OUT: -185,    /* Operation timed out */
    RD_KAFKA_RESP_ERR__QUEUE_FULL: -184,   /* Queue is full */
        RD_KAFKA_RESP_ERR__ISR_INSUFF: -183,   /* ISR count < required.acks */
    RD_KAFKA_RESP_ERR__END: -100,       /* end internal error codes */

    /* Standard Kafka errors: */
    RD_KAFKA_RESP_ERR_UNKNOWN: -1,
    RD_KAFKA_RESP_ERR_NO_ERROR: 0,
    RD_KAFKA_RESP_ERR_OFFSET_OUT_OF_RANGE: 1,
    RD_KAFKA_RESP_ERR_INVALID_MSG: 2,
    RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART: 3,
    RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE: 4,
    RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE: 5,
    RD_KAFKA_RESP_ERR_NOT_LEADER_FOR_PARTITION: 6,
    RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT: 7,
    RD_KAFKA_RESP_ERR_BROKER_NOT_AVAILABLE: 8,
    RD_KAFKA_RESP_ERR_REPLICA_NOT_AVAILABLE: 9,
    RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE: 10,
    RD_KAFKA_RESP_ERR_STALE_CTRL_EPOCH: 11,
    RD_KAFKA_RESP_ERR_OFFSET_METADATA_TOO_LARGE: 12
};

var rd_kafka_errs = (function() {
    var ret = {};
    var code;
    var str;
    for (str in rd_kafka_err_str_to_code) {
        if (rd_kafka_err_str_to_code.hasOwnProperty(str)) {
            code = rd_kafka_err_str_to_code[str];
            ret[code] = str;
        }
    }
    return ret;
})();

rd_kafka_offsets = {
    RD_KAFKA_OFFSET_BEGINNING: -2,
    RD_KAFKA_OFFSET_END: -1,
    RD_KAFKA_OFFSET_STORED: -1000,
};

module.exports = {
    RawConsumer: addon.Consumer,
    RawProducer: addon.Producer,
    rd_kafka_errs: rd_kafka_errs,
    rd_kafka_offsets: rd_kafka_offsets,
};
