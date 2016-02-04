#!/bin/bash
set -e

dir="$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cfile=${dir}/compose-kafka-zookeeper.yml

start() {
    # See docker scripts at https://github.com/wurstmeister/kafka-docker
    # to see how these KAFKA_* variables configure the kafka broker.
    export KAFKA_NUM_PARTITIONS=2
    if [[ -n "$DOCKER_MACHINE_NAME" ]]; then
        export KAFKA_ADVERTISED_HOST_NAME=$(docker-machine ip "$DOCKER_MACHINE_NAME" 2> /dev/null)
        broker=$ip:${barr[1]}
    else
        export KAFKA_ADVERTISED_HOST_NAME=localhost
    fi

    docker-compose -f ${cfile} up --force-recreate -d &> /dev/null
    port
}

port() {
    broker=$(docker-compose -f ${cfile} port kafka 9092 2>/dev/null)
    barr=(${broker//:/ })

    # Would be great if docker-compose did this automatically.
    if [[ -n "$DOCKER_MACHINE_NAME" ]]; then
        ip=$(docker-machine ip "$DOCKER_MACHINE_NAME" 2> /dev/null)
        broker=$ip:${barr[1]}
    else
        broker=localhost:${barr[1]}
    fi

    export NODE_KAFKA_NATIVE_BROKER=${broker}
    echo "export NODE_KAFKA_NATIVE_BROKER=${broker}"
}

stop() {
    docker-compose -f ${cfile} kill &>/dev/null
}


case "$1" in
    start)
        start
        ;;

    stop)
        stop
        ;;

    port)
        port
        ;;

    *)
        echo $"Usage: $0 {start|port|stop}"
        exit 1
esac
