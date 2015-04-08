#include <node.h>
#include <nan.h>
#include "producer.h"
#include "consumer.h"

using namespace v8;

NAN_METHOD(CreateProducer) {
    NanScope();
    NanReturnValue(Producer::NewInstance(args[0]));
}

NAN_METHOD(CreateConsumer) {
    NanScope();
    NanReturnValue(Consumer::NewInstance(args[0]));
}

void InitAll(Handle<Object> exports) {
    NanScope();

    Producer::Init();
    Consumer::Init();

    exports->Set(NanNew("Producer"),
        NanNew<FunctionTemplate>(CreateProducer)->GetFunction());
    exports->Set(NanNew("Consumer"),
        NanNew<FunctionTemplate>(CreateConsumer)->GetFunction());
}

NODE_MODULE(jut_node_kafka, InitAll)
