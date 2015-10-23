#include <node.h>
#include <nan.h>
#include "producer.h"
#include "consumer.h"

using namespace v8;

NAN_METHOD(CreateProducer) {
    info.GetReturnValue().Set(Producer::NewInstance(info[0]));
}

NAN_METHOD(CreateConsumer) {
    info.GetReturnValue().Set(Consumer::NewInstance(info[0]));
}

void InitAll(Handle<Object> exports) {
    Nan::HandleScope scope;

    Producer::Init();
    Consumer::Init();

    Nan::Set(exports, Nan::New("Producer").ToLocalChecked(),
        Nan::GetFunction(Nan::New<FunctionTemplate>(CreateProducer)).ToLocalChecked());
    Nan::Set(exports, Nan::New("Consumer").ToLocalChecked(),
        Nan::GetFunction(Nan::New<FunctionTemplate>(CreateConsumer)).ToLocalChecked());
}

NODE_MODULE(jut_node_kafka, InitAll)
