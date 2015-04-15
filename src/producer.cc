#include "producer.h"
#include <nan.h>
#include <v8.h>
#include <errno.h>
#include <iostream>

using namespace v8;
using namespace std;

Producer::Producer(Local<Object> &options)
    : Common(RD_KAFKA_PRODUCER, options)
{
}

Producer::~Producer()
{
}

Persistent<Function> Producer::constructor;

void
Producer::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("Producer"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    NODE_SET_PROTOTYPE_METHOD(tpl, "send", WRAPPED_METHOD_NAME(Send));
    NODE_SET_PROTOTYPE_METHOD(tpl, "get_metadata", WRAPPED_METHOD_NAME(GetMetadata));

    NanAssignPersistent(constructor, tpl->GetFunction());
}

Local<Object>
Producer::NewInstance(Local<Value> arg) {
    NanEscapableScope();

    const unsigned argc = 1;
    Local<Value> argv[argc] = { arg };
    Local<Function> cons = NanNew<Function>(constructor);
    Local<Object> instance = cons->NewInstance(argc, argv);

    return NanEscapeScope(instance);
}

NAN_METHOD(Producer::New) {
    NanScope();

    if (!args.IsConstructCall()) {
        return NanThrowError("non-constructor invocation not supported");
    }

    Local<Object> options(NanNew<Object>());

    if (args.Length() == 1 && args[0] != NanUndefined()) {
        options = args[0].As<Object>();
    }

    Producer* obj = new Producer(options);
    obj->Wrap(args.This());

    string error;
    if (obj->producer_init(&error)) {
        NanThrowError(error.c_str());
        NanReturnUndefined();
    }

    NanReturnValue(args.This());
}

int
Producer::producer_init(string *error_str) {
    return common_init(error_str);
}

WRAPPED_METHOD(Producer, Send) {
    NanScope();

    if (!kafka_client_) {
        NanThrowError("you must setup the client before using send");
        NanReturnUndefined();
    }

    if (args.Length() != 3 ||
        !( args[0]->IsString() && args[1]->IsNumber() && args[2]->IsArray()) ) {
        NanThrowError("you must supply a topic name, partition, and array of strings");
        NanReturnUndefined();
    }

    String::AsciiValue topic_name(args[0]);
    rd_kafka_topic_t *topic = get_topic(*topic_name);
    if (!topic) {
        string error;
        topic = setup_topic(*topic_name, &error);
        if (!topic) {
            NanThrowError(error.c_str());
            NanReturnUndefined();
        }
    }

    int32_t partition = args[1].As<Number>()->Int32Value();
    Local<Array> msg_array(args[2].As<Array>());
    uint32_t message_cnt = msg_array->Length();

    unique_ptr<rd_kafka_message_t[]> holder(new rd_kafka_message_t[message_cnt]());
    rd_kafka_message_t *messages = holder.get();

    for (uint32_t i = 0; i < message_cnt; ++i) {
        rd_kafka_message_t *msg = &messages[i];
        const Local<String>& str(msg_array->Get(i).As<String>());

        int length = str->Length();
        msg->len = length;
        // malloc here to match the F_FREE flag below
        msg->payload = malloc(length);
        // WriteAscii in v8/src/api.cc
        str->WriteAscii((char*)msg->payload, /*start*/0, length, /*flags*/0);
    }

    uint32_t sent = rd_kafka_produce_batch(
        topic, partition, RD_KAFKA_MSG_F_FREE, messages, message_cnt);

    if (sent != message_cnt) {
        // Since rdkafka didn't take ownership of these,
        // we need to free them ourselves.
        for (uint32_t i = 0; i < (message_cnt - sent); ++i) {
            rd_kafka_message_t *msg = &messages[i + sent];
            free(msg->payload);
        }
    }

    NanReturnValue(NanNew<Number>(sent));
}

WRAPPED_METHOD(Producer, GetMetadata) {
    NanScope();
    get_metadata(args);
    NanReturnUndefined();
}
