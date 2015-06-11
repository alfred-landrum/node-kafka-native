#include "producer.h"
#include <nan.h>
#include <v8.h>
#include <errno.h>
#include <iostream>

#include "persistent-string.h"

using namespace v8;

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
    NODE_SET_PROTOTYPE_METHOD(tpl, "outq_length", WRAPPED_METHOD_NAME(OutQueueLength));
    NODE_SET_PROTOTYPE_METHOD(tpl, "stop", WRAPPED_METHOD_NAME(Stop));


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

    std::string error;
    if (obj->producer_init(&error)) {
        NanThrowError(error.c_str());
        NanReturnUndefined();
    }

    NanReturnValue(args.This());
}

int
Producer::producer_init(std::string *error_str) {
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set_opaque(conf, this);

    int err = common_init(conf, error_str);
    if (err) {
        return err;
    }

    start_poll();

    return 0;
}

WRAPPED_METHOD(Producer, Send) {
    NanScope();

    if (stop_called_) {
        NanThrowError("already shutdown");
        NanReturnUndefined();
    }

    if (args.Length() != 3 ||
        !( args[0]->IsString() && args[1]->IsNumber() && args[2]->IsArray()) ) {
        NanThrowError("you must supply a topic name, partition, and array of strings");
        NanReturnUndefined();
    }

    String::Utf8Value topic_name(args[0]);
    rd_kafka_topic_t *topic = get_topic(*topic_name);
    if (!topic) {
        std::string error;
        topic = setup_topic(*topic_name, &error);
        if (!topic) {
            NanThrowError(error.c_str());
            NanReturnUndefined();
        }
    }

    int32_t partition = args[1].As<Number>()->Int32Value();
    Local<Array> msg_array(args[2].As<Array>());
    uint32_t message_cnt = msg_array->Length();

    std::unique_ptr<rd_kafka_message_t[]> holder(new rd_kafka_message_t[message_cnt]());
    rd_kafka_message_t *messages = holder.get();

    for (uint32_t i = 0; i < message_cnt; ++i) {
        rd_kafka_message_t *msg = &messages[i];
        const Local<String>& str(msg_array->Get(i).As<String>());

        int length = str->Utf8Length();
        msg->len = length;
        // malloc here to match the F_FREE flag below
        msg->payload = malloc(length);
        // WriteUtf8 in v8/src/api.cc
        str->WriteUtf8((char*)msg->payload, length, nullptr, 0);

        // ensure errors aren't missed by checking that
        // prodce_batch clears the error for good messages
        msg->err = RD_KAFKA_RESP_ERR_UNKNOWN;
    }

    uint32_t sent = rd_kafka_produce_batch(
        topic, partition, RD_KAFKA_MSG_F_FREE, messages, message_cnt);

    if (sent != message_cnt) {
        // Since rdkafka didn't take ownership of these,
        // we need to free them ourselves.
        for (uint32_t i = 0; i < message_cnt; ++i) {
            rd_kafka_message_t *msg = &messages[i];
            if (msg->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                free(msg->payload);
            }
        }
    }

    Local<Object> ret(NanNew<Object>());
    static PersistentString queue_length_key("queue_length");
    static PersistentString queued_key("queued");

    ret->Set(queue_length_key.handle(), NanNew<Number>(rd_kafka_outq_len(kafka_client_)));
    ret->Set(queued_key.handle(), NanNew<Number>(sent));

    NanReturnValue(ret);
}

WRAPPED_METHOD(Producer, GetMetadata) {
    NanScope();

    if (stop_called_) {
        NanThrowError("already shutdown");
        NanReturnUndefined();
    }

    get_metadata(args);

    NanReturnUndefined();
}

WRAPPED_METHOD(Producer, OutQueueLength) {
    NanScope();

    if (stop_called_) {
        NanThrowError("already shutdown");
        NanReturnUndefined();
    }

    int qlen = rd_kafka_outq_len(kafka_client_);

    NanReturnValue(NanNew<Number>(qlen));
}

WRAPPED_METHOD(Producer, Stop) {
    NanScope();

    stop_called_ = true;
    stop_poll();

    NanReturnUndefined();
}
