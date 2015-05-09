#include "consumer.h"
#include <nan.h>
#include <v8.h>
#include <errno.h>
#include <iostream>

#include "persistent-string.h"

using namespace v8;
using namespace std;

Consumer::Consumer(Local<Object> &options):
    Common(RD_KAFKA_CONSUMER, options),
    partitions_(),
    recv_callback_(),
    kafka_queue_(nullptr),
    topic_(nullptr),
    shutdown_(false),
    buffer_pool_()
{
}

Consumer::~Consumer()
{
    if (kafka_queue_) {
        rd_kafka_queue_destroy(kafka_queue_);
        kafka_queue_ = nullptr;
    }
    // rd_kafka_topic_destroy() called in ~Common
    topic_ = nullptr;
}

Persistent<Function> Consumer::constructor;

void
Consumer::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("Consumer"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    NODE_SET_PROTOTYPE_METHOD(tpl, "start_recv", WRAPPED_METHOD_NAME(StartRecv));
    NODE_SET_PROTOTYPE_METHOD(tpl, "stop_recv", WRAPPED_METHOD_NAME(StopRecv));
    NODE_SET_PROTOTYPE_METHOD(tpl, "get_metadata", WRAPPED_METHOD_NAME(GetMetadata));

    NanAssignPersistent(constructor, tpl->GetFunction());
}

Local<Object>
Consumer::NewInstance(Local<Value> arg) {
    NanEscapableScope();

    const unsigned argc = 1;
    Local<Value> argv[argc] = { arg };
    Local<Function> cons = NanNew<Function>(constructor);
    Local<Object> instance = cons->NewInstance(argc, argv);

    return NanEscapeScope(instance);
}

NAN_METHOD(Consumer::New) {
    NanScope();

    if (!args.IsConstructCall()) {
        return NanThrowError("non-constructor invocation not supported");
    }

    Local<Object> options(NanNew<Object>());

    if (args.Length() == 1 && args[0]->IsObject()) {
        options = args[0].As<Object>();
    }

    Consumer* obj = new Consumer(options);
    obj->Wrap(args.This());

    string error;
    if (obj->consumer_init(&error)) {
        NanThrowError(error.c_str());
        NanReturnUndefined();
    }

    NanReturnValue(args.This());
}

void
consumer_trampoline(void *_consumer) {
    ((Consumer *)_consumer)->kafka_consumer();
}

int
Consumer::consumer_init(string *error) {
    NanScope();

    static PersistentString topic_key("topic");
    Local<String> name = options_->Get(topic_key).As<String>();
    if (name == NanUndefined()) {
        *error = "options must contain a topic";
        return -1;
    }
    String::AsciiValue topic_name(name);
    topic_ = setup_topic(*topic_name, error);
    if (!topic_) {
        return -1;
    }

    static PersistentString recv_cb_key("recv_cb");
    Local<Function> recv_cb_fn = options_->Get(recv_cb_key).As<Function>();
    if (recv_cb_fn == NanUndefined()) {
        *error = "options must contain a recv_cb function";
        return -1;
    }
    recv_callback_.reset(new NanCallback(recv_cb_fn));

    int err = common_init(error);
    if (err) {
        return err;
    }

    kafka_queue_ = rd_kafka_queue_new(kafka_client_);
    uv_thread_create(&consume_thread_, consumer_trampoline, this);

    Ref();
    return 0;
}

WRAPPED_METHOD(Consumer, StartRecv) {
    NanScope();

    if (!recv_callback_) {
        NanThrowError("you must add a recv callback before start");
        NanReturnUndefined();
    }

    if (args.Length() != 1 ||
        !( args[0]->IsObject()) ) {
        NanThrowError("you must specify partition/offsets");
        NanReturnUndefined();
    }

    vector<pair<uint32_t, int64_t> > offsets;
    Local<Object> args_offsets = args[0].As<Object>();
    Local<Array> keys = args_offsets->GetOwnPropertyNames();
    for (size_t i = 0; i < keys->Length(); i++) {
        Local<Value> key = keys->Get(i);
        int32_t partition = key.As<Number>()->Int32Value();
        int64_t offset = args_offsets->Get(key).As<Number>()->IntegerValue();

        if (offset < 0 || partition < 0) {
            NanThrowError("invalid partition/offset");
            NanReturnUndefined();
        }

        offsets.push_back(make_pair((uint32_t)partition, offset));
    }

    // The only reason rd_kafka_consume_start_queue fails is if
    // partition or offset are < 0.
    for (size_t i = 0; i < offsets.size(); ++i) {
        uint32_t partition = offsets[i].first;
        int64_t offset = offsets[i].second;
        partitions_.push_back(partition);
        rd_kafka_consume_start_queue(topic_, partition, offset, kafka_queue_);
    }

    NanReturnUndefined();
}

WRAPPED_METHOD(Consumer, StopRecv) {
    NanScope();

    for (size_t i = 0; i < partitions_.size(); ++i) {
       rd_kafka_consume_stop(topic_, partitions_[i]);
    }

    partitions_.clear();

    NanReturnUndefined();
}

WRAPPED_METHOD(Consumer, GetMetadata) {
    NanScope();
    get_metadata(args);
    NanReturnUndefined();
}

class RecvEvent : public KafkaEvent {
public:
    RecvEvent(Consumer *consumer, vector<rd_kafka_message_t*> &&msgs):
        KafkaEvent(),
        consumer_(consumer),
        msgs_(msgs)
    {}

    virtual ~RecvEvent() {
        for (auto &msg : msgs_) {
            rd_kafka_message_destroy(msg);
            msg = nullptr;
        }
    }

    virtual void v8_cb() {
        consumer_->kafka_recv(msgs_);
    }

    Consumer *consumer_;
    vector<rd_kafka_message_t*> msgs_;
};

// why use rd_kafka_consume_batch instead of rd_kafka_consume_callback?
// because batch gives us ownership of the rkmessages
void
Consumer::kafka_consumer() {
    vector<rd_kafka_message_t*> vec;

    while (!shutdown_) {
        const int max_size = 10000;
        const int timeout_ms = 500;
        vec.resize(max_size);
        int cnt = rd_kafka_consume_batch_queue(kafka_queue_, timeout_ms, &vec[0], max_size);
        if (cnt > 0) {
            // Note that some messages may be errors, eg: RD_KAFKA_RESP_ERR__PARTITION_EOF
            vec.resize(cnt);
            ke_push(unique_ptr<KafkaEvent>(new RecvEvent(this, move(vec))));
        }
    }
}

void
Consumer::kafka_recv(const vector<rd_kafka_message_t*> &vec) {
    // called in v8 thread
    NanScope();

    if (!recv_callback_) {
        return;
    }

    static PersistentString topic_key("topic");
    static PersistentString partition_key("partition");
    static PersistentString offset_key("offset");
    static PersistentString payload_key("payload");
    static PersistentString key_key("key");
    static PersistentString errcode_key("errcode");

    int msg_idx = -1;
    int err_idx = -1;

    const bool recv_as_strings = true;
    Local<Array> messages = NanNew<Array>();
    Local<Array> errors = NanNew<Array>();
    for (auto msg : vec) {
        Local<Object> obj = NanNew<Object>();

        obj->Set(topic_key.handle(), NanNew<String>(rd_kafka_topic_name(msg->rkt)));
        obj->Set(partition_key.handle(), NanNew<Number>(msg->partition));
        obj->Set(offset_key.handle(), NanNew<Number>(msg->offset));

        if (msg->err) {
            obj->Set(errcode_key.handle(), NanNew<Number>(msg->err));
            errors->Set(++err_idx, obj);
            continue;
        }

        if (recv_as_strings) {
            if (msg->key_len) {
                obj->Set(key_key.handle(), NanNew<String>((char*)msg->key, msg->key_len));
            }
            if (msg->len) {
                obj->Set(payload_key.handle(), NanNew<String>((char*)msg->payload, msg->len));
            }
        } else {
            if (msg->key_len) {
                obj->Set(key_key.handle(), buffer_pool_.allocate((const unsigned char *)msg->key, msg->key_len));
            }
            if (msg->len) {
                obj->Set(payload_key.handle(), buffer_pool_.allocate((const unsigned char *)msg->payload, msg->len));
            }
        }
        messages->Set(++msg_idx, obj);
    }

    if (msg_idx > -1 || err_idx > -1) {
        Local<Value> argv[] = { messages, errors };
        recv_callback_->Call(2, argv);
    }
}
