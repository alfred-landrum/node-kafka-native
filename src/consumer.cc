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
    toppars_(),
    recv_callback_(),
    kafka_queue_(NULL),
    shutdown_(false),
    buffer_pool_()
{
}

Consumer::~Consumer()
{
    if (kafka_queue_) {
        rd_kafka_queue_destroy(kafka_queue_);
        kafka_queue_ = NULL;
    }
}

Persistent<Function> Consumer::constructor;

void
Consumer::Init() {
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(New);
    tpl->SetClassName(NanNew("Consumer"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Common::Init(tpl);

    NODE_SET_PROTOTYPE_METHOD(tpl, "start_recv", WRAPPED_METHOD_NAME(StartRecv));
    NODE_SET_PROTOTYPE_METHOD(tpl, "stop_recv", WRAPPED_METHOD_NAME(StopRecv));
    NODE_SET_PROTOTYPE_METHOD(tpl, "set_offset", WRAPPED_METHOD_NAME(SetOffset));
    NODE_SET_PROTOTYPE_METHOD(tpl, "get_metadata", WRAPPED_METHOD_NAME(GetMetadata));

    NanAssignPersistent(constructor, tpl->GetFunction());
}

Local<Object> Consumer::NewInstance(Local<Value> arg) {
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

void consumer_trampoline(void *_consumer) {
    ((Consumer *)_consumer)->kafka_consumer();
}

int
Consumer::consumer_init(string *error) {
    NanScope();

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

    if (args.Length() != 2 ||
        !( args[0]->IsString() && args[1]->IsNumber()) ) {
        NanThrowError("you must supply a topic name and partition");
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

    uint32_t partition = args[1].As<Number>()->Uint32Value();
    for (auto &i : toppars_) {
        if (i.first == topic && i.second == partition) {
            NanThrowError("already receiving for requested topic and partition");
            NanReturnUndefined();
        }
    }

    if (rd_kafka_consume_start_queue(topic, partition, RD_KAFKA_OFFSET_STORED, kafka_queue_)) {
        int kafka_errno = errno;
        NanThrowError(rdk_error_string(kafka_errno).c_str());
        NanReturnUndefined();
    }

    toppars_.push_back(make_pair(topic, partition));

    NanReturnUndefined();
}

WRAPPED_METHOD(Consumer, StopRecv) {
    NanScope();

    if (args.Length() != 2 ||
        !( args[0]->IsString() && args[1]->IsNumber()) ) {
        NanThrowError("you must supply a topic name and partition");
        NanReturnUndefined();
    }

    String::AsciiValue topic_name(args[0]);
    rd_kafka_topic_t *topic = get_topic(*topic_name);
    if (!topic) {
        NanThrowError("not receiving from topic");
        NanReturnUndefined();
    }

    uint32_t partition = args[1].As<Number>()->Uint32Value();

    auto iter(find(toppars_.begin(), toppars_.end(), make_pair(topic, partition)));
    if (iter == toppars_.end()) {
        NanThrowError("not receiving for requested topic and partition");
        NanReturnUndefined();
    }

    rd_kafka_consume_stop(topic, partition);
    toppars_.erase(iter);

    NanReturnUndefined();
}

WRAPPED_METHOD(Consumer, SetOffset) {
    NanScope();

    if (args.Length() != 3 ||
        !( args[0]->IsString() && args[1]->IsNumber() && args[2]->IsNumber() )) {
        NanThrowError("you must supply a topic name, partition, and offset");
        NanReturnUndefined();
    }

    String::AsciiValue topic_name(args[0]);
    rd_kafka_topic_t *topic = get_topic(*topic_name);
    if (!topic) {
        NanThrowError("unknown topic");
        NanReturnUndefined();
    }

    uint32_t partition = args[1]->Uint32Value();
    int64_t offset = args[2]->IntegerValue();

    int err = rd_kafka_offset_store(topic, partition, offset);
    // only possible error from librdkafka is unknown partition
    assert(err == 0);

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
            msg = NULL;
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

Local<Value>
Consumer::create_object(const unsigned char* data, size_t size) {
    NanScope();
    const bool recv_as_strings = true;
    if (recv_as_strings) {
        return NanNew<String>(data, size);
    } else {
        return buffer_pool_.allocate(data, size);
    }
}

void
Consumer::kafka_recv(const vector<rd_kafka_message_t*> &vec) {
    // called in v8 thread
    NanScope();

    if (!recv_callback_) {
        return;
    }

    static PersistentString topic("topic");
    static PersistentString partition("partition");
    static PersistentString offset("offset");
    static PersistentString payload("payload");
    static PersistentString key("key");

    uint good = 0;
    size_t n = 0;
    Local<Array> messages = NanNew<Array>();
    for (auto msg : vec) {
        if (msg->err) {
            continue;
        }
        good++;
        Local<Object> obj = NanNew<Object>();
        obj->Set(topic.handle(), NanNew<String>(rd_kafka_topic_name(msg->rkt)));
        obj->Set(partition.handle(), NanNew<Number>(msg->partition));
        obj->Set(offset.handle(), NanNew<Number>(msg->offset));
        if (msg->key_len) {
            obj->Set(key.handle(), create_object((uint8_t*)msg->key, msg->key_len));
        }
        if (msg->len) {
            obj->Set(payload.handle(), create_object((uint8_t*)msg->payload, msg->len));
        }
        messages->Set(n++, obj);
    }
    if (good) {
        Local<Value> argv[] = { messages };
        recv_callback_->Call(1, argv);
    }
}
