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
    topic_(nullptr),
    partitions_(),
    looper_(nullptr),
    queue_(nullptr),
    paused_(false),
    recv_callback_(),
    max_messages_per_callback_(10000)
{
}

Consumer::~Consumer()
{
    if (queue_) {
        rd_kafka_queue_destroy(queue_);
        queue_ = nullptr;
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

    NODE_SET_PROTOTYPE_METHOD(tpl, "start", WRAPPED_METHOD_NAME(Start));
    NODE_SET_PROTOTYPE_METHOD(tpl, "stop", WRAPPED_METHOD_NAME(Stop));
    NODE_SET_PROTOTYPE_METHOD(tpl, "pause", WRAPPED_METHOD_NAME(Pause));
    NODE_SET_PROTOTYPE_METHOD(tpl, "resume", WRAPPED_METHOD_NAME(Resume));
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

int
Consumer::consumer_init(string *error) {
    NanScope();

    // Convert persistent options to local for >node 0.12 compatibility
    Local<Object> options = NanNew(options_);

    static PersistentString topic_key("topic");
    Local<String> name = options->Get(topic_key).As<String>();
    if (name == NanUndefined()) {
        *error = "options must contain a topic";
        return -1;
    }

    static PersistentString recv_cb_key("recv_cb");
    Local<Function> recv_cb_fn = options->Get(recv_cb_key).As<Function>();
    if (recv_cb_fn == NanUndefined()) {
        *error = "options must contain a recv_cb function";
        return -1;
    }
    recv_callback_.reset(new NanCallback(recv_cb_fn));

    static PersistentString max_message_key("max_messages_per_callback");
    Local<Number> max_messages_obj = options->Get(max_message_key).As<Number>();
    if (max_messages_obj != NanUndefined()) {
        this->max_messages_per_callback_ = max_messages_obj->Uint32Value();
    }

    int err = common_init(error);
    if (err) {
        return err;
    }

    String::Utf8Value topic_name(name);
    topic_ = setup_topic(*topic_name, error);
    if (!topic_) {
        return -1;
    }

    queue_ = rd_kafka_queue_new(kafka_client_);

    return 0;
}

class ConsumerLoop {
public:
    ConsumerLoop(Consumer *consumer, rd_kafka_queue_t *queue):
        handle_(),
        consumer_(consumer),
        queue_(queue),
        paused_(false),
        shutdown_(false)
    {
        uv_cond_init(&cond_);
        uv_mutex_init(&mutex_);
    }

    ~ConsumerLoop()
    {
        uv_cond_destroy(&cond_);
        uv_mutex_destroy(&mutex_);
    }

    void start() {
        uv_thread_create(&handle_, ConsumerLoop::main, this);
    }

    void pause() {
        uv_mutex_lock(&mutex_);
        paused_ = true;
        uv_mutex_unlock(&mutex_);
    }

    void resume() {
        uv_mutex_lock(&mutex_);
        paused_ = false;
        uv_cond_signal(&cond_);
        uv_mutex_unlock(&mutex_);
    }

    void stop() {
        uv_mutex_lock(&mutex_);
        shutdown_ = true;
        uv_cond_signal(&cond_);
        uv_mutex_unlock(&mutex_);
    }

    bool stopping() { return shutdown_; }

    static void main(void *_loop) {
        ConsumerLoop *loop = static_cast<ConsumerLoop*>(_loop);
        loop->run();
    }

private:
    bool should_continue();
    void run();

    uv_thread_t handle_;
    uv_cond_t cond_;
    uv_mutex_t mutex_;
    Consumer *consumer_;
    rd_kafka_queue_t *queue_;
    bool paused_;
    bool shutdown_;
};

class RecvEvent : public KafkaEvent {
public:
    RecvEvent(Consumer *consumer, ConsumerLoop *looper, vector<rd_kafka_message_t*> &&msgs):
        KafkaEvent(),
        consumer_(consumer),
        looper_(looper),
        msgs_(msgs)
    {}

    virtual ~RecvEvent() {
        for (auto &msg : msgs_) {
            rd_kafka_message_destroy(msg);
            msg = nullptr;
        }
    }

    virtual void v8_cb() {
        consumer_->receive(looper_, msgs_);
    }

    Consumer *consumer_;
    ConsumerLoop *looper_;
    vector<rd_kafka_message_t*> msgs_;
};

class LooperStopped : public KafkaEvent {
public:
    LooperStopped(Consumer *consumer, ConsumerLoop *looper):
        KafkaEvent(),
        consumer_(consumer),
        looper_(looper)
    {}

    virtual ~LooperStopped() { }

    virtual void v8_cb() {
        consumer_->looper_stopped(looper_);
    }

    Consumer *consumer_;
    ConsumerLoop *looper_;
};

bool
ConsumerLoop::should_continue()
{
    uv_mutex_lock(&mutex_);
    if (shutdown_) {
        uv_mutex_unlock(&mutex_);
        return false;
    }

    while (paused_) {
        uv_cond_wait(&cond_, &mutex_);
        if (shutdown_) {
            uv_mutex_unlock(&mutex_);
            return false;
        }
    }

    uv_mutex_unlock(&mutex_);
    return true;
}

void
ConsumerLoop::run()
{
    vector<rd_kafka_message_t*> vec;

    while (should_continue()) {
        const uint32_t max_size = consumer_->max_messages_per_callback();
        const int timeout_ms = 500;
        vec.resize(max_size);
        int cnt = rd_kafka_consume_batch_queue(queue_, timeout_ms, &vec[0], max_size);
        if (cnt > 0) {
            // Note that some messages may be errors, eg: RD_KAFKA_RESP_ERR__PARTITION_EOF
            vec.resize(cnt);

            pause();
            consumer_->ke_push(unique_ptr<KafkaEvent>(new RecvEvent(consumer_, this, move(vec))));
        }
    }

    consumer_->ke_push(unique_ptr<KafkaEvent>(new LooperStopped(consumer_, this)));
}

WRAPPED_METHOD(Consumer, Start) {
    NanScope();

    if (looper_) {
        NanThrowError("consumer already started");
        NanReturnUndefined();
    }

    if (args.Length() != 1 ||
        !( args[0]->IsObject()) ) {
        NanThrowError("you must specify partition/offsets");
        NanReturnUndefined();
    }

    Local<Object> offsets = args[0].As<Object>();
    Local<Array> keys = offsets->GetOwnPropertyNames();
    for (size_t i = 0; i < keys->Length(); i++) {
        Local<Value> key = keys->Get(i);
        uint32_t partition = key.As<Number>()->Uint32Value();
        int64_t offset = offsets->Get(key).As<Number>()->IntegerValue();

        partitions_.push_back(partition);
        rd_kafka_consume_start_queue(topic_, partition, offset, queue_);
    }

    Ref();

    paused_ = false;
    looper_ = new ConsumerLoop(this, queue_);
    looper_->start();

    NanReturnUndefined();
}

WRAPPED_METHOD(Consumer, Stop) {
    NanScope();

    if (!looper_) {
        NanThrowError("consumer not started");
        NanReturnUndefined();
    }

    for (size_t i = 0; i < partitions_.size(); ++i) {
        rd_kafka_consume_stop(topic_, partitions_[i]);
    }

    partitions_.clear();

    paused_ = true;
    looper_->stop();
    looper_ = nullptr;

    NanReturnUndefined();
}

WRAPPED_METHOD(Consumer, Pause) {
    NanScope();

    if (!looper_) {
        NanThrowError("consumer not started");
        NanReturnUndefined();
    }

    paused_ = true;
    looper_->pause();

    NanReturnUndefined();
}

WRAPPED_METHOD(Consumer, Resume) {
    NanScope();

    if (!looper_) {
        NanThrowError("consumer not started");
        NanReturnUndefined();
    }

    paused_ = false;
    looper_->resume();

    NanReturnUndefined();
}

void
Consumer::looper_stopped(ConsumerLoop *looper) {
    // TODO: dec a looper count on consumer for graceful shutdown
    Unref();
    delete looper;
}

WRAPPED_METHOD(Consumer, GetMetadata) {
    NanScope();
    get_metadata(args);
    NanReturnUndefined();
}

void
Consumer::receive(ConsumerLoop *looper, const vector<rd_kafka_message_t*> &vec) {
    // called in v8 thread
    NanScope();

    if (looper->stopping()) {
        // This message came in after a stop_recv was issued, but before
        // the consumer thread was shutdown; dont deliver these messages.
        // Intentionally not checking _paused here, as doing so would cause
        // us to miss messages if Pause is called outside of the recv_callback
        // call.
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

        if (msg->key_len) {
            obj->Set(key_key.handle(), NanNew<String>((char*)msg->key, msg->key_len));
        }
        if (msg->len) {
            obj->Set(payload_key.handle(), NanNew<String>((char*)msg->payload, msg->len));
        }
        messages->Set(++msg_idx, obj);
    }

    if (msg_idx > -1 || err_idx > -1) {
        Local<Value> argv[] = { messages, errors };
        recv_callback_->Call(2, argv);
    }

    if (!paused_) {
        looper_->resume();
    }
}
