#include "consumer.h"
#include <nan.h>
#include <v8.h>
#include <errno.h>
#include <iostream>

#include "persistent-string.h"

using namespace v8;

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

Nan::Persistent<Function> Consumer::constructor;

void
Consumer::Init() {
    Nan::HandleScope scope;

    // Prepare constructor template
    Local<FunctionTemplate> tpl = Nan::New<FunctionTemplate>(New);
    tpl->SetClassName(Nan::New("Consumer").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Nan::SetPrototypeMethod(tpl, "start", WRAPPED_METHOD_NAME(Start));
    Nan::SetPrototypeMethod(tpl, "stop", WRAPPED_METHOD_NAME(Stop));
    Nan::SetPrototypeMethod(tpl, "pause", WRAPPED_METHOD_NAME(Pause));
    Nan::SetPrototypeMethod(tpl, "resume", WRAPPED_METHOD_NAME(Resume));
    Nan::SetPrototypeMethod(tpl, "get_metadata", WRAPPED_METHOD_NAME(GetMetadata));

    constructor.Reset(tpl->GetFunction());
}

Local<Object>
Consumer::NewInstance(Local<Value> arg) {
    Nan::EscapableHandleScope scope;

    const unsigned argc = 1;
    Local<Value> argv[argc] = { arg };
    Local<Function> cons = Nan::New<Function>(constructor);
    Local<Object> instance = Nan::NewInstance(cons, argc, argv).ToLocalChecked();

    return scope.Escape(instance);
}

NAN_METHOD(Consumer::New) {

    if (!info.IsConstructCall()) {
        return Nan::ThrowError("non-constructor invocation not supported");
    }

    Local<Object> options(Nan::New<Object>());

    if (info.Length() == 1 && info[0]->IsObject()) {
        options = info[0].As<Object>();
    }

    Consumer* obj = new Consumer(options);
    obj->Wrap(info.This());

    std::string error;
    if (obj->consumer_init(&error)) {
        Nan::ThrowError(error.c_str());
        return;
    }

    info.GetReturnValue().Set(info.This());
}

int
Consumer::consumer_init(std::string *error) {
    Nan::HandleScope scope;

    // Convert persistent options to local for >node 0.12 compatibility
    Local<Object> options = Nan::New(options_);

    static PersistentString topic_key("topic");
    Local<String> name = Nan::Get(options, topic_key).ToLocalChecked().As<String>();
    if (name == Nan::Undefined()) {
        *error = "options must contain a topic";
        return -1;
    }

    static PersistentString recv_cb_key("recv_cb");
    Local<Function> recv_cb_fn = Nan::Get(options, recv_cb_key).ToLocalChecked().As<Function>();
    if (recv_cb_fn == Nan::Undefined()) {
        *error = "options must contain a recv_cb function";
        return -1;
    }
    recv_callback_.reset(new Nan::Callback(recv_cb_fn));

    static PersistentString max_message_key("max_messages_per_callback");
    Local<Number> max_messages_obj = Nan::Get(options, max_message_key).ToLocalChecked().As<Number>();
    if (max_messages_obj != Nan::Undefined()) {
        this->max_messages_per_callback_ = max_messages_obj->Uint32Value();
    }

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set_opaque(conf, this);

    int err = common_init(conf, error);
    if (err) {
        return err;
    }

    String::Utf8Value topic_name(name);
    topic_ = setup_topic(*topic_name, error);
    if (!topic_) {
        return -1;
    }

    queue_ = rd_kafka_queue_new(kafka_client_);

    start_poll();

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
    RecvEvent(Consumer *consumer, ConsumerLoop *looper, std::vector<rd_kafka_message_t*> &&msgs):
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
    std::vector<rd_kafka_message_t*> msgs_;
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
    std::vector<rd_kafka_message_t*> vec;

    while (should_continue()) {
        const uint32_t max_size = consumer_->max_messages_per_callback();
        const int timeout_ms = 500;
        vec.resize(max_size);
        int cnt = rd_kafka_consume_batch_queue(queue_, timeout_ms, &vec[0], max_size);
        if (cnt > 0) {
            // Note that some messages may be errors, eg: RD_KAFKA_RESP_ERR__PARTITION_EOF
            vec.resize(cnt);

            pause();
            consumer_->ke_push(std::unique_ptr<KafkaEvent>(new RecvEvent(consumer_, this, std::move(vec))));
        }
    }

    consumer_->ke_push(std::unique_ptr<KafkaEvent>(new LooperStopped(consumer_, this)));
}

WRAPPED_METHOD(Consumer, Start) {
    Nan::HandleScope scope;

    if (stop_called_) {
        Nan::ThrowError("already shutdown");
        return;
    }

    if (looper_) {
        Nan::ThrowError("consumer already started");
        return;
    }

    if (info.Length() != 1 ||
        !( info[0]->IsObject()) ) {
        Nan::ThrowError("you must specify partition/offsets");
        return;
    }

    Local<Object> offsets = info[0].As<Object>();
    Local<Array> keys = Nan::GetOwnPropertyNames(offsets).ToLocalChecked();
    for (size_t i = 0; i < keys->Length(); i++) {
        Local<Value> key = Nan::Get(keys, i).ToLocalChecked();
        uint32_t partition = key.As<Number>()->Uint32Value();
        int64_t offset = Nan::Get(offsets, key).ToLocalChecked().As<Number>()->IntegerValue();

        partitions_.push_back(partition);
        rd_kafka_consume_start_queue(topic_, partition, offset, queue_);
    }

    paused_ = false;
    looper_ = new ConsumerLoop(this, queue_);
    looper_->start();

    return;
}

WRAPPED_METHOD(Consumer, Stop) {
    Nan::HandleScope scope;

    if (stop_called_) {
        Nan::ThrowError("already shutdown");
        return;
    }

    stop_called_ = true;

    if (!looper_) {
        stop_poll();
        return;
    }

    // start was called
    for (size_t i = 0; i < partitions_.size(); ++i) {
        rd_kafka_consume_stop(topic_, partitions_[i]);
    }

    partitions_.clear();

    paused_ = true;
    looper_->stop();
    looper_ = nullptr;

    return;
}

void
Consumer::looper_stopped(ConsumerLoop *looper) {
    delete looper;
    looper = nullptr;
    stop_poll();
}

WRAPPED_METHOD(Consumer, Pause) {
    Nan::HandleScope scope;

    if (stop_called_) {
        Nan::ThrowError("already shutdown");
        return;
    }

    if (!looper_) {
        Nan::ThrowError("consumer not started");
        return;
    }

    paused_ = true;
    looper_->pause();

    return;
}

WRAPPED_METHOD(Consumer, Resume) {
    Nan::HandleScope scope;

    if (stop_called_) {
        Nan::ThrowError("already shutdown");
        return;
    }

    if (!looper_) {
        Nan::ThrowError("consumer not started");
        return;
    }

    if (paused_) {
        paused_ = false;
        looper_->resume();
    }

    return;
}

WRAPPED_METHOD(Consumer, GetMetadata) {
    Nan::HandleScope scope;
    get_metadata(info);
    return;
}

void
Consumer::receive(ConsumerLoop *looper, const std::vector<rd_kafka_message_t*> &vec) {
    // called in v8 thread
    Nan::HandleScope scope;

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

    Local<Array> messages = Nan::New<Array>();
    Local<Array> errors = Nan::New<Array>();
    for (auto msg : vec) {
        Local<Object> obj = Nan::New<Object>();

        Nan::Set(obj, topic_key.handle(), Nan::New<String>(rd_kafka_topic_name(msg->rkt)).ToLocalChecked());
        Nan::Set(obj, partition_key.handle(), Nan::New<Number>(msg->partition));
        Nan::Set(obj, offset_key.handle(), Nan::New<Number>(msg->offset));

        if (msg->err) {
            Nan::Set(obj, errcode_key.handle(), Nan::New<Number>(msg->err));
            Nan::Set(errors, ++err_idx, obj);
            continue;
        }

        if (msg->key_len) {
            Nan::Set(obj, key_key.handle(), Nan::New<String>((char*)msg->key, msg->key_len).ToLocalChecked());
        }
        if (msg->len) {
            Nan::Set(obj, payload_key.handle(), Nan::New<String>((char*)msg->payload, msg->len).ToLocalChecked());
        }
        Nan::Set(messages, ++msg_idx, obj);
    }

    if (msg_idx > -1 || err_idx > -1) {
        Local<Value> argv[] = { messages, errors };
        recv_callback_->Call(2, argv);
    }

    if (!paused_) {
        looper_->resume();
    }
}
