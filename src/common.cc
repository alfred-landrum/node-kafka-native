#include "producer.h"
#include "common.h"
#include <nan.h>
#include <v8.h>
#include <errno.h>
#include <iostream>
#include "persistent-string.h"

using namespace v8;

void
#if UV_VERSION_MAJOR == 0
ke_async_ready(uv_async_t* handle, int status)
#else
ke_async_ready(uv_async_t* handle)
#endif
{
    Common* common = (Common*)handle->data;
    common->ke_check();
}

void
ke_async_destroy(uv_handle_t* _ke_async)
{
    uv_async_t* ke_async = (uv_async_t*)_ke_async;
    delete ke_async;
}

Common::Common(rd_kafka_type_t ktype, Local<Object> &options):
    ktype_(ktype),
    kafka_client_(nullptr),
    poll_thread_(),
    ke_async_(nullptr),
    stop_called_(false),
    keep_polling_(true)
{
    uv_mutex_init(&ke_queue_lock_);
    ke_async_ = new uv_async_t();
    ke_async_->data = this;
    uv_async_init(uv_default_loop(), ke_async_, ke_async_ready);

    options_.Reset(options);
}

Common::~Common()
{
    if (!options_.IsEmpty()) {
        options_.Reset();
    }

    for (auto& iter : topics_) {
        rd_kafka_topic_destroy(iter.second);
        iter.second = nullptr;
    }

    if (kafka_client_) {
        rd_kafka_destroy(kafka_client_);
        kafka_client_ = nullptr;
    }

    uv_mutex_destroy(&ke_queue_lock_);
    uv_close((uv_handle_t*)ke_async_, ke_async_destroy);
    ke_async_ = nullptr;
}

std::string
Common::rdk_error_string(int err) {
    return std::string(rd_kafka_err2str(rd_kafka_errno2err(err)));
}

// Event handling

class LogEvent : public KafkaEvent {
public:
    static void kafka_cb(const rd_kafka_t *rk, int level, const char *facility, const char *message) {
        // called from poller or kafka broker threads
        Common *common = (Common*)rd_kafka_opaque(rk);
        common->ke_push(std::unique_ptr<KafkaEvent>(new LogEvent(common, level, facility, message)));
    }

    virtual void v8_cb() {
        Nan::HandleScope scope;
        if (common_->log_event_callback_) {
            Local<Value> argv[] = { toJsObject() };
            common_->log_event_callback_->Call(1, argv);
        }
    }

protected:
    Local<Object> toJsObject() {
        Nan::EscapableHandleScope scope;

        static PersistentString level_key("level");
        static PersistentString facility_key("facility");
        static PersistentString message_key("message");

        Local<Object> obj = Nan::New<Object>();
        Nan::Set(obj, level_key.handle(), Nan::New<Number>(level_));
        Nan::Set(obj, facility_key.handle(), Nan::New<String>(facility_).ToLocalChecked());
        Nan::Set(obj, message_key.handle(), Nan::New<String>(message_).ToLocalChecked());

        return scope.Escape(obj);
    }

    LogEvent(Common *common, int level, const char *facility, const char *message):
        KafkaEvent(),
        common_(common),
        level_(level),
        facility_(facility),
        message_(message)
    {}
    virtual ~LogEvent() {}

    Common *common_;
    int level_;
    std::string facility_;
    std::string message_;
};

class ErrorEvent : public KafkaEvent {
public:
    static void kafka_cb(rd_kafka_t *rk, int error, const char *reason, void* opaque) {
        // called from poller thread
        (void) rk;
        Common *common = (Common*)opaque;
        common->ke_push(std::unique_ptr<KafkaEvent>(new ErrorEvent(common, error, reason)));
    }

    virtual void v8_cb() {
        Nan::HandleScope scope;
        if (common_->error_event_callback_) {
            Local<Value> argv[] = { toJsObject() };
            common_->error_event_callback_->Call(1, argv);
        }
    }

protected:
    Local<Object> toJsObject() {
        Nan::EscapableHandleScope scope;

        static PersistentString error_key("error");
        static PersistentString reason_key("reason");

        Local<Object> jsobj = Nan::New<Object>();

        Nan::Set(jsobj, error_key.handle(), Nan::New<Number>(error_));
        Nan::Set(jsobj, reason_key.handle(), Nan::New<String>(reason_).ToLocalChecked());

        return scope.Escape(jsobj);
    }

    ErrorEvent(Common* common, int error, const char *reason):
        KafkaEvent(),
        common_(common),
        error_(error),
        reason_(reason)
    {}
    virtual ~ErrorEvent() {}

    Common *common_;
    int error_;
    std::string reason_;
};

class StatEvent : public KafkaEvent {
public:
    static int kafka_cb(rd_kafka_t *rk, char *json, size_t json_len, void *opaque) {
        // called from poller thread
        (void) rk;
        (void) json_len;
        Common *common = (Common*)opaque;
        common->ke_push(std::unique_ptr<KafkaEvent>(new StatEvent(common, json)));
        return 0; // 0 lets rd_kafka free json pointer
    }

    virtual void v8_cb() {
        Nan::HandleScope scope;
        if (common_->stat_event_callback_) {
            Local<Value> argv[] = { Nan::New<String>(stats_).ToLocalChecked() };
            common_->stat_event_callback_->Call(1, argv);
        }
    }

protected:
    StatEvent(Common *common, const char *stats):
        KafkaEvent(),
        common_(common),
        stats_(stats)
    {}
    virtual ~StatEvent() {}

    Common *common_;
    std::string stats_;
};

class PollStopped : public KafkaEvent {
public:
    PollStopped(Common *common):
        KafkaEvent(),
        common_(common)
    {}

    virtual ~PollStopped() { }

    virtual void v8_cb() {
        common_->poll_stopped();
    }

    Common *common_;
};

void
Common::ke_push(std::unique_ptr<KafkaEvent> event) {
    // called from poller or kafka broker threads
    uv_mutex_lock(&ke_queue_lock_);
    ke_queue_.push_back(std::move(event));
    uv_mutex_unlock(&ke_queue_lock_);
    uv_async_send(ke_async_);
}

void
Common::ke_check()
{
    // run from v8 thread
    do {
        decltype(ke_queue_) events;

        uv_mutex_lock(&ke_queue_lock_);
        if (ke_queue_.empty()) {
            uv_mutex_unlock(&ke_queue_lock_);
            break;
        }
        ke_queue_.swap(events);
        uv_mutex_unlock(&ke_queue_lock_);

        for (auto& event : events) {
            event->v8_cb();
        }
    } while (true);
}

void
poller_trampoline(void *_common) {
    ((Common *)_common)->kafka_poller();
}

void
Common::kafka_poller() {
    // This is a separate thread that exists solely to make the below call to
    // rd_kafka_poll, which is how the stats, error, and log callbacks execute.
    const int timeout_ms = 500;
    while (keep_polling_) {
        rd_kafka_poll(kafka_client_, timeout_ms);
    }
    ke_push(std::unique_ptr<KafkaEvent>(new PollStopped(this)));
}

void
Common::start_poll() {
    Ref();

    uv_thread_create(&poll_thread_, poller_trampoline, this);
}

void
Common::stop_poll() {
    keep_polling_ = false;
}

void
Common::poll_stopped() {
    Unref();
}

rd_kafka_topic_t*
Common::setup_topic(const char *name, std::string *error) {
    Nan::HandleScope scope;

    rd_kafka_topic_t *topic = get_topic(name);
    if (topic) {
        return topic;
    }

    rd_kafka_topic_conf_t *conf = rd_kafka_topic_conf_new();

    const int errsize = 512;
    char errstr[errsize];

    static PersistentString topic_options_key("topic_options");
    Local<Object> topic_options = Nan::Get(Nan::New(options_), topic_options_key).ToLocalChecked().As<Object>();
    if (topic_options != Nan::Undefined()) {
        Local<Array> keys = Nan::GetOwnPropertyNames(topic_options).ToLocalChecked();
        for (size_t i = 0; i < keys->Length(); i++) {
            Local<Value> key = Nan::Get(keys, i).ToLocalChecked();
            Local<Value> val = Nan::Get(topic_options, key).ToLocalChecked();
            if (val == Nan::Undefined()) {
                continue;
            }
            if (rd_kafka_topic_conf_set(conf, *Nan::Utf8String(key), *Nan::Utf8String(val),
                                        errstr, errsize) != RD_KAFKA_CONF_OK) {
                *error = std::string(errstr);
                rd_kafka_topic_conf_destroy(conf);
                return nullptr;
            }
        }
    }

    topic = rd_kafka_topic_new(kafka_client_, name, conf);
    if (!topic) {
        int kafka_errno = errno;
        *error = rdk_error_string(kafka_errno);
        rd_kafka_topic_conf_destroy(conf);
        return nullptr;
    }
    // conf now owned by rd_kafka
    conf = nullptr;

    topics_.insert(std::make_pair(name, topic));

    return topic;
}

rd_kafka_topic_t*
Common::get_topic(const char *name) {
    auto iter(topics_.find(name));
    if (iter != topics_.end()) {
        return iter->second;
    }
    return nullptr;
}

int
Common::common_init(rd_kafka_conf_t *conf, std::string *error) {
    Nan::HandleScope scope;

    // Convert persistent options to local for >node 0.12 compatibility
    Local<Object> options = Nan::New(options_);

    // callbacks
    static PersistentString stat_cb_key("stat_cb");
    Local<Function> stat_cb_fn = Nan::Get(options, stat_cb_key).ToLocalChecked().As<Function>();
    if (stat_cb_fn != Nan::Undefined()) {
        rd_kafka_conf_set_stats_cb(conf, StatEvent::kafka_cb);
        stat_event_callback_.reset(new Nan::Callback(stat_cb_fn));
    }

    static PersistentString error_cb_key("error_cb");
    Local<Function> error_cb_fn = Nan::Get(options, error_cb_key).ToLocalChecked().As<Function>();
    if (error_cb_fn != Nan::Undefined()) {
        rd_kafka_conf_set_error_cb(conf, ErrorEvent::kafka_cb);
        error_event_callback_.reset(new Nan::Callback(error_cb_fn));
    }

    static PersistentString log_cb_key("log_cb");
    Local<Function> log_cb_fn = Nan::Get(options, log_cb_key).ToLocalChecked().As<Function>();
    if (log_cb_fn != Nan::Undefined()) {
        rd_kafka_conf_set_log_cb(conf, LogEvent::kafka_cb);
        log_event_callback_.reset(new Nan::Callback(log_cb_fn));
    }

    const int errsize = 512;
    char errstr[errsize];

    // look for RD_KAFKA_DEBUG_CONTEXTS for debug options
    // rd_kafka_conf_set(conf, "debug", "all", errstr, sizeof(errstr));
    // rd_kafka_conf_set(conf, "debug", "generic,broker,producer,queue", errstr, sizeof(errstr));
    // rd_kafka_conf_set(conf, "debug", "topic", errstr, sizeof(errstr));

    static PersistentString driver_options_key("driver_options");
    Local<Object> driver_options = Nan::Get(options, driver_options_key).ToLocalChecked().As<Object>();
    if (driver_options != Nan::Undefined()) {
        Local<Array> keys = Nan::GetOwnPropertyNames(driver_options).ToLocalChecked();
        for (size_t i = 0; i < keys->Length(); i++) {
            Local<Value> key = Nan::Get(keys, i).ToLocalChecked();
            Local<Value> val = Nan::Get(driver_options, key).ToLocalChecked();
            if (val == Nan::Undefined()) {
                continue;
            }
            if (rd_kafka_conf_set(conf, *Nan::Utf8String(key), *Nan::Utf8String(val),
                                    errstr, errsize) != RD_KAFKA_CONF_OK) {
                *error = std::string(errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
            }
        }
    }

    kafka_client_ = rd_kafka_new(ktype_, conf, errstr, sizeof(errstr));
    if (!kafka_client_) {
        *error = std::string(errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }
    // conf now owned by rd_kafka
    conf = nullptr;

    return 0;
}

Local<Object>
metadata_to_jsobj(struct rd_kafka_metadata *metadata) {
    Nan::EscapableHandleScope scope;

    static PersistentString error_key("error");

    Local<Object> obj = Nan::New<Object>();

    static PersistentString orig_broker_id_key("orig_broker_id");
    static PersistentString orig_broker_name_key("orig_broker_name");
    Nan::Set(obj, orig_broker_id_key.handle(), Nan::New<Number>(metadata->orig_broker_id));
    Nan::Set(obj, orig_broker_name_key.handle(), Nan::New<String>(metadata->orig_broker_name).ToLocalChecked());

    static PersistentString brokers_key("brokers");
    static PersistentString id_key("id");
    static PersistentString host_key("host");
    static PersistentString port_key("port");

    Local<Object> brokers_obj = Nan::New<Object>();
    for (int i = 0; i < metadata->broker_cnt; ++i) {
        struct rd_kafka_metadata_broker *broker = &metadata->brokers[i];
        Local<Object> broker_obj = Nan::New<Object>();
        Nan::Set(broker_obj, id_key.handle(), Nan::New<Number>(broker->id));
        Nan::Set(broker_obj, host_key.handle(), Nan::New<String>(broker->host).ToLocalChecked());
        Nan::Set(broker_obj, port_key.handle(), Nan::New<Number>(broker->port));
        Nan::Set(brokers_obj, i, broker_obj);
    }
    Nan::Set(obj, brokers_key.handle(), brokers_obj);

    static PersistentString topics_key("topics");
    static PersistentString partitions_key("partitions");
    static PersistentString topic_key("topic");
    static PersistentString leader_key("leader");
    static PersistentString replicas_key("replicas");
    static PersistentString isrs_key("isrs");

    Local<Object> topics_obj = Nan::New<Array>();
    for (int i = 0; i < metadata->topic_cnt; ++i) {
        struct rd_kafka_metadata_topic *topic = &metadata->topics[i];
        Local<Object> topic_obj = Nan::New<Object>();
        Nan::Set(topic_obj, topic_key.handle(), Nan::New<String>(topic->topic).ToLocalChecked());
        Nan::Set(topic_obj, error_key.handle(), Nan::New<Number>(topic->err));

        Local<Object> partitions_obj = Nan::New<Array>();
        for (int j = 0; j < topic->partition_cnt; ++j) {
            struct rd_kafka_metadata_partition *partition = &topic->partitions[j];
            Local<Object> partition_obj = Nan::New<Object>();
            Nan::Set(partition_obj, id_key.handle(), Nan::New<Number>(partition->id));
            Nan::Set(partition_obj, error_key.handle(), Nan::New<Number>(partition->err));
            Nan::Set(partition_obj, leader_key.handle(), Nan::New<Number>(partition->leader));

            Local<Object> replicas_obj = Nan::New<Array>();
            for (int k = 0; k < partition->replica_cnt; ++k) {
                Nan::Set(replicas_obj, k, Nan::New<Number>(partition->replicas[k]));
            }
            Nan::Set(partition_obj, replicas_key.handle(), replicas_obj);

            Local<Object> isrs_obj = Nan::New<Array>();
            for (int k = 0; k < partition->isr_cnt; ++k) {
                Nan::Set(isrs_obj, k, Nan::New<Number>(partition->isrs[k]));
            }
            Nan::Set(partition_obj, isrs_key.handle(), isrs_obj);

            Nan::Set(partitions_obj, j, partition_obj);
        }
        Nan::Set(topic_obj, partitions_key.handle(), partitions_obj);

        Nan::Set(topics_obj, i, topic_obj);
    }
    Nan::Set(obj, topics_key.handle(), topics_obj);

    return scope.Escape(obj);
}


class MetadataWorker : public Nan::AsyncWorker {
public:
    MetadataWorker( Nan::Callback *callback,
                    rd_kafka_t *client,
                    rd_kafka_topic_t *topic):
            Nan::AsyncWorker(callback),
            client_(client),
            topic_(topic),
            metadata_(nullptr)
    {}

    ~MetadataWorker() {
        if (metadata_) {
            rd_kafka_metadata_destroy(metadata_);
            metadata_ = nullptr;
        }
    }

    void Execute() {
        // worker thread
        rd_kafka_resp_err_t err;
        const int timeout_ms = 1000;

        err = rd_kafka_metadata(client_, /*all_topics*/ 0, topic_,
            (const struct rd_kafka_metadata **) &metadata_, timeout_ms);
        if (err) {
            std::string errstr(Common::rdk_error_string(err));
            SetErrorMessage(errstr.c_str());
        }
    }

    // Executed when the async work is complete
    // this function will be run inside the main event loop
    // so it is safe to use V8 again
    void HandleOKCallback () {
        Nan::HandleScope scope;

        Local<Value> argv[] = {
            Nan::Null(),
            metadata_to_jsobj(metadata_)
        };

        callback->Call(2, argv);
    }

private:
    rd_kafka_t *client_;
    rd_kafka_topic_t *topic_;
    struct rd_kafka_metadata *metadata_;
};

NAN_METHOD(Common::get_metadata)
{

    if (info.Length() != 2 ||
        !( info[0]->IsString() && info[1]->IsFunction()) ) {
        Nan::ThrowError("you must supply a topic name and callback");
        return;
    }

    String::Utf8Value topic_name(info[0]);
    rd_kafka_topic_t *topic = get_topic(*topic_name);
    if (!topic) {
        std::string error;
        topic = setup_topic(*topic_name, &error);
        if (!topic) {
            Nan::ThrowError(error.c_str());
            return;
        }
    }

    Nan::Callback *callback = new Nan::Callback(info[1].As<Function>());
    Nan::AsyncQueueWorker(new MetadataWorker(callback, kafka_client_, topic));
    return;
}
