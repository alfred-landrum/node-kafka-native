#include "producer.h"
#include "common.h"
#include <nan.h>
#include <v8.h>
#include <errno.h>
#include <iostream>
#include "persistent-string.h"

using namespace std;
using namespace v8;

void
Common::Init(const Local<FunctionTemplate> &tpl) {
}

void
ke_async_ready(uv_async_t* handle, int status)
{
    Common* common = (Common*)handle->data;
    common->ke_check();
}

Common::Common(rd_kafka_type_t ktype, v8::Local<v8::Object> &options):
    ktype_(ktype),
    kafka_client_(NULL),
    poll_thread_(0),
    shutting_(false)
{
    uv_mutex_init(&ke_queue_lock_);
    ke_async_.data = this;
    uv_async_init(uv_default_loop(), &ke_async_, ke_async_ready);

    NanAssignPersistent(options_, options);
}

void
Common::shutdown() {
    // TODO: stop the polling loop
}

Common::~Common()
{
    if (!options_.IsEmpty()) {
        NanDisposePersistent(options_);
    }

    for (auto& iter : topics_) {
        rd_kafka_topic_destroy(iter.second);
        iter.second = NULL;
    }
    // TODO: investigate rd_kafka_wait_destroyed
    if (kafka_client_) {
        rd_kafka_destroy(kafka_client_);
        kafka_client_ = NULL;
    }

    uv_mutex_destroy(&ke_queue_lock_);
    uv_close((uv_handle_t*)&ke_async_, NULL);
}

string
Common::rdk_error_string(int err) {
    return string(rd_kafka_err2str(rd_kafka_errno2err(err)));
}

// Event handling

class LogEvent : public KafkaEvent {
public:
    static void kafka_cb(const rd_kafka_t *rk, int level, const char *facility, const char *message) {
        // called from poller or kafka broker threads
        Common *common = (Common*)rd_kafka_opaque(rk);
        common->ke_push(unique_ptr<KafkaEvent>(new LogEvent(common, level, facility, message)));
    }

    virtual void v8_cb() {
        NanScope();
        if (common_->log_event_callback_) {
            Local<Value> argv[] = { toJsObject() };
            common_->log_event_callback_->Call(1, argv);
        }
    }

protected:
    Local<Object> toJsObject() {
        NanEscapableScope();

        static PersistentString level("level");
        static PersistentString facility("facility");
        static PersistentString message("message");

        Local<Object> obj = NanNew<Object>();
        obj->Set(level.handle(), NanNew<Number>(level_));
        obj->Set(facility.handle(), NanNew<String>(facility_));
        obj->Set(message.handle(), NanNew<String>(message_));

        return NanEscapeScope(obj);
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
        common->ke_push(unique_ptr<KafkaEvent>(new ErrorEvent(common, error, reason)));
    }

    virtual void v8_cb() {
        NanScope();
        if (common_->error_event_callback_) {
            Local<Value> argv[] = { toJsObject() };
            common_->error_event_callback_->Call(1, argv);
        }
    }

protected:
    Local<Object> toJsObject() {
        NanEscapableScope();

        static PersistentString error("error");
        static PersistentString reason("reason");

        Local<Object> jsobj = NanNew<Object>();

        jsobj->Set(error.handle(), NanNew<Number>(error_));
        jsobj->Set(reason.handle(), NanNew<String>(reason_));

        return NanEscapeScope(jsobj);
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
        common->ke_push(unique_ptr<KafkaEvent>(new StatEvent(common, json)));
        return 0; // 0 lets rd_kafka free json pointer
    }

    virtual void v8_cb() {
        NanScope();
        if (common_->stat_event_callback_) {
            Local<Value> argv[] = { NanNew<String>(stats_).As<Object>() };
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

void
Common::ke_push(unique_ptr<KafkaEvent> event) {
    // called from poller or kafka broker threads
    uv_mutex_lock(&ke_queue_lock_);
    ke_queue_.push_back(move(event));
    uv_mutex_unlock(&ke_queue_lock_);
    uv_async_send(&ke_async_);
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

void poller_trampoline(void *_common) {
    ((Common *)_common)->kafka_poller();
}

void
Common::kafka_poller() {
    // poller thread
    int timeout_ms = 500;
    while (!shutting_) {
        rd_kafka_poll(kafka_client_, timeout_ms);
    }
}

rd_kafka_topic_t*
Common::setup_topic(const char *name, std::string *error) {
    NanScope();

    rd_kafka_topic_t *topic = get_topic(name);
    if (topic) {
        return topic;
    }

    rd_kafka_topic_conf_t *conf = rd_kafka_topic_conf_new();

    const int errsize = 512;
    char errstr[errsize];

    static PersistentString topic_options_key("topic_options");
    Local<Object> topic_options = options_->Get(topic_options_key).As<Object>();
    if (topic_options != NanUndefined()) {
        Local<Array> keys = topic_options->GetOwnPropertyNames();
        for (size_t i = 0; i < keys->Length(); i++) {
            Local<Value> key = keys->Get(i);
            Local<Value> val = topic_options->Get(key);
            if (rd_kafka_topic_conf_set(conf, *NanAsciiString(key), *NanAsciiString(val),
                                        errstr, errsize) != RD_KAFKA_CONF_OK) {
                *error = string(errstr);
                rd_kafka_topic_conf_destroy(conf);
                return NULL;
            }
        }
    }

    topic = rd_kafka_topic_new(kafka_client_, name, conf);
    if (!topic) {
        int kafka_errno = errno;
        *error = rdk_error_string(kafka_errno);
        rd_kafka_topic_conf_destroy(conf);
        return NULL;
    }
    // conf now owned by rd_kafka
    conf = NULL;

    topics_.insert(make_pair(name, topic));

    return topic;
}

rd_kafka_topic_t*
Common::get_topic(const char *name) {
    auto iter(topics_.find(name));
    if (iter != topics_.end()) {
        return iter->second;
    }
    return NULL;
}

int
Common::common_init(std::string *error) {
    NanScope();

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set_opaque(conf, this);

    // callbacks
    static PersistentString stat_cb_key("stat_cb");
    Local<Function> stat_cb_fn = options_->Get(stat_cb_key).As<Function>();
    if (stat_cb_fn != NanUndefined()) {
        rd_kafka_conf_set_stats_cb(conf, StatEvent::kafka_cb);
        stat_event_callback_.reset(new NanCallback(stat_cb_fn));
    }

    static PersistentString error_cb("error_cb");
    Local<Function> error_cb_fn = options_->Get(error_cb).As<Function>();
    if (error_cb_fn != NanUndefined()) {
        rd_kafka_conf_set_error_cb(conf, ErrorEvent::kafka_cb);
        error_event_callback_.reset(new NanCallback(error_cb_fn));
    }

    static PersistentString log_cb("log_cb");
    Local<Function> log_cb_fn = options_->Get(log_cb).As<Function>();
    if (log_cb_fn != NanUndefined()) {
        rd_kafka_conf_set_log_cb(conf, LogEvent::kafka_cb);
        log_event_callback_.reset(new NanCallback(log_cb_fn));
    }

    const int errsize = 512;
    char errstr[errsize];

    // look for RD_KAFKA_DEBUG_CONTEXTS for debug options
    // rd_kafka_conf_set(conf, "debug", "all", errstr, sizeof(errstr));
    // rd_kafka_conf_set(conf, "debug", "generic,broker,producer,queue", errstr, sizeof(errstr));
    // rd_kafka_conf_set(conf, "debug", "topic", errstr, sizeof(errstr));

    static PersistentString driver_options_key("driver_options");
    Local<Object> driver_options = options_->Get(driver_options_key).As<Object>();
    if (driver_options != NanUndefined()) {
        Local<Array> keys = driver_options->GetOwnPropertyNames();
        for (size_t i = 0; i < keys->Length(); i++) {
            Local<Value> key = keys->Get(i);
            Local<Value> val = driver_options->Get(key);
            if (rd_kafka_conf_set(conf, *NanAsciiString(key), *NanAsciiString(val),
                                    errstr, errsize) != RD_KAFKA_CONF_OK) {
                *error = string(errstr);
                rd_kafka_conf_destroy(conf);
                return -1;
            }
        }
    }

    kafka_client_ = rd_kafka_new(ktype_, conf, errstr, sizeof(errstr));
    if (!kafka_client_) {
        *error = string(errstr);
        rd_kafka_conf_destroy(conf);
        return -1;
    }
    // conf now owned by rd_kafka
    conf = NULL;

    uv_thread_create(&poll_thread_, poller_trampoline, this);

    Ref();
    return 0;
}
