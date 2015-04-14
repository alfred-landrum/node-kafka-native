#pragma once
#include <string>
#include <memory>
#include <vector>
#include <unordered_map>
#include <node.h>
#include <nan.h>

#include "rdkafka.h"

class KafkaEvent {
public:
    virtual ~KafkaEvent() {}
    KafkaEvent(const KafkaEvent &) = delete;
    KafkaEvent &operator=(const KafkaEvent &) = delete;

    virtual void v8_cb() = 0;

protected:
    KafkaEvent() {}
};

class Common : public node::ObjectWrap {
public:
    explicit Common(rd_kafka_type_t ktype, v8::Local<v8::Object> &options);
    ~Common();
    Common(const Common &) = delete;
    Common &operator=(const Common &) = delete;

    static void Init(const v8::Local<v8::FunctionTemplate> &tpl);
    int common_init(std::string *error);

    NAN_METHOD(get_metadata);

    v8::Persistent<v8::Object> options_;

    std::vector<std::unique_ptr<KafkaEvent> > ke_queue_;
    void ke_push(std::unique_ptr<KafkaEvent> event);
    void ke_check();
    void ke_callback(std::unique_ptr<KafkaEvent> event);
    void kafka_poller();

    static std::string rdk_error_string(int err);

    rd_kafka_topic_t* setup_topic(const char *name, std::string *error);
    rd_kafka_topic_t* get_topic(const char *name);

    rd_kafka_type_t ktype_;
    rd_kafka_t *kafka_client_;
    std::unordered_map<std::string, rd_kafka_topic_t*> topics_;

    // callbacks
    std::unique_ptr<NanCallback> stat_event_callback_;
    std::unique_ptr<NanCallback> error_event_callback_;
    std::unique_ptr<NanCallback> log_event_callback_;

    // rd_kafka_poll
    uv_thread_t poll_thread_;
    uv_async_t ke_async_;
    uv_mutex_t ke_queue_lock_;

    // shutting down
    void shutdown();
    bool shutting_;
};
