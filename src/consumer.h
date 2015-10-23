#pragma once

#include <node.h>
#include <nan.h>
#include <utility>
#include "common.h"
#include "wrapped-method.h"

class ConsumerLoop;

class Consumer : public Common {
public:
    static void Init();
    static v8::Local<v8::Object> NewInstance(v8::Local<v8::Value> arg);
    int consumer_init(std::string *error);

    void receive(ConsumerLoop *looper, const std::vector<rd_kafka_message_t*> &vec);
    void looper_stopped(ConsumerLoop *looper);

    uint32_t max_messages_per_callback() { return max_messages_per_callback_; }

private:
    explicit Consumer(v8::Local<v8::Object> &options);
    ~Consumer();
    Consumer(const Consumer &) = delete;
    Consumer &operator=(const Consumer &) = delete;

    static NAN_METHOD(New);

    WRAPPED_METHOD_DECL(Start);
    WRAPPED_METHOD_DECL(Stop);
    WRAPPED_METHOD_DECL(Pause);
    WRAPPED_METHOD_DECL(Resume);
    WRAPPED_METHOD_DECL(GetMetadata);

    static Nan::Persistent<v8::Function> constructor;

    rd_kafka_topic_t *topic_;
    std::vector<uint32_t> partitions_;
    ConsumerLoop *looper_;
    rd_kafka_queue_t *queue_;
    bool paused_;
    std::unique_ptr<Nan::Callback> recv_callback_;
    uint32_t max_messages_per_callback_;
};
