#pragma once

#include <node.h>
#include <nan.h>
#include <utility>
#include "common.h"
#include "wrapped-method.h"
#include "buffer-pool.h"

class Consumer : public Common {
public:
    static void Init();
    static v8::Local<v8::Object> NewInstance(v8::Local<v8::Value> arg);
    int consumer_init(std::string *error);

    void kafka_consumer();
    void kafka_recv(const std::vector<rd_kafka_message_t*> &vec);

private:
    explicit Consumer(v8::Local<v8::Object> &options);
    ~Consumer();
    Consumer(const Consumer &) = delete;
    Consumer &operator=(const Consumer &) = delete;

    static NAN_METHOD(New);

    WRAPPED_METHOD_DECL(SetRecvCallback);
    WRAPPED_METHOD_DECL(StartRecv);
    WRAPPED_METHOD_DECL(StopRecv);
    WRAPPED_METHOD_DECL(SetOffset);
    WRAPPED_METHOD_DECL(GetMetadata);

    static v8::Persistent<v8::Function> constructor;

    std::vector<std::pair<rd_kafka_topic_t*, uint32_t> > toppars_;
    std::unique_ptr<NanCallback> recv_callback_;

    uv_thread_t consume_thread_;
    rd_kafka_queue_t *kafka_queue_;
    bool shutdown_;

    BufferPool buffer_pool_;
    v8::Local<v8::Value> create_object(const unsigned char* data, size_t size);
};
