#pragma once

#include <node.h>
#include <nan.h>
#include "common.h"
#include "wrapped-method.h"

class Producer : public Common {
public:
    static void Init();
    static v8::Local<v8::Object> NewInstance(v8::Local<v8::Value> arg);
    int producer_init(std::string *error);

private:
    explicit Producer(v8::Local<v8::Object> &options);
    ~Producer();
    Producer(const Producer &) = delete;
    Producer &operator=(const Producer &) = delete;

    static NAN_METHOD(New);

    WRAPPED_METHOD_DECL(GetMetadata);
    WRAPPED_METHOD_DECL(OutQueueLength);
    WRAPPED_METHOD_DECL(Send);
    WRAPPED_METHOD_DECL(Stop);

    static Nan::Persistent<v8::Function> constructor;

    friend class DeliveryReportEvent;
    void dr_cb(rd_kafka_resp_err_t err);
    void dr_cb_v8();
    uv_mutex_t dr_lock_;
    std::map<rd_kafka_resp_err_t, size_t> dr_results_;
    std::unique_ptr<Nan::Callback> dr_callback_;
};
