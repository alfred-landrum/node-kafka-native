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

    static v8::Persistent<v8::Function> constructor;
};
