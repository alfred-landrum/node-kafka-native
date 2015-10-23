#pragma once

#include "nan.h"

// Simple helper class
class PersistentString {
public:
    typedef Nan::Persistent<v8::String> PersistentStringRef;
    
    PersistentString(const char* str) : string_(str) {
    }

    // Return a local handle to the persistent string
    v8::Local<v8::String> handle() {
        if (handle_.IsEmpty()) {
            v8::Local<v8::String> local = Nan::New<v8::String>(string_).ToLocalChecked();
            handle_.Reset(local);
        }
        return Nan::New<v8::String>(handle_);
    }

    // Define the casting operator to a local handle so that a persistent string
    // can be passed as a parameter to Object::Set()
    operator const v8::Handle<v8::Value>() {
        return handle();
    }

    operator const v8::Handle<v8::String>() {
        return handle();
    }

private:
    PersistentStringRef handle_;
    std::string string_;
};
