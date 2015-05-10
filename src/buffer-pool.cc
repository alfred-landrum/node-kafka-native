#include <nan.h>
#include "buffer-pool.h"

using namespace v8;

v8::Persistent<v8::Function> BufferPool::buffer_constructor_;

BufferPool::BufferPool(const size_t page_size)
    : page_size_(page_size), buffer_(nullptr)
{
}

Local<Object>
BufferPool::allocate(const unsigned char* data, size_t size) {
    NanScope();

    if (buffer_constructor_.IsEmpty()) {
        // Grab a handle on the buffer constructor
        Local<Object> global = Context::GetCurrent()->Global();
        Local<Value> val = global->Get(String::New("Buffer"));
        assert(!val.IsEmpty() && "type not found: Buffer");
        assert(val->IsFunction() && "not a constructor: Buffer");
        buffer_constructor_ = Persistent<Function>::New(val.As<Function>());
    }

    if (buffer_ == nullptr || (buf_offset_ + size) > page_size_) {
        buffer_ = node::Buffer::New(std::max(page_size_, size));
        buf_data_ = node::Buffer::Data(buffer_->handle_);
        buf_offset_ = 0;
    }

    memcpy(buf_data_ + buf_offset_, data, size);

    Handle<Value> constructorArgs[3] = {
        buffer_->handle_, Integer::New(size), v8::Integer::New(buf_offset_)
    };
    buf_offset_ += size;

    return buffer_constructor_->NewInstance(3, constructorArgs);
}
