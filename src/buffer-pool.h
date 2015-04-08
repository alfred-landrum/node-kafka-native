#pragma once

#include <v8.h>
#include <node.h>

// To amortize the overhead of allocating small Buffer objects for
// blob data, this object maintains a shared SlowBuffer object of
// a given size (default 10MB) and then allocates small Buffer
// objects out of this shared space.

class BufferPool {
public:
    BufferPool(const size_t page_size = 10 * 1024 * 1024);

    v8::Local<v8::Object> allocate(const unsigned char* data, size_t size);

private:
    static v8::Persistent<v8::Function> buffer_constructor_;
    size_t page_size_;

    node::Buffer* buffer_;
    char* buf_data_;
    size_t buf_offset_;
};
