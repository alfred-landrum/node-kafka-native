#pragma once

#define WRAPPED_METHOD_NAME(_name) JS##_name

#define WRAPPED_METHOD(_cls, _name) \
    NAN_METHOD(_cls::WRAPPED_METHOD_NAME(_name)) { \
        _cls* obj = node::ObjectWrap::Unwrap<_cls>(args.Holder()); \
        return obj->_name(args); \
    } \
    NAN_METHOD(_cls::_name)

#define WRAPPED_METHOD_DECL(_name) \
    static NAN_METHOD(WRAPPED_METHOD_NAME(_name)); \
    NAN_METHOD(_name);

#define WRAPPED_METHOD(_cls, _name) \
    NAN_METHOD(_cls::WRAPPED_METHOD_NAME(_name)) { \
        _cls* obj = node::ObjectWrap::Unwrap<_cls>(args.Holder()); \
        return obj->_name(args); \
    } \
    NAN_METHOD(_cls::_name)
