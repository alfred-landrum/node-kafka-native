#include "producer.h"
#include <nan.h>
#include <v8.h>
#include <errno.h>
#include <iostream>

#include "persistent-string.h"

using namespace v8;

Producer::Producer(Local<Object> &options):
    Common(RD_KAFKA_PRODUCER, options),
    dr_results_(),
    dr_callback_()
{
    uv_mutex_init(&dr_lock_);
}

Producer::~Producer()
{
}

Nan::Persistent<Function> Producer::constructor;

void
Producer::Init() {
    Nan::HandleScope scope;

    // Prepare constructor template
    Local<FunctionTemplate> tpl = Nan::New<FunctionTemplate>(New);
    tpl->SetClassName(Nan::New("Producer").ToLocalChecked());
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    Nan::SetPrototypeMethod(tpl, "send", WRAPPED_METHOD_NAME(Send));
    Nan::SetPrototypeMethod(tpl, "get_metadata", WRAPPED_METHOD_NAME(GetMetadata));
    Nan::SetPrototypeMethod(tpl, "outq_length", WRAPPED_METHOD_NAME(OutQueueLength));
    Nan::SetPrototypeMethod(tpl, "stop", WRAPPED_METHOD_NAME(Stop));

    constructor.Reset(tpl->GetFunction());
}

Local<Object>
Producer::NewInstance(Local<Value> arg) {
    Nan::EscapableHandleScope scope;

    const unsigned argc = 1;
    Local<Value> argv[argc] = { arg };
    Local<Function> cons = Nan::New<Function>(constructor);
    Local<Object> instance = Nan::NewInstance(cons, argc, argv).ToLocalChecked();

    return scope.Escape(instance);
}

NAN_METHOD(Producer::New) {

    if (!info.IsConstructCall()) {
        return Nan::ThrowError("non-constructor invocation not supported");
    }

    Local<Object> options(Nan::New<Object>());

    if (info.Length() == 1 && info[0] != Nan::Undefined()) {
        options = info[0].As<Object>();
    }

    Producer* obj = new Producer(options);
    obj->Wrap(info.This());

    std::string error;
    if (obj->producer_init(&error)) {
        Nan::ThrowError(error.c_str());
        return;
    }

    info.GetReturnValue().Set(info.This());
}

class DeliveryReportEvent : public KafkaEvent {
public:
    DeliveryReportEvent(Producer *producer):
        KafkaEvent(),
        producer_(producer)
    {}

    static void kafka_cb(rd_kafka_t *rk, void *payload, size_t len, rd_kafka_resp_err_t err, void *opaque, void *msg_opaque) {
        (void)rk;
        (void)payload;
        (void)len;
        (void)opaque;
        (void)msg_opaque;
        Producer *producer = (Producer*)opaque;
        producer->dr_cb(err);
    }

    virtual ~DeliveryReportEvent() {}

    virtual void v8_cb() {
        producer_->dr_cb_v8();
    }

    Producer *producer_;
};

void
Producer::dr_cb(rd_kafka_resp_err_t err) {
    // Called in librdkafka's polling thread.
    uv_mutex_lock(&dr_lock_);

    bool send_event = dr_results_.empty();

    auto iter(dr_results_.find(err));
    if (iter != dr_results_.end()) {
        iter->second++;
    } else {
        dr_results_.insert(std::make_pair(err, 1));
    }

    uv_mutex_unlock(&dr_lock_);

    if (send_event) {
        ke_push(std::unique_ptr<KafkaEvent>(new DeliveryReportEvent(this)));
    }
}

void
Producer::dr_cb_v8() {
    Nan::HandleScope scope;

    decltype(dr_results_) results;

    uv_mutex_lock(&dr_lock_);
    if (dr_results_.empty()) {
        uv_mutex_unlock(&dr_lock_);
        return;
    }

    dr_results_.swap(results);
    uv_mutex_unlock(&dr_lock_);

    if (!dr_callback_) {
        return;
    }

    static PersistentString errcode_key("errcode");
    static PersistentString count_key("count");
    static PersistentString errstr_key("errstr");

    int pos = 0;
    Local<Array> result_arr(Nan::New<Array>());

    for (auto& iter : results) {
        Local<Object> obj(Nan::New<Object>());
        Nan::Set(obj, errcode_key.handle(), Nan::New<Number>(iter.first));
        Nan::Set(obj, count_key.handle(), Nan::New<Number>(iter.second));
        Nan::Set(obj, errstr_key.handle(), Nan::New<String>(rd_kafka_err2str(iter.first)).ToLocalChecked());
        Nan::Set(result_arr, pos++, obj);
    }

    Local<Value> argv[] = { result_arr };
    dr_callback_->Call(1, argv);
}

int
Producer::producer_init(std::string *error_str) {
    Nan::HandleScope scope;

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set_opaque(conf, this);

    Local<Object> options = Nan::New(options_);

    static PersistentString dr_cb_key("dr_cb");
    Local<Function> dr_cb_fn = Nan::Get(options, dr_cb_key).ToLocalChecked().As<Function>();
    if (dr_cb_fn != Nan::Undefined()) {
        dr_callback_.reset(new Nan::Callback(dr_cb_fn));
        rd_kafka_conf_set_dr_cb(conf, DeliveryReportEvent::kafka_cb);
    }

    int err = common_init(conf, error_str);
    if (err) {
        return err;
    }

    start_poll();

    return 0;
}

WRAPPED_METHOD(Producer, Send) {
    Nan::HandleScope scope;

    if (stop_called_) {
        Nan::ThrowError("already shutdown");
        return;
    }

    if (info.Length() != 3 ||
        !( info[0]->IsString() && info[1]->IsNumber() && info[2]->IsArray()) ) {
        Nan::ThrowError("you must supply a topic name, partition, and array of strings");
        return;
    }

    String::Utf8Value topic_name(info[0]);
    rd_kafka_topic_t *topic = get_topic(*topic_name);
    if (!topic) {
        std::string error;
        topic = setup_topic(*topic_name, &error);
        if (!topic) {
            Nan::ThrowError(error.c_str());
            return;
        }
    }

    int32_t partition = info[1].As<Number>()->Int32Value();
    Local<Array> msg_array(info[2].As<Array>());
    uint32_t message_cnt = msg_array->Length();

    std::unique_ptr<rd_kafka_message_t[]> holder(new rd_kafka_message_t[message_cnt]());
    rd_kafka_message_t *messages = holder.get();

    for (uint32_t i = 0; i < message_cnt; ++i) {
        rd_kafka_message_t *msg = &messages[i];
        const Local<String>& str(Nan::Get(msg_array, i).ToLocalChecked().As<String>());

        int length = str->Utf8Length();
        msg->len = length;
        // malloc here to match the F_FREE flag below
        msg->payload = malloc(length);
        // WriteUtf8 in v8/src/api.cc
        str->WriteUtf8((char*)msg->payload, length, nullptr, 0);

        // ensure errors aren't missed by checking that
        // prodce_batch clears the error for good messages
        msg->err = RD_KAFKA_RESP_ERR_UNKNOWN;
    }

    uint32_t sent = rd_kafka_produce_batch(
        topic, partition, RD_KAFKA_MSG_F_FREE, messages, message_cnt);

    if (sent != message_cnt) {
        // Since rdkafka didn't take ownership of these,
        // we need to free them ourselves.
        for (uint32_t i = 0; i < message_cnt; ++i) {
            rd_kafka_message_t *msg = &messages[i];
            if (msg->err != RD_KAFKA_RESP_ERR_NO_ERROR) {
                free(msg->payload);
            }
        }
    }

    Local<Object> ret(Nan::New<Object>());
    static PersistentString queue_length_key("queue_length");
    static PersistentString queued_key("queued");

    Nan::Set(ret, queue_length_key.handle(), Nan::New<Number>(rd_kafka_outq_len(kafka_client_)));
    Nan::Set(ret, queued_key.handle(), Nan::New<Number>(sent));

    info.GetReturnValue().Set(ret);
}

WRAPPED_METHOD(Producer, GetMetadata) {
    Nan::HandleScope scope;

    if (stop_called_) {
        Nan::ThrowError("already shutdown");
        return;
    }

    get_metadata(info);

    return;
}

WRAPPED_METHOD(Producer, OutQueueLength) {
    Nan::HandleScope scope;

    if (stop_called_) {
        Nan::ThrowError("already shutdown");
        return;
    }

    int qlen = rd_kafka_outq_len(kafka_client_);

    info.GetReturnValue().Set(Nan::New<Number>(qlen));
}

WRAPPED_METHOD(Producer, Stop) {
    Nan::HandleScope scope;

    stop_called_ = true;
    stop_poll();

    return;
}
