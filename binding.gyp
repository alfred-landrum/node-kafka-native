{
  "targets": [
    {
      "target_name": "kafka-native",
      "sources": [
        "src/kafka-native.cc",
        "src/common.cc",
        "src/producer.cc",
        "src/consumer.cc"
      ],
      'dependencies': [
        'librdkafka'
      ],
      "include_dirs": [
            "<!(node -e \"require('nan')\")",
            "deps/librdkafka/src"
      ],
      'conditions': [
        [
          'OS=="mac"',
          {
            'xcode_settings': {
              'MACOSX_DEPLOYMENT_TARGET': '10.11'
            },
            'libraries' : ['-lz']
          }
        ],[
          'OS=="linux" and gcc_version<=46',
          {
            'cflags': ['-std=c++0x','-g'],
            'libraries' : ['-lz']
          }
        ],[
          'OS=="linux" and gcc_version>46',
          {
            'cflags': ['-std=c++11','-g'],
            'libraries' : ['-lz']
          }
        ]
      ]
    },
    {
      "target_name": "librdkafka_config_h",
      "type": "none",
      "actions": [
        {
          'action_name': 'configure_librdkafka',
          'message': 'configuring librdkafka...',
          'inputs': [
            'deps/librdkafka/configure',
          ],
          'outputs': [
            'deps/librdkafka/config.h',
          ],
          'action': ['eval', 'cd deps/librdkafka && ./configure'],
        },
      ],
    },
    {
      "target_name": "librdkafka",
      "type": "static_library",
      'dependencies': [
        'librdkafka_config_h',
      ],
      "sources": [
        "deps/librdkafka/src/rd.c",
        "deps/librdkafka/src/rdaddr.c",
        "deps/librdkafka/src/rdcrc32.c",
        "deps/librdkafka/src/rdgz.c",
        "deps/librdkafka/src/rdkafka.c",
        "deps/librdkafka/src/rdkafka_broker.c",
        "deps/librdkafka/src/rdkafka_defaultconf.c",
        "deps/librdkafka/src/rdkafka_msg.c",
        "deps/librdkafka/src/rdkafka_offset.c",
        "deps/librdkafka/src/rdkafka_timer.c",
        "deps/librdkafka/src/rdkafka_topic.c",
        "deps/librdkafka/src/rdlog.c",
        "deps/librdkafka/src/rdqueue.c",
        "deps/librdkafka/src/rdrand.c",
        "deps/librdkafka/src/rdthread.c",
        "deps/librdkafka/src/snappy.c"
      ],
      'conditions': [
        [
          'OS=="mac"',
          {
            'xcode_settings': {
              'MACOSX_DEPLOYMENT_TARGET': '10.11',
              'OTHER_CFLAGS' : ['-Wno-sign-compare', '-Wno-missing-field-initializers'],
            },
            'libraries' : ['-lz']
          }
        ],[
          'OS=="linux"',
          {
            'cflags' : [ '-Wno-sign-compare', '-Wno-missing-field-initializers', '-Wno-empty-body', '-g'],
          }
        ]
      ]
    }
  ]
}
