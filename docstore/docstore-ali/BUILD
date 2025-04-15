load("//tools/junit5:junit5.bzl", "java_junit5_test")

deps = [
    "@com_google//:com_google_auto_auto_common",
    "@com_google//:com_google_auto_service_auto_service",
    "@com_google//:com_google_auto_service_auto_service_annotations",
    "@com_google//:com_google_guava_guava",
    "@org_apache//:org_apache_commons_commons_lang3",
    "//tools/maven_install/managed/restricted:cag_com_aliyun_aliyun_java_sdk_core",
    "//tools/maven_install/managed/restricted:cag_com_aliyun_openservices_tablestore",
    "//tools/common/java/lombok",
    "//projects/libs/chameleon/multicloudj/docstore/docstore-client:docstore-client",
    "//projects/libs/chameleon/multicloudj/multicloudj-common",
    "//projects/libs/chameleon/multicloudj/sts/sts-client",
]

runtime_deps = [
]

java_plugin(
    name = "auto_service_plugin",
    processor_class = "com.google.auto.service.processor.AutoServiceProcessor",
    deps = [
        "@com_google//:com_google_auto_service_auto_service",
    ],
)

java_library(
    name = "docstore-ali",
    srcs = glob(["src/main/java/**/*.java"]),
    javacopts = ["--release 11"],
    plugins = [
        ":auto_service_plugin",
    ],
    visibility = ["//visibility:public"],
    runtime_deps = runtime_deps,
    deps = deps,
)

test_deps = [
    "//tools/junit5:junit5_deps",
    "//projects/libs/chameleon/multicloudj/docstore/docstore-client:docstore_client_junit",
    "//projects/libs/chameleon/multicloudj/multicloudj-common:multicloudj_common_junit",
    "@com_google//:com_google_protobuf_protobuf_java",
    "@test//:org_mockito_mockito_junit_jupiter",
    "@test//:org_mockito_mockito_core",
    "@test//:org_mockito_mockito_inline",
]

java_library(
    name = "docstore-ali-junit",
    testonly = True,
    srcs = glob(["src/test/java/**/*.java"]),
    runtime_deps = runtime_deps,
    deps = [
        ":docstore-ali",
    ] + test_deps + deps,
)

""" Disabling per security guidance to not ali unit tests
[java_junit5_test(
    name = name[:-len(".java")],
    size = "small",
    runtime_deps = [":docstore-ali-junit"],
) for name in glob([
    "src/test/java/**/*Test.java",
    "src/test/java/**/*IT.java",
])]
"""
