load("//tools/junit5:junit5.bzl", "java_junit5_test")

deps = [
    "@awssdk//:software_amazon_awssdk_s3",
    "@awssdk//:software_amazon_awssdk_sts",
    "@awssdk//:software_amazon_awssdk_auth",
    "@awssdk//:software_amazon_awssdk_regions",
    "@awssdk//:software_amazon_awssdk_sdk_core",
    "@awssdk//:software_amazon_awssdk_aws_core",
    "@awssdk//:software_amazon_awssdk_dynamodb",
    "@com_google//:com_google_auto_service_auto_service",
    "@com_google//:com_google_auto_service_auto_service_annotations",
    "@commons//:commons_logging_commons_logging",
    "@com_google//:com_google_guava_guava",
    "@org_slf4j//:org_slf4j_slf4j_api",
    "@com_google//:com_google_auto_auto_common",
    "@org_apache//:org_apache_commons_commons_lang3",
    "//projects/libs/chameleon/multicloudj/docstore/docstore-client:docstore-client",
    "//projects/libs/chameleon/multicloudj/multicloudj-common",
    "//projects/libs/chameleon/multicloudj/multicloudj-common-aws",
    "//projects/libs/chameleon/multicloudj/sts/sts-client",
    "//tools/common/java/lombok",
    "@nexus//:org_projectlombok_lombok",
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
    name = "docstore-aws",
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
    "//projects/libs/chameleon/multicloudj/multicloudj-common-aws:multicloudj_common_aws_junit",
    "@test//:org_mockito_mockito_junit_jupiter",
    "@test//:org_mockito_mockito_core",
    "@test//:org_mockito_mockito_inline",
    "@awssdk//:software_amazon_awssdk_http_client_spi",
    "@ch_qos_logback//:ch_qos_logback_logback_classic",
    "@com_google//:com_google_protobuf_protobuf_java",
]

java_library(
    name = "docstore-aws-junit",
    testonly = True,
    srcs = glob(["src/test/java/**/*.java"]),
    data = glob(["src/test/resources/**/*"]),
    resources = glob(["src/test/resources/**/*"]),
    runtime_deps = runtime_deps,
    deps = [":docstore-aws"] + test_deps + deps,
)

[java_junit5_test(
    name = name[:-len(".java")],
    size = "medium",
    timeout = "long",
    runtime_deps = [":docstore-aws-junit"],
) for name in glob([
    "src/test/java/**/*IT.java",
    "src/test/java/**/*Test.java",
])]
