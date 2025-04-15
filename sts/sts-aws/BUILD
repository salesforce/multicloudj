load("//tools/junit5:junit5.bzl", "java_junit5_test")

deps = [
    "//projects/libs/chameleon/multicloudj/sts/sts-client:sts-client",
    "//projects/libs/chameleon/multicloudj/multicloudj-common",
    "//projects/libs/chameleon/multicloudj/multicloudj-common-aws",
    "@awssdk//:software_amazon_awssdk_auth",
    "@awssdk//:software_amazon_awssdk_aws_core",
    "@awssdk//:software_amazon_awssdk_checksums",
    "@awssdk//:software_amazon_awssdk_checksums_spi",
    "@awssdk//:software_amazon_awssdk_http_auth_aws",
    "@awssdk//:software_amazon_awssdk_http_auth_spi",
    "@awssdk//:software_amazon_awssdk_http_client_spi",
    "@awssdk//:software_amazon_awssdk_identity_spi",
    "@awssdk//:software_amazon_awssdk_regions",
    "@awssdk//:software_amazon_awssdk_sdk_core",
    "@awssdk//:software_amazon_awssdk_sts",
    "@com_google//:com_google_auto_service_auto_service",
    "@com_google//:com_google_auto_service_auto_service_annotations",
    "@com_google//:com_google_auto_auto_common",
    "@commons//:commons_logging_commons_logging",
    "@org_slf4j//:org_slf4j_slf4j_api",
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
    name = "sts-aws",
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
    "//projects/libs/chameleon/multicloudj/sts/sts-client:sts_client_junit",
    "//projects/libs/chameleon/multicloudj/multicloudj-common:multicloudj_common_junit",
    "//projects/libs/chameleon/multicloudj/multicloudj-common-aws:multicloudj_common_aws_junit",
    "@test//:org_hamcrest_hamcrest",
    "@test//:org_hamcrest_hamcrest_core",
    "@test//:org_mockito_mockito_junit_jupiter",
    "@test//:org_mockito_mockito_core",
    "@test//:org_mockito_mockito_inline",
    "@ch_qos_logback//:ch_qos_logback_logback_classic",
    "@unmanaged_wiremock_jetty11//:org_wiremock_wiremock",
]

java_library(
    name = "sts_aws_junit",
    testonly = True,
    srcs = glob(["src/test/java/**/*.java"]),
    data = glob(["src/test/resources/**/*"]),
    resources = glob(["src/test/resources/**/*"]),
    runtime_deps = runtime_deps,
    deps = [":sts-aws"] + test_deps + deps,
)

[java_junit5_test(
    name = name[:-len(".java")],
    size = "small",
    runtime_deps = [":sts_aws_junit"],
) for name in glob([
    "src/test/java/**/*Test.java",
    "src/test/java/**/*IT.java",
])]
