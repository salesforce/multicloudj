load("//tools/junit5:junit5.bzl", "java_junit5_test")

deps = [
    "//projects/libs/chameleon/multicloudj/multicloudj-common",
    "//projects/libs/chameleon/multicloudj/sts/sts-client:sts-client",
    "@awssdk//:software_amazon_awssdk_aws_core",
    "@awssdk//:software_amazon_awssdk_auth",
    "@awssdk//:software_amazon_awssdk_regions",
    "@awssdk//:software_amazon_awssdk_sdk_core",
    "@awssdk//:software_amazon_awssdk_s3",
    "@awssdk//:software_amazon_awssdk_sts",
    "@commons//:commons_logging_commons_logging",
    "@org_slf4j//:org_slf4j_slf4j_api",
]

runtime_deps = [
]

java_library(
    name = "multicloudj-common-aws",
    srcs = glob(["src/main/java/**/*.java"]),
    javacopts = ["--release 11"],
    visibility = ["//visibility:public"],
    deps = deps,
)

test_deps = [
    "//projects/libs/chameleon/multicloudj/multicloudj-common:multicloudj_common_junit",
    "@awssdk//:software_amazon_awssdk_apache_client",
    "@awssdk//:software_amazon_awssdk_http_client_spi",
    "//tools/junit5:junit5_deps",
    "@test//:org_hamcrest_hamcrest",
    "@test//:org_hamcrest_hamcrest_core",
    "@test//:org_mockito_mockito_junit_jupiter",
    "@test//:org_mockito_mockito_core",
    "@test//:org_mockito_mockito_inline",
]

java_library(
    name = "multicloudj_common_aws_junit",
    testonly = True,
    srcs = glob(["src/test/java/**/*.java"]),
    data = glob(["src/test/resources/**/*"]),
    resources = glob(["src/test/resources/**/*"]),
    visibility = ["//visibility:public"],
    runtime_deps = runtime_deps,
    deps = [":multicloudj-common-aws"] + test_deps + deps,
)

[java_junit5_test(
    name = name[:-len(".java")],
    size = "small",
    runtime_deps = [":multicloudj_common_aws_junit"],
) for name in glob([
    "src/test/java/**/*Test.java",
])]
