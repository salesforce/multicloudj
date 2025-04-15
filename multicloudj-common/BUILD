load("//tools/junit5:junit5.bzl", "java_junit5_test")

deps = [
]

runtime_deps = [
]

java_library(
    name = "multicloudj-common",
    srcs = glob(["src/main/java/**/*.java"]),
    javacopts = ["--release 11"],
    visibility = ["//visibility:public"],
    deps = deps,
)

test_deps = [
    "//tools/junit5:junit5_deps",
    "@ch_qos_logback//:ch_qos_logback_logback_classic",
    "@ch_qos_logback//:ch_qos_logback_logback_core",
    "@org_slf4j//:org_slf4j_slf4j_api",
    "@test//:org_hamcrest_hamcrest",
    "@test//:org_hamcrest_hamcrest_core",
    "@test//:org_mockito_mockito_junit_jupiter",
    "@test//:org_mockito_mockito_core",
    "@test//:org_mockito_mockito_inline",
    "@unmanaged_wiremock_jetty11//:org_wiremock_wiremock",
]

java_library(
    name = "multicloudj_common_junit",
    testonly = True,
    srcs = glob(["src/test/java/**/*.java"]),
    data = glob(["src/test/resources/**/*"]),
    resources = glob(["src/test/resources/**/*"]),
    visibility = ["//visibility:public"],
    runtime_deps = runtime_deps,
    deps = [":multicloudj-common"] + test_deps + deps,
)

[java_junit5_test(
    name = name[:-len(".java")],
    size = "small",
    runtime_deps = [":multicloudj_common_junit"],
) for name in glob([
    "src/test/java/**/*Test.java",
])]
