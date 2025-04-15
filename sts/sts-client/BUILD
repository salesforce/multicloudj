load("//tools/junit5:junit5.bzl", "java_junit5_test")

deps = [
    "//projects/libs/chameleon/multicloudj/multicloudj-common",
    "@com_google//:com_google_guava_guava",
    "@org_apache//:org_apache_commons_commons_lang3",
]

runtime_deps = [
]

java_library(
    name = "sts-client",
    srcs = glob(["src/main/java/**/*.java"]),
    javacopts = ["--release 11"],
    visibility = ["//visibility:public"],
    runtime_deps = runtime_deps,
    deps = deps,
)

test_deps = [
    "//tools/junit5:junit5_deps",
    "//projects/libs/chameleon/multicloudj/multicloudj-common:multicloudj_common_junit",
    "@test//:org_hamcrest_hamcrest",
    "@test//:org_hamcrest_hamcrest_core",
    "@test//:org_mockito_mockito_junit_jupiter",
    "@test//:org_mockito_mockito_core",
    "@test//:org_mockito_mockito_inline",
    "@test//:org_powermock_powermock_api_mockito2",
    "@test//:org_powermock_powermock_api_support",
    "@test//:org_powermock_powermock_core",
    "@test//:org_powermock_powermock_module_junit4",
    "@test//:org_powermock_powermock_module_junit4_common",
]

java_library(
    name = "sts_client_junit",
    testonly = True,
    srcs = glob(["src/test/java/**/*.java"]),
    visibility = ["//visibility:public"],
    runtime_deps = runtime_deps,
    deps = [":sts-client"] + test_deps + deps,
)

[java_junit5_test(
    name = name[:-len(".java")],
    size = "small",
    runtime_deps = [":sts_client_junit"],
) for name in glob([
    "src/test/java/**/*Test.java",
])]
