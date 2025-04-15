load("//tools/junit5:junit5.bzl", "java_junit5_test")

deps = [
    "//projects/libs/chameleon/multicloudj/sts/sts-client:sts-client",
    "//projects/libs/chameleon/multicloudj/multicloudj-common",
    "//tools/maven_install/managed/restricted:cag_com_aliyun_aliyun_java_sdk_core",
    "//tools/maven_install/managed/restricted:cag_com_aliyun_aliyun_java_sdk_sts",
    "//tools/maven_install/managed/restricted:cag_io_opentracing_opentracing_api",
    "//tools/maven_install/managed/restricted:cag_io_opentracing_opentracing_noop",
    "//tools/maven_install/managed/restricted:cag_io_opentracing_opentracing_util",
    "@org_bouncycastle//:org_bouncycastle_bcprov_jdk18on",
    "@org_apache//:org_apache_commons_commons_lang3",
    "@com_google//:com_google_auto_service_auto_service",
    "@com_google//:com_google_auto_service_auto_service_annotations",
    "@com_google//:com_google_auto_auto_common",
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
    name = "sts-ali",
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
    "@com_google//:com_google_guava_guava",
    "@test//:org_hamcrest_hamcrest",
    "@test//:org_hamcrest_hamcrest_core",
    "@test//:org_mockito_mockito_junit_jupiter",
    "@test//:org_mockito_mockito_core",
    "@test//:org_mockito_mockito_inline",
    "@org_slf4j//:org_slf4j_slf4j_api",
    "@ch_qos_logback//:ch_qos_logback_logback_classic",
]

java_library(
    name = "sts_ali_junit",
    testonly = True,
    srcs = glob(["src/test/java/**/*.java"]),
    data = glob(["src/test/resources/**/*"]),
    resources = glob(["src/test/resources/**/*"]),
    runtime_deps = runtime_deps,
    deps = [":sts-ali"] + test_deps + deps,
)

[java_junit5_test(
    name = name[:-len(".java")],
    size = "small",
    runtime_deps = [":sts_ali_junit"],
) for name in glob([
    "src/test/java/**/*Test.java",
])]

''' TODO: Ali tests are disabled unless we find the safe environment to run them.
[java_junit5_test(
    name = name[:-len(".java")],
    size = "medium",
    # requires an Ali specific cert in the local truststore
    # sts.cn-shanghai.aliyuncs.com
    tags = ["ci_only"],
    runtime_deps = [":sts_ali_junit"],
) for name in glob([
    "src/test/java/**/*IT.java",
])]

'''
