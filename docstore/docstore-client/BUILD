load("//tools/junit5:junit5.bzl", "java_junit5_test")

deps = [
    "//projects/libs/chameleon/multicloudj/multicloudj-common",
    "//projects/libs/chameleon/multicloudj/sts/sts-client:sts-client",
    "//tools/common/java/lombok",
    "@nexus//:org_projectlombok_lombok",
    "@org_apache//:org_apache_commons_commons_lang3",
    "@com_google//:com_google_guava_guava",
    "@com_google//:com_google_protobuf_protobuf_java",
    "@com_google//:com_google_protobuf_protobuf_java_util",
    "@com//:com_fasterxml_classmate",
    "@com//:com_fasterxml_woodstox_woodstox_core",
    "@com_fasterxml_jackson//:com_fasterxml_jackson_core_jackson_annotations",
    "@com_fasterxml_jackson//:com_fasterxml_jackson_core_jackson_core",
    "@com_fasterxml_jackson//:com_fasterxml_jackson_core_jackson_databind",
    "@com_fasterxml_jackson//:com_fasterxml_jackson_dataformat_jackson_dataformat_cbor",
    "@com_fasterxml_jackson//:com_fasterxml_jackson_dataformat_jackson_dataformat_xml",
    "@com_fasterxml_jackson//:com_fasterxml_jackson_dataformat_jackson_dataformat_yaml",
    "@com_fasterxml_jackson//:com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
    "@com_fasterxml_jackson//:com_fasterxml_jackson_datatype_jackson_datatype_jsr310",
    "@com_fasterxml_jackson//:com_fasterxml_jackson_module_jackson_module_jaxb_annotations",
    "@com_fasterxml_jackson//:com_fasterxml_jackson_module_jackson_module_parameter_names",
]

runtime_deps = [
]

java_plugin(
    name = "lombok",
    generates_api = True,
    processor_class = "lombok.launch.AnnotationProcessorHider$AnnotationProcessor",
    deps = [
        "@nexus//:org_projectlombok_lombok",
    ],
)

java_library(
    name = "docstore-client",
    srcs = glob(["src/main/java/**/*.java"]),
    javacopts = ["--release 11"],
    plugins = [":lombok"],
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
    name = "docstore_client_junit",
    testonly = True,
    srcs = glob(["src/test/java/**/*.java"]),
    visibility = ["//visibility:public"],
    runtime_deps = runtime_deps,
    deps = [":docstore-client"] + test_deps + deps,
)

[java_junit5_test(
    name = name[:-len(".java")],
    size = "small",
    runtime_deps = [":docstore_client_junit"],
) for name in glob([
    "src/test/java/**/*Test.java",
])]
