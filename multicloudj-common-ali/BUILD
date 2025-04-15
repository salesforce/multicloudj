deps = [
    "//projects/libs/chameleon/multicloudj/multicloudj-common",
    "//projects/libs/chameleon/multicloudj/sts/sts-client:sts-client",
    "@commons//:commons_logging_commons_logging",
    "@org_slf4j//:org_slf4j_slf4j_api",
]

runtime_deps = [
]

java_library(
    name = "multicloudj-common-ali",
    srcs = glob(["src/main/java/**/*.java"]),
    javacopts = ["--release 11"],
    visibility = ["//visibility:public"],
    deps = deps,
)