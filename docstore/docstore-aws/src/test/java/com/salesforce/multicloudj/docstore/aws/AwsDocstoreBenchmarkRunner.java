package com.salesforce.multicloudj.docstore.aws;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH entry point for the AWS DynamoDB docstore benchmark suite.
 *
 * <p>Not {@code @Disabled} so it can be invoked directly by Maven Surefire:
 * <pre>
 *   export AWS_ACCESS_KEY_ID=...
 *   export AWS_SECRET_ACCESS_KEY=...
 *   export AWS_SESSION_TOKEN=...
 *   export DOCSTORE_BENCHMARK_AWS_REGION=us-east-2
 *   export DOCSTORE_BENCHMARK_AWS_SINGLE_KEY_TABLE=docstore-benchmark-test1
 *   export DOCSTORE_BENCHMARK_AWS_COMPOSITE_KEY_TABLE=docstore-benchmark-test2
 *   mvn test -pl docstore/docstore-aws -Dtest=AwsDocstoreBenchmarkRunner#runBenchmarks
 * </pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsDocstoreBenchmarkRunner {

  @Test
  public void runBenchmarks() throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(".*AwsDocstoreBenchmarkTest.*")
            .forks(1)
            .resultFormat(ResultFormatType.JSON)
            .result("target/jmh-docstore-results-aws.json")
            .build();

    new Runner(opt).run();
  }
}
