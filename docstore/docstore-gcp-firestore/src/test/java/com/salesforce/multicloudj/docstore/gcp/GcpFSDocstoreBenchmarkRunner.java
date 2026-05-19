package com.salesforce.multicloudj.docstore.gcp;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH entry point for the GCP Firestore docstore benchmark suite.
 *
 * <p>Not {@code @Disabled} so it can be invoked directly by Maven Surefire:
 * <pre>
 *   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
 *   export DOCSTORE_BENCHMARK_GCP_PROJECT_ID=my-gcp-project
 *   export DOCSTORE_BENCHMARK_GCP_SINGLE_KEY_COLLECTION=firestore-benchmark-test1
 *   export DOCSTORE_BENCHMARK_GCP_COMPOSITE_KEY_COLLECTION=firestore-benchmark-test2
 *   mvn test -pl docstore/docstore-gcp-firestore -Dtest=GcpFSDocstoreBenchmarkRunner#runBenchmarks
 * </pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpFSDocstoreBenchmarkRunner {

  @Test
  public void runBenchmarks() throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(".*GcpFSDocstoreBenchmarkTest.*")
            .forks(1)
            .resultFormat(ResultFormatType.JSON)
            .result("target/jmh-docstore-results-gcp-firestore.json")
            .build();

    new Runner(opt).run();
  }
}
