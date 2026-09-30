package com.salesforce.multicloudj.docstore.aws;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Guards the keyless-credentials contract of the docstore-aws benchmark harness.
 *
 * <p>The benchmark itself only runs under {@code -DrunBenchmarks=true}, which no CI job sets, so a
 * regression to demanding static AWS keys would go unnoticed. This test drives the harness
 * client-construction path directly: with no AWS key credentials present it must build a docstore
 * without throwing. Construction is lazy, so this never contacts DynamoDB.
 */
public class AwsDocstoreBenchmarkHarnessTest {

  private static final String SINGLE_KEY_TABLE = "DOCSTORE_BENCHMARK_AWS_SINGLE_KEY_TABLE";
  private static final String REGION = "DOCSTORE_BENCHMARK_AWS_REGION";

  @AfterEach
  void clearProperties() {
    System.clearProperty(SINGLE_KEY_TABLE);
    System.clearProperty(REGION);
  }

  @Test
  void createDocStoreSucceedsWithoutStaticAwsCredentials() throws Exception {
    // Only meaningful when no AWS key creds are present: the old harness resolved them via
    // requireEnv, which also reads system properties, so a set key would mask this regression.
    assumeCredentialAbsent("AWS_ACCESS_KEY_ID");
    assumeCredentialAbsent("AWS_SECRET_ACCESS_KEY");
    assumeCredentialAbsent("AWS_SESSION_TOKEN");

    // requireEnv falls back to system properties, so these stand in for the deploy's env vars.
    System.setProperty(SINGLE_KEY_TABLE, "mcj-docstore-bench-players");
    System.setProperty(REGION, "us-west-2");

    AwsDocstoreBenchmarkTest.HarnessImpl harness = new AwsDocstoreBenchmarkTest.HarnessImpl();
    try {
      AbstractDocStore docStore = assertDoesNotThrow(harness::createDocStore);
      assertNotNull(docStore);
    } finally {
      harness.close();
    }
  }

  private static void assumeCredentialAbsent(String name) {
    Assumptions.assumeTrue(
        StringUtils.isBlank(System.getenv(name)) && StringUtils.isBlank(System.getProperty(name)),
        () -> name + " is set; skipping keyless-credentials guard");
  }
}
