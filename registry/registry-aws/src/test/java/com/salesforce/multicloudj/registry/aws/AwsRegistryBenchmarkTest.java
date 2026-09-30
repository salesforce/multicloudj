package com.salesforce.multicloudj.registry.aws;

import com.salesforce.multicloudj.registry.client.AbstractRegistryBenchmarkTest;
import com.salesforce.multicloudj.registry.client.ContainerRegistryClient;
import org.junit.jupiter.api.TestInstance;

/**
 * Hits the real ECR endpoint directly (no wiremock replay), so it requires live AWS credentials via
 * the default provider chain (OS env / profile) and network access to ECR.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsRegistryBenchmarkTest extends AbstractRegistryBenchmarkTest {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return "aws";
  }

  public static class HarnessImpl implements Harness {

    private final String endpoint = requireEnv("REGISTRY_BENCHMARK_AWS_ENDPOINT");
    private final String region = requireEnv("REGISTRY_BENCHMARK_AWS_REGION");
    private final String smallImageRef = requireEnv("REGISTRY_BENCHMARK_AWS_SMALL_IMAGE_REF");
    private final String multiArchImageRef =
        optionalEnv("REGISTRY_BENCHMARK_AWS_MULTIARCH_IMAGE_REF");

    @Override
    public ContainerRegistryClient createClient() {
      // Credentials flow via the AWS default provider chain (OS env / profile), never -D.
      return ContainerRegistryClient.builder("aws")
          .withRegistryEndpoint(endpoint)
          .withRegion(region)
          .build();
    }

    @Override
    public String getSmallImageRef() {
      return smallImageRef;
    }

    @Override
    public String getMultiArchImageRef() {
      return multiArchImageRef;
    }

    @Override
    public void close() {}
  }
}
