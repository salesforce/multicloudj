package com.salesforce.multicloudj.registry.gcp;

import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.registry.client.AbstractRegistryBenchmarkTest;
import com.salesforce.multicloudj.registry.client.ContainerRegistryClient;
import org.junit.jupiter.api.TestInstance;

/**
 * Hits the real Artifact Registry endpoint directly (no wiremock replay), so it requires live
 * Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS) and network access to GAR.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpRegistryBenchmarkTest extends AbstractRegistryBenchmarkTest {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return GcpConstants.PROVIDER_ID;
  }

  public static class HarnessImpl implements Harness {

    private final String endpoint = requireEnv("REGISTRY_BENCHMARK_GCP_ENDPOINT");
    private final String smallImageRef = requireEnv("REGISTRY_BENCHMARK_GCP_SMALL_IMAGE_REF");
    private final String multiArchImageRef =
        optionalEnv("REGISTRY_BENCHMARK_GCP_MULTIARCH_IMAGE_REF");

    @Override
    public ContainerRegistryClient createClient() {
      // Uses application default credentials; run with GOOGLE_APPLICATION_CREDENTIALS set.
      return ContainerRegistryClient.builder(GcpConstants.PROVIDER_ID)
          .withRegistryEndpoint(endpoint)
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
