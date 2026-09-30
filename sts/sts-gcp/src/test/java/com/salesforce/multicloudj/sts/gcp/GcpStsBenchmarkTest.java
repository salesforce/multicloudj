package com.salesforce.multicloudj.sts.gcp;

import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.sts.client.AbstractStsBenchmarkTest;
import com.salesforce.multicloudj.sts.client.StsClient;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpStsBenchmarkTest extends AbstractStsBenchmarkTest {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return GcpConstants.PROVIDER_ID;
  }

  @Override
  protected boolean supportsGetAccessToken() {
    return true;
  }

  public static class HarnessImpl implements Harness {

    private final String serviceAccount = requireEnv("STS_BENCHMARK_GCP_SERVICE_ACCOUNT");
    private final String webIdentityToken = optionalEnv("STS_BENCHMARK_GCP_WEB_IDENTITY_TOKEN");

    @Override
    public StsClient createStsClient() {
      return StsClient.builder(GcpConstants.PROVIDER_ID).build();
    }

    @Override
    public String getRoleName() {
      return serviceAccount;
    }

    @Override
    public String getWebIdentityToken() {
      // External OIDC token exchanged for a GCP access token; supplied via env when available.
      return webIdentityToken;
    }

    @Override
    public void close() {
      // No client resource to release.
    }
  }
}
