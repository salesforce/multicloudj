package com.salesforce.multicloudj.sts.aws;

import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.sts.client.AbstractStsBenchmarkTest;
import com.salesforce.multicloudj.sts.client.StsClient;
import java.net.URI;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsStsBenchmarkTest extends AbstractStsBenchmarkTest {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return AwsConstants.PROVIDER_ID;
  }

  @Override
  protected boolean supportsGetAccessToken() {
    // GetSessionToken rejects temporary/session credentials, so it cannot run under the
    // assumed-role creds the pipeline uses.
    return false;
  }

  public static class HarnessImpl implements Harness {

    private final String region = requireEnv("STS_BENCHMARK_AWS_REGION");
    private final String roleArn = requireEnv("STS_BENCHMARK_AWS_ROLE_ARN");
    // Optional: the SDK derives the default regional STS endpoint from the region. Set only for
    // non-default endpoints (FIPS, VPC/PrivateLink, custom partitions).
    private final String endpoint = optionalEnv("STS_BENCHMARK_AWS_ENDPOINT");
    private final String webIdentityToken = optionalEnv("STS_BENCHMARK_AWS_WEB_IDENTITY_TOKEN");

    @Override
    public StsClient createStsClient() {
      StsClient.StsBuilder builder = StsClient.builder(AwsConstants.PROVIDER_ID).withRegion(region);
      if (endpoint != null) {
        builder.withEndpoint(URI.create(endpoint));
      }
      return builder.build();
    }

    @Override
    public String getRoleName() {
      return roleArn;
    }

    @Override
    public String getWebIdentityToken() {
      // External OIDC token minted by a trusted IdP; supplied via env when available.
      return webIdentityToken;
    }

    @Override
    public void close() {
      // StsClient has no explicit resource to release.
    }
  }
}
