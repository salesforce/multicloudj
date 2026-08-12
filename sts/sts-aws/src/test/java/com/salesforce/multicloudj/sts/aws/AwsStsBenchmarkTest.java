package com.salesforce.multicloudj.sts.aws;

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
    return "aws";
  }

  public static class HarnessImpl implements Harness {

    private final String region = requireEnv("STS_BENCHMARK_AWS_REGION");
    private final String roleArn = requireEnv("STS_BENCHMARK_AWS_ROLE_ARN");
    private final String endpoint = requireEnv("STS_BENCHMARK_AWS_ENDPOINT");

    @Override
    public StsClient createStsClient() {
      return StsClient.builder("aws").withRegion(region).withEndpoint(URI.create(endpoint)).build();
    }

    @Override
    public String getRoleName() {
      return roleArn;
    }

    @Override
    public String getWebIdentityToken() {
      // AWS web identity federation requires an external OIDC token; not benchmarkable here.
      return null;
    }

    @Override
    public void close() {
      // StsClient has no explicit resource to release.
    }
  }
}
