package com.salesforce.multicloudj.iam.aws;

import com.salesforce.multicloudj.iam.client.AbstractIamBenchmarkTest;
import com.salesforce.multicloudj.iam.client.IamClient;
import java.net.URI;
import java.util.List;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AwsIamBenchmarkTest extends AbstractIamBenchmarkTest {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return "aws";
  }

  public static class HarnessImpl implements Harness {

    private final String region = requireEnv("IAM_BENCHMARK_AWS_REGION");
    private final String identityName = requireEnv("IAM_BENCHMARK_AWS_IDENTITY_NAME");
    private final String tenantId = requireEnv("IAM_BENCHMARK_AWS_TENANT_ID");
    private final String policyName = requireEnv("IAM_BENCHMARK_AWS_POLICY_NAME");
    private final String endpoint = requireEnv("IAM_BENCHMARK_AWS_ENDPOINT");
    private final String policyResource = requireEnv("IAM_BENCHMARK_AWS_POLICY_RESOURCE");

    @Override
    public IamClient createIamClient() {
      // Credentials flow via the AWS default provider chain (OS env / profile), never -D.
      return IamClient.builder("aws").withRegion(region).withEndpoint(URI.create(endpoint)).build();
    }

    @Override
    public String getIdentityName() {
      return identityName;
    }

    @Override
    public String getTenantId() {
      return tenantId;
    }

    @Override
    public String getRegion() {
      return region;
    }

    @Override
    public String getPolicyName() {
      return policyName;
    }

    @Override
    public String getRoleName() {
      return identityName;
    }

    @Override
    public List<String> getPolicyActions() {
      return List.of("storage:GetObject", "storage:PutObject");
    }

    @Override
    public String getPolicyResource() {
      return policyResource;
    }

    @Override
    public String getPolicyVersion() {
      return "2012-10-17";
    }

    @Override
    public void close() {
      // IamClient owns and closes the underlying AWS SDK IamClient
    }
  }
}
