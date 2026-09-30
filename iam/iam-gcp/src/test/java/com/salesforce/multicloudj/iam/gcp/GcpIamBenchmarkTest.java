package com.salesforce.multicloudj.iam.gcp;

import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.iam.client.AbstractIamBenchmarkTest;
import com.salesforce.multicloudj.iam.client.IamClient;
import java.util.List;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GcpIamBenchmarkTest extends AbstractIamBenchmarkTest {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  @Override
  protected String getProviderId() {
    return GcpConstants.PROVIDER_ID;
  }

  public static class HarnessImpl implements Harness {

    private final String tenantId = requireEnv("IAM_BENCHMARK_GCP_TENANT_ID");
    private final String region = requireEnv("IAM_BENCHMARK_GCP_REGION");
    private final String identityName = requireEnv("IAM_BENCHMARK_GCP_IDENTITY_NAME");
    private final String roleName = requireEnv("IAM_BENCHMARK_GCP_ROLE_NAME");

    @Override
    public IamClient createIamClient() {
      // GCP IAM Admin/Resource Manager clients authenticate via Application Default Credentials
      // (GOOGLE_APPLICATION_CREDENTIALS); GcpIam does not support CredentialsOverrider injection.
      return IamClient.builder(GcpConstants.PROVIDER_ID).withRegion(region).build();
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
      return roleName;
    }

    @Override
    public String getRoleName() {
      return roleName;
    }

    @Override
    public List<String> getPolicyActions() {
      return List.of("storage:GetObject");
    }

    @Override
    public String toPolicyMember(String identityName, String identityId) {
      // GCP's createIdentity returns the service account email; attach/removePolicy require the
      // identityName to be a GCP member string ("serviceAccount:email"), not the bare account id.
      return "serviceAccount:" + identityId;
    }

    @Override
    public void close() {
      // IamClient owns and closes the underlying GCP IAMClient/ProjectsClient
    }
  }
}
