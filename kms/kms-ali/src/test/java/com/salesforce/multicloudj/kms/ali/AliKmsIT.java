package com.salesforce.multicloudj.kms.ali;

import com.aliyun.kms20160120.Client;
import com.aliyun.kms20160120.models.ScheduleKeyDeletionRequest;
import com.aliyun.teaopenapi.models.Config;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.kms.client.AbstractKmsIT;
import com.salesforce.multicloudj.kms.driver.AbstractKms;
import com.salesforce.multicloudj.kms.model.KeyPrincipal;
import com.salesforce.multicloudj.kms.model.KeyRef;
import com.salesforce.multicloudj.kms.model.PrincipalType;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Disabled;

@Disabled(
    "Alibaba KMS conformance recordings are captured on dedicated machines with Alibaba "
        + "credentials; they are not recorded or replayed in the shared build.")
public class AliKmsIT extends AbstractKmsIT {

  @Override
  protected Harness createHarness() {
    return new HarnessImpl();
  }

  public static class HarnessImpl implements Harness {
    private static final String REGION = "cn-shanghai";
    private static final String ENDPOINT = "kms." + REGION + ".aliyuncs.com";
    private static final String ACCOUNT_ID = "1234567890123456";

    private final int port = ThreadLocalRandom.current().nextInt(2000, 20000);
    private Client kmsClient;

    @Override
    public AbstractKms createKms() {
      String accessKeyId =
          System.getenv().getOrDefault("ALIBABA_CLOUD_ACCESS_KEY_ID", "FAKE_ACCESS_KEY");
      String accessKeySecret =
          System.getenv().getOrDefault("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "FAKE_SECRET_KEY");
      String securityToken =
          System.getenv().getOrDefault("ALIBABA_CLOUD_SECURITY_TOKEN", "FAKE_SESSION_TOKEN");

      Config config =
          new Config()
              .setAccessKeyId(accessKeyId)
              .setAccessKeySecret(accessKeySecret)
              .setSecurityToken(securityToken)
              .setType("sts")
              .setEndpoint(ENDPOINT)
              .setRegionId(REGION)
              .setProtocol("http")
              .setHttpProxy("http://" + TestsUtil.WIREMOCK_HOST + ":" + (port + 1));
      try {
        kmsClient = new Client(config);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create KMS client", e);
      }
      return new AliKms.Builder().withRegion(REGION).withKmsClient(kmsClient).build();
    }

    @Override
    public String getProviderId() {
      return "ali";
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    public List<String> getWiremockExtensions() {
      return List.of();
    }

    @Override
    public String getKmsEndpoint() {
      return "http://" + ENDPOINT;
    }

    @Override
    public String getKeyName(String testName) {
      return "multicloudj-" + testName;
    }

    @Override
    public String getNonexistentKeyId() {
      return "00000000-0000-0000-0000-000000000000";
    }

    @Override
    public KeyPrincipal getAdminPrincipal() {
      return KeyPrincipal.of(PrincipalType.IAM_ROLE, "acs:ram::" + ACCOUNT_ID + ":role/kms-admin");
    }

    @Override
    public KeyPrincipal getGranteePrincipal() {
      return KeyPrincipal.of(PrincipalType.IAM_ROLE, "acs:ram::" + ACCOUNT_ID + ":role/kms-user");
    }

    @Override
    public void deleteKey(KeyRef keyRef) {
      try {
        kmsClient.scheduleKeyDeletion(
            new ScheduleKeyDeletionRequest().setKeyId(keyRef.getKeyId()).setPendingWindowInDays(7));
      } catch (Exception e) {
        throw new RuntimeException("Failed to schedule key deletion", e);
      }
    }

    @Override
    public void close() {}
  }
}
